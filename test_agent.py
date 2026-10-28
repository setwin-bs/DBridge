"""Tests for the agent's control-connection liveness and reconnect behaviour.

These cover the incident where a proxy pod was replaced, the agent stayed blocked on a
half-open socket and the tenant lost access until a human restarted the service.
"""

import asyncio
import socket
import time
import unittest
import uuid

import agent


class FakeWriter:
    """Minimal StreamWriter stand-in for forward() tests."""

    transport = None

    def __init__(self):
        self.written = b""
        self.closed = False

    def write(self, data):
        self.written += data

    async def drain(self):
        pass

    def close(self):
        self.closed = True

    async def wait_closed(self):
        pass

    def is_closing(self):
        return self.closed


class FakeTransport:
    def __init__(self, sock):
        self._sock = sock

    def get_extra_info(self, name):
        if name == "socket":
            return self._sock
        return None


class FakeConnection:
    def __init__(self, sock):
        self.transport = FakeTransport(sock)


class AgentLivenessTest(unittest.IsolatedAsyncioTestCase):
    async def _serve(self, handler):
        server = await asyncio.start_server(handler, "127.0.0.1", 0)
        self.addAsyncCleanup(self._close_server, server)
        return server.sockets[0].getsockname()[1]

    async def _close_server(self, server):
        server.close()
        await server.wait_closed()

    async def _free_port(self):
        sock = socket.socket()
        sock.bind(("127.0.0.1", 0))
        port = sock.getsockname()[1]
        sock.close()
        return port

    async def test_control_timeout_reconnects(self):
        """A silent proxy must not leave the agent blocked forever."""
        token = uuid.uuid4()

        async def silent_proxy(reader, writer):
            try:
                await reader.readexactly(16)
                await reader.read()  # never ping, wait for the agent to give up
            finally:
                writer.close()

        port = await self._serve(silent_proxy)
        queue = asyncio.Queue()

        task = asyncio.create_task(
            agent.run_agent(
                token,
                "127.0.0.1",
                port,
                "127.0.0.1",
                1,
                False,
                None,
                0.01,
                0,
                0.2,
                queue,
            )
        )

        replacement = await asyncio.wait_for(queue.get(), timeout=3)
        await asyncio.wait_for(task, timeout=3)

        self.assertEqual(replacement, 1)
        self.assertEqual(queue.qsize(), 0, "exactly one replacement must be queued")

    async def test_handshake_failure_queues_single_replacement(self):
        """The handshake-failure branch and finally must not both queue a replacement."""

        async def bogus_proxy(reader, writer):
            try:
                await reader.readexactly(16)
                writer.write(b"bogus!!")
                await writer.drain()
                await reader.read()
            finally:
                writer.close()

        port = await self._serve(bogus_proxy)
        queue = asyncio.Queue()

        await asyncio.wait_for(
            agent.run_agent(
                uuid.uuid4(),
                "127.0.0.1",
                port,
                "127.0.0.1",
                1,
                False,
                None,
                0.01,
                0,
                1.0,
                queue,
            ),
            timeout=3,
        )

        self.assertEqual(queue.qsize(), 1)

    async def test_service_failure_queues_single_replacement(self):
        """A failed local-service dial logs the real error and queues one replacement."""

        async def connect_proxy(reader, writer):
            try:
                await reader.readexactly(16)
                writer.write(b"connect")
                await writer.drain()
                await reader.read()
            finally:
                writer.close()

        port = await self._serve(connect_proxy)
        closed_port = await self._free_port()
        queue = asyncio.Queue()

        with self.assertLogs(level="ERROR") as captured:
            await asyncio.wait_for(
                agent.run_agent(
                    uuid.uuid4(),
                    "127.0.0.1",
                    port,
                    "127.0.0.1",
                    closed_port,
                    False,
                    None,
                    0.01,
                    0,
                    1.0,
                    queue,
                ),
                timeout=3,
            )

        self.assertEqual(queue.qsize(), 1)
        self.assertTrue(
            any("service_connection_failed" in line for line in captured.output),
            "the real service error must be logged",
        )
        self.assertFalse(
            any("TypeError" in line for line in captured.output),
            "the old gather/unpack TypeError must be gone",
        )

    async def test_silent_stream_survives_without_idle_timeout(self):
        """With idle_timeout=0 a quiet stream stays open and delivers later bytes."""
        reader = asyncio.StreamReader()
        destination = FakeWriter()

        async def feed_later():
            await asyncio.sleep(0.3)
            reader.feed_data(b"x")
            reader.feed_eof()

        feeder = asyncio.create_task(feed_later())
        started = time.monotonic()

        total = await agent.forward(
            uuid.uuid4(),
            "test",
            ("127.0.0.1", 1),
            reader,
            destination,
            0,
        )

        elapsed = time.monotonic() - started
        await feeder

        self.assertEqual(total, 1)
        self.assertEqual(destination.written, b"x")
        self.assertGreaterEqual(elapsed, 0.25)
        self.assertTrue(destination.closed)

    async def test_fragmented_connect_is_accepted(self):
        """A `connect` split across TCP segments is still a valid frame."""
        reader = asyncio.StreamReader()
        task = asyncio.create_task(agent.read_control_frame(reader, 1))

        reader.feed_data(b"con")
        await asyncio.sleep(0.05)
        reader.feed_data(b"nect")

        self.assertEqual(await asyncio.wait_for(task, timeout=1), b"connect")

    async def test_buffered_frames_are_not_glued(self):
        """A buffered `ping` followed by `connect` must be read as two frames."""
        reader = asyncio.StreamReader()
        reader.feed_data(b"pingconnect")

        self.assertEqual(await agent.read_control_frame(reader, 1), b"ping")
        self.assertEqual(await agent.read_control_frame(reader, 1), b"connect")

    async def test_control_frame_times_out(self):
        reader = asyncio.StreamReader()

        with self.assertRaises(asyncio.TimeoutError):
            await agent.read_control_frame(reader, 0.1)

    async def test_reconnect_delay_has_jitter(self):
        for _ in range(20):
            delay = agent.reconnect_delay(1.0)
            self.assertGreaterEqual(delay, 1.0)
            self.assertLessEqual(delay, 1.5)

    async def test_failed_connection_closes_before_replacement(self):
        """The failed socket must be closed before its successor is queued."""
        active = {"now": 0}

        async def bogus_proxy(reader, writer):
            active["now"] += 1
            try:
                await reader.readexactly(16)
                writer.write(b"bogus!!")
                await writer.drain()
                await reader.read()
            except (asyncio.IncompleteReadError, ConnectionResetError):
                pass
            finally:
                active["now"] -= 1
                writer.close()

        port = await self._serve(bogus_proxy)
        queue = asyncio.Queue()

        task = asyncio.create_task(
            agent.run_agent(
                uuid.uuid4(),
                "127.0.0.1",
                port,
                "127.0.0.1",
                1,
                False,
                None,
                0.3,
                0,
                5.0,
                queue,
            )
        )

        self.assertEqual(await asyncio.wait_for(queue.get(), timeout=2), 1)
        self.assertEqual(active["now"], 0, "a failed connection stayed open while its successor ran")
        await asyncio.wait_for(task, timeout=2)

    async def test_failures_do_not_accumulate_connections(self):
        """AC-10: failures driven through spawn_agents must not grow live connections."""
        state = {"now": 0, "max": 0, "accepted": 0}

        async def bogus_proxy(reader, writer):
            state["accepted"] += 1
            state["now"] += 1
            state["max"] = max(state["max"], state["now"])
            try:
                await reader.readexactly(16)
                writer.write(b"bogus!!")
                await writer.drain()
                await reader.read()
            except (asyncio.IncompleteReadError, ConnectionResetError):
                pass
            finally:
                state["now"] -= 1
                writer.close()

        port = await self._serve(bogus_proxy)
        queue = asyncio.Queue()
        await queue.put(1)
        spawner = asyncio.create_task(
            agent.spawn_agents(
                queue,
                "127.0.0.1",
                port,
                "127.0.0.1",
                1,
                False,
                None,
                0.1,
                0,
                5.0,
            )
        )

        try:
            await asyncio.sleep(2.0)
        finally:
            spawner.cancel()
            await asyncio.gather(spawner, return_exceptions=True)
            for task in asyncio.all_tasks():
                if task.get_coro().__name__ == "run_agent":
                    task.cancel()
            await asyncio.sleep(0)

        self.assertGreaterEqual(state["accepted"], 3, "the harness must exercise several failures")
        self.assertLessEqual(
            state["max"],
            2,
            f"connections accumulated: max={state['max']} accepted={state['accepted']}",
        )

    async def test_retry_delay_applies_to_control_timeout(self):
        """A silent proxy is followed by the jittered retry delay, not an immediate retry."""

        async def silent_proxy(reader, writer):
            try:
                await reader.readexactly(16)
                await reader.read()
            finally:
                writer.close()

        port = await self._serve(silent_proxy)
        queue = asyncio.Queue()
        started = time.monotonic()

        await asyncio.wait_for(
            agent.run_agent(
                uuid.uuid4(),
                "127.0.0.1",
                port,
                "127.0.0.1",
                1,
                False,
                None,
                0.3,
                0,
                0.1,
                queue,
            ),
            timeout=3,
        )

        elapsed = time.monotonic() - started
        self.assertGreaterEqual(elapsed, 0.4, "control timeout did not back off before queueing")
        self.assertEqual(queue.qsize(), 1)

    async def test_retry_delay_applies_to_eof(self):
        """A proxy that closes after the token is followed by the jittered retry delay."""

        async def close_after_token(reader, writer):
            try:
                await reader.readexactly(16)
            finally:
                writer.close()

        port = await self._serve(close_after_token)
        queue = asyncio.Queue()
        started = time.monotonic()

        await asyncio.wait_for(
            agent.run_agent(
                uuid.uuid4(),
                "127.0.0.1",
                port,
                "127.0.0.1",
                1,
                False,
                None,
                0.3,
                0,
                5.0,
                queue,
            ),
            timeout=3,
        )

        elapsed = time.monotonic() - started
        self.assertGreaterEqual(elapsed, 0.3, "EOF did not back off before queueing")
        self.assertEqual(queue.qsize(), 1)

    async def test_enable_keepalive_is_best_effort(self):
        left, right = socket.socketpair()
        try:
            agent.enable_keepalive(FakeConnection(left))
            self.assertEqual(left.getsockopt(socket.SOL_SOCKET, socket.SO_KEEPALIVE), 1)
        finally:
            left.close()
            right.close()

        # None and a transport without a socket must be tolerated.
        agent.enable_keepalive(None)
        agent.enable_keepalive(FakeWriter())

        # A real asyncio writer exposes a TransportSocket, not the raw socket;
        # this is the shape that reaches enable_keepalive() in production.
        async def handler(reader, writer):
            await reader.read()
            writer.close()

        server = await asyncio.start_server(handler, "127.0.0.1", 0)
        port = server.sockets[0].getsockname()[1]
        reader, writer = await asyncio.open_connection("127.0.0.1", port)
        try:
            agent.enable_keepalive(writer)
            sock = writer.transport.get_extra_info("socket")
            self.assertEqual(sock.getsockopt(socket.SOL_SOCKET, socket.SO_KEEPALIVE), 1)
        finally:
            writer.close()
            await asyncio.gather(writer.wait_closed(), return_exceptions=True)
            server.close()
            await server.wait_closed()


if __name__ == "__main__":
    unittest.main()
