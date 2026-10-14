"""Regression tests for the agent-pool lifecycle in proxy.py.

The production incident: an agent connection is reset (RST) instead of closed
cleanly. The agent's ping/pong keepalive loop then ends in a failed state
because ``StreamWriter.wait_closed()`` re-raises the connection error. Later,
``pop_agent()`` awaits that dead task in ``stop_ping_pong()`` and the
``ConnectionResetError`` escapes all the way out of ``client_connected_cb``,
killing the client handler.
"""

import asyncio
import socket
import struct
import unittest
import uuid

import proxy


def force_reset(writer: asyncio.StreamWriter) -> None:
    """Abort a connection so the peer sees an RST (not a clean FIN)."""
    sock = writer.transport.get_extra_info("socket")
    if sock is not None:
        sock.setsockopt(socket.SOL_SOCKET, socket.SO_LINGER, struct.pack("ii", 1, 0))
    writer.transport.abort()


async def wait_done(task: asyncio.Task, timeout: float = 2.0) -> None:
    async def _poll():
        while not task.done():
            await asyncio.sleep(0.01)

    await asyncio.wait_for(_poll(), timeout=timeout)


class AgentPoolLifecycleTest(unittest.IsolatedAsyncioTestCase):
    async def asyncSetUp(self):
        self.context = proxy.Context()
        self.ping_pong_interval = 0.05

    async def _serve(self, handler):
        server = await asyncio.start_server(handler, "127.0.0.1", 0)
        self.addAsyncCleanup(self._close_server, server)
        return server.sockets[0].getsockname()[1]

    async def _close_server(self, server):
        server.close()
        await server.wait_closed()

    async def test_pop_agent_returns_healthy_registered_agent(self):
        token = uuid.uuid4()

        async def fake_agent(reader, writer):
            try:
                writer.write(token.bytes)
                await writer.drain()
                while True:
                    data = await reader.read(4)
                    if data != b"ping":
                        break
                    writer.write(b"pong")
                    await writer.drain()
            finally:
                writer.close()
                await asyncio.gather(writer.wait_closed(), return_exceptions=True)

        port = await self._serve(fake_agent)
        reader, writer = await asyncio.open_connection("127.0.0.1", port)
        self.addAsyncCleanup(self._close_writer, writer)

        await self.context.add_agent(reader, writer, self.ping_pong_interval)
        self.assertEqual(await self.context.length(), 1)

        await asyncio.sleep(self.ping_pong_interval * 3)

        agent = await self.context.pop_agent()
        self.assertIsNotNone(agent)
        self.assertEqual(agent.token, token)
        self.assertEqual(await self.context.length(), 0)

    async def test_pop_agent_survives_agent_reset(self):
        token = uuid.uuid4()
        registered = asyncio.Event()
        peer_writer: dict = {}

        async def fake_agent(reader, writer):
            peer_writer["w"] = writer
            writer.write(token.bytes)
            await writer.drain()
            registered.set()
            await asyncio.Event().wait()  # stay silent, never answer pings

        port = await self._serve(fake_agent)
        reader, writer = await asyncio.open_connection("127.0.0.1", port)
        self.addAsyncCleanup(self._close_writer, writer)

        await self.context.add_agent(reader, writer, self.ping_pong_interval)
        await asyncio.wait_for(registered.wait(), timeout=1)

        ping_task = self.context.agents[0].task_ping_pong

        # The agent is killed hard: the proxy must see a reset, not an EOF.
        force_reset(peer_writer["w"])
        await wait_done(ping_task)

        # The keepalive loop must not die in a failed state.
        self.assertIsNone(
            ping_task.exception(),
            "ping loop ended with an exception instead of closing cleanly",
        )

        # And pop_agent must never propagate that failure to the client handler.
        agent = await self.context.pop_agent()
        self.assertIsNone(agent, "a reset agent must be discarded, not handed to a client")
        self.assertEqual(await self.context.length(), 0)

    async def test_pop_agent_empty_pool_returns_none(self):
        self.assertIsNone(await self.context.pop_agent())

    async def _close_writer(self, writer):
        writer.close()
        await asyncio.gather(writer.wait_closed(), return_exceptions=True)


if __name__ == "__main__":
    unittest.main()
