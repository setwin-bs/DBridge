import os
import argparse
import asyncio
import logging
import random
import socket
import typing
import uuid
import ssl
import time


T = typing.TypeVar("T")


def format_peer(peer: tuple[str, int] | None) -> str:
    if isinstance(peer, tuple) and len(peer) >= 2:
        return f"{peer[0]}:{peer[1]}"
    return "unknown"


def writer_peer(writer: asyncio.StreamWriter | None) -> tuple[str, int] | None:
    if writer is None:
        return None

    transport = writer.transport
    if not transport:
        return None

    return transport.get_extra_info('peername')


def configure_logging(level: str) -> None:
    logging.basicConfig(level=level, format="%(asctime)s %(levelname)s %(message)s")
    logging.Formatter.converter = time.gmtime  # timestamps in UTC


def reconnect_delay(base_seconds: float) -> float:
    """Keep the configured retry delay and add jitter so agents never sync up."""
    return base_seconds + random.uniform(0, 0.5)


def enable_keepalive(
    writer: asyncio.StreamWriter | None,
    idle_seconds: int = 30,
    interval_seconds: int = 10,
    count: int = 3,
) -> None:
    """Best-effort OS keepalive; the application deadline is the real guarantee."""
    if writer is None or writer.transport is None:
        return

    sock = writer.transport.get_extra_info("socket")
    if sock is None:
        return

    # transport.get_extra_info("socket") is an asyncio TransportSocket wrapper,
    # which does not expose ioctl(); the raw socket does.
    raw = getattr(sock, "_sock", sock)

    try:
        raw.setsockopt(socket.SOL_SOCKET, socket.SO_KEEPALIVE, 1)

        if hasattr(socket, "SIO_KEEPALIVE_VALS") and hasattr(raw, "ioctl"):  # Windows
            raw.ioctl(
                socket.SIO_KEEPALIVE_VALS,
                (1, idle_seconds * 1000, interval_seconds * 1000),
            )
        elif hasattr(socket, "TCP_KEEPIDLE"):  # Linux
            raw.setsockopt(socket.IPPROTO_TCP, socket.TCP_KEEPIDLE, idle_seconds)
            raw.setsockopt(socket.IPPROTO_TCP, socket.TCP_KEEPINTVL, interval_seconds)
            raw.setsockopt(socket.IPPROTO_TCP, socket.TCP_KEEPCNT, count)

    except (OSError, AttributeError, ValueError) as exc:
        logging.debug("keepalive_not_tuned exception=%r", exc)


async def close_writer(writer: asyncio.StreamWriter | None, timeout: float = 5.0) -> None:
    """Close a writer without letting a dead peer wedge the caller."""
    if writer is None:
        return

    writer.close()
    try:
        await asyncio.wait_for(writer.wait_closed(), timeout=timeout)
    except (asyncio.TimeoutError, OSError):
        pass


async def read_control_frame(reader: asyncio.StreamReader, timeout: float) -> bytes:
    """Read exactly one control frame (`ping` or `connect`) within the deadline.

    Reads an exact prefix first: `read(n)` may return several frames at once, so a
    buffered `ping` followed by `connect` must not be glued into `pingcon`.
    """
    loop = asyncio.get_running_loop()
    deadline = loop.time() + timeout

    remaining = deadline - loop.time()
    if remaining <= 0:
        raise asyncio.TimeoutError

    head = await asyncio.wait_for(reader.readexactly(4), timeout=remaining)

    if head == b"ping":
        return head

    if head == b"conn":
        remaining = deadline - loop.time()
        if remaining <= 0:
            raise asyncio.TimeoutError
        return head + await asyncio.wait_for(reader.readexactly(3), timeout=remaining)

    return head


def parse(to: typing.Type[T], value: typing.Any, or_default: T | None = None) -> T | None:
    if isinstance(value, to):
        return value
    try:
        return to(value)
    except (ValueError, TypeError):
        return or_default


def parse_bool(value: typing.Any, or_default: typing.Any | None = None) -> typing.Any | None:
    if isinstance(value, int):
        return value != 0

    if isinstance(value, str):
        match value.lower().strip():
            case "yes" | "y" | "t" | "true" | "s" | "sim" | "yea" | "yeah" | "on" | 1 | "1":
                return True
            case "no" | "n" | "f" | "false" | "ñ" | "não" | "nao" | "nop" | "nope" | "off" | 0 | "0" | "":
                return False
            case _:
                return or_default

    return or_default


def validate_min_threads(value):
    try:
        f_value = int(value)
        if f_value <= 0:
            raise argparse.ArgumentTypeError("must be a positive integer")
        return f_value
    except ValueError as exc:
        raise argparse.ArgumentTypeError("Invalid integer value.") from exc


def validate_retry_delay_seconds(value):
    try:
        f_value = float(value)
        if f_value <= 0 or f_value > 300:
            raise argparse.ArgumentTypeError("must be > 0 and <= 300")
        return f_value
    except ValueError as exc:
        raise argparse.ArgumentTypeError("Invalid float value.") from exc


def validate_idle_timeout(value):
    try:
        f_value = float(value)
        if f_value < 0:
            raise argparse.ArgumentTypeError("must be >= 0")
        return f_value
    except ValueError as exc:
        raise argparse.ArgumentTypeError("Invalid float value.") from exc


def validate_control_idle_timeout(value):
    try:
        f_value = float(value)
        if f_value <= 0 or f_value > 300:
            raise argparse.ArgumentTypeError("must be > 0 and <= 300")
        return f_value
    except ValueError as exc:
        raise argparse.ArgumentTypeError("Invalid float value.") from exc


def validate_log_level(value: str):
    value = value.upper()
    log_levels = logging.getLevelNamesMapping()

    if value not in log_levels:
        raise argparse.ArgumentTypeError(f"options are {", ".join(log_levels)}")

    return value


def parse_args():
    parser = argparse.ArgumentParser(description="Agent to connect to proxy and forward data.")

    default_proxy_host = os.getenv("PROXY_HOST")
    default_proxy_port = parse(to=int, value=os.getenv("PROXY_PORT"), or_default=7000)

    default_health_port = parse(to=int, value=os.getenv("HEALTH_PORT"), or_default=4000)

    default_service_host = os.getenv("SERVICE_HOST", "localhost")
    default_service_port = parse(to=int, value=os.getenv("SERVICE_PORT"), or_default=None)

    default_min_threads = parse(to=int, value=os.getenv("MIN_THREADS", os.cpu_count()), or_default=1)
    default_retry_delay_seconds = parse(to=float, value=os.getenv("RETRY_DELAY_SECONDS"), or_default=1.0)
    default_forward_idle_timeout = parse(to=float, value=os.getenv("FORWARD_IDLE_TIMEOUT_SECONDS"), or_default=0.0)
    default_control_idle_timeout = parse(
        to=float,
        value=os.getenv("CONTROL_IDLE_TIMEOUT_SECONDS"),
        or_default=45.0,
    )

    default_log_level = os.getenv("LOG_LEVEL", "INFO")

    default_use_ssl = parse_bool(value=os.getenv("USE_SSL"), or_default=False)
    default_ssl_cert = os.getenv("SSL_CERT", None)

    parser.add_argument(
        "-x", "--proxy-host",
        default=default_proxy_host,
        required=default_proxy_host is None,
        help="Proxy server host (required if not set in environment)",
    )

    parser.add_argument(
        "-p", "--proxy-port",
        type=int,
        default=default_proxy_port,
        help=f"Proxy server port (default: {default_proxy_port})",
    )

    parser.add_argument(
        "-e", "--health-port",
        type=int,
        default=default_health_port,
        help=f"Health prob port (default: {default_health_port})",
    )

    parser.add_argument(
        "-d", "--service-host",
        default=default_service_host,
        help=f"Service host (default: {default_service_host})",
    )

    parser.add_argument(
        "-b", "--service-port",
        type=int,
        default=default_service_port,
        required=default_service_port is None,
        help="Service port (required if not set in environment)",
    )

    parser.add_argument(
        "-n", "--min-threads",
        type=validate_min_threads,
        default=default_min_threads,
        help=(
            "Minimum number of agent threads to start initially. "
            "The system will scale based on load "
            f"(default: {default_min_threads})"
        ),
    )

    parser.add_argument(
        "-r", "--retry-delay-seconds",
        type=validate_retry_delay_seconds,
        default=default_retry_delay_seconds,
        help=(
            "Delay before retrying connection "
            f"(default: {default_retry_delay_seconds:.2f}s)"
        ),
    )

    parser.add_argument(
        "-f", "--forward-idle-timeout",
        type=validate_idle_timeout,
        default=default_forward_idle_timeout,
        help=(
            "Maximum seconds of inactivity allowed on a data stream before the agent closes the "
            "connection. Set to 0 to disable idle timeout (default: %.0fs)."
        ) % default_forward_idle_timeout,
    )

    parser.add_argument(
        "--control-idle-timeout",
        type=validate_control_idle_timeout,
        default=default_control_idle_timeout,
        help=(
            "Seconds the agent waits for the proxy's next ping before closing the control "
            "connection and reconnecting. Must be greater than the proxy's ping interval "
            f"(default: {default_control_idle_timeout:.0f}s)."
        ),
    )

    parser.add_argument(
        "-v", "--log-level",
        type=validate_log_level,
        default=default_log_level,
        help=f"Log Level (default: {default_log_level})",
    )

    parser.add_argument(
        "-s", "--ssl",
        action="store_true",
        default=default_use_ssl,
        help="Enable SSL for connecting to the proxy (default: no)",
    )

    parser.add_argument(
        "-c", "--cert",
        type=str,
        default=default_ssl_cert,
        help="Path to the CA certificate file (optional)",
    )

    return parser.parse_args()


async def forward(
    agent_token: uuid.UUID,
    direction: str,
    source_peername: tuple[str, int],
    source: asyncio.StreamReader,
    destination: asyncio.StreamWriter,
    idle_timeout: float,
):
    timeout_enabled = idle_timeout > 0

    total_bytes = 0
    dest_peer = format_peer(writer_peer(destination))

    try:
        while True:
            if timeout_enabled:
                data = await asyncio.wait_for(source.read(4096), timeout=idle_timeout)
            else:
                data = await source.read(4096)

            if not data:
                break

            destination.write(data)
            await destination.drain()
            total_bytes += len(data)
    except asyncio.TimeoutError:
        logging.warning(
            "agent=%s direction=%s source=%s destination=%s idle_timeout_seconds=%.0f",
            agent_token,
            direction,
            format_peer(source_peername),
            dest_peer,
            idle_timeout,
        )
    except asyncio.IncompleteReadError:
        logging.error(
            "agent=%s direction=%s source=%s destination=%s unexpected_eof",
            agent_token,
            direction,
            format_peer(source_peername),
            dest_peer,
        )

    except ConnectionResetError:
        logging.error(
            "agent=%s direction=%s source=%s destination=%s connection_reset",
            agent_token,
            direction,
            format_peer(source_peername),
            dest_peer,
        )

    except Exception:
        logging.exception(
            "agent=%s direction=%s source=%s destination=%s unexpected_exception",
            agent_token,
            direction,
            format_peer(source_peername),
            dest_peer,
        )

    finally:
        await close_writer(destination)

    return total_bytes


async def handle_health(_reader: asyncio.StreamReader, writer: asyncio.StreamWriter):
    writer.write("💅 slaying and thriving, PERIOD.".encode())
    await writer.drain()

    writer.close()
    await writer.wait_closed()


async def run_agent(
    token: uuid.UUID,
    proxy_host,
    proxy_port,
    svc_host,
    svc_port,
    use_ssl,
    cert,
    retry_delay_seconds,
    forward_idle_timeout,
    control_idle_timeout,
    queue: asyncio.Queue,
):
    proxy_writer = None
    svc_writer = None
    replacement_queued = False

    async def queue_replacement():
        """Queue exactly one replacement for this control connection."""
        nonlocal replacement_queued
        if replacement_queued:
            return
        replacement_queued = True
        await queue.put(1)

    try:
        ssl_context = ssl.create_default_context(cafile=cert) if use_ssl else None
        proxy_reader, proxy_writer = await asyncio.open_connection(proxy_host, proxy_port, ssl=ssl_context)
        enable_keepalive(proxy_writer)

        proxy_peername = writer_peer(proxy_writer)
        logging.info("agent=%s state=connected proxy=%s ssl=%s", token, format_peer(proxy_peername), bool(use_ssl))

        proxy_writer.write(token.bytes)  # Send my identification
        await proxy_writer.drain()

        while True:
            try:
                data = await read_control_frame(proxy_reader, control_idle_timeout)
            except asyncio.TimeoutError:
                logging.warning(
                    "agent=%s state=control_timeout idle_seconds=%.0f proxy=%s",
                    token,
                    control_idle_timeout,
                    format_peer(proxy_peername),
                )
                return
            except asyncio.IncompleteReadError:
                logging.info("agent=%s state=closed_by_proxy proxy=%s", token, format_peer(proxy_peername))
                return

            if data != b"ping":
                break

            proxy_writer.write(b"pong")
            await proxy_writer.drain()

        if data != b"connect":
            logging.error("agent=%s handshake_failed payload=%r", token, data)
            return

        logging.info("agent=%s bridge=preparing service=%s:%d", token, svc_host, svc_port)

        # Refill the pool before bridging: this connection is busy from now on.
        await queue_replacement()

        try:
            svc_reader, svc_writer = await asyncio.open_connection(svc_host, svc_port)
        except OSError as exc:
            logging.error(
                "agent=%s service_connection_failed service=%s:%d exception=%r",
                token,
                svc_host,
                svc_port,
                exc,
            )
            return

        proxy_writer.write(b"ready")
        await proxy_writer.drain()

        svc_peername = svc_writer.transport.get_extra_info('peername')
        proxy_peername = proxy_writer.transport.get_extra_info('peername')

        try:
            bridge_started_at = time.monotonic()
            proxy_to_service_bytes, service_to_proxy_bytes = await asyncio.gather(
                forward(token, "proxy->service", proxy_peername, proxy_reader, svc_writer, forward_idle_timeout),
                forward(token, "service->proxy", svc_peername, svc_reader, proxy_writer, forward_idle_timeout),
            )

            duration = time.monotonic() - bridge_started_at
            logging.info(
                (
                    "agent=%s bridge=finished proxy=%s service=%s duration=%.2fs "
                    "bytes_proxy_to_service=%d bytes_service_to_proxy=%d"
                ),
                token,
                format_peer(proxy_peername),
                format_peer(svc_peername),
                duration,
                proxy_to_service_bytes,
                service_to_proxy_bytes,
            )

        except Exception:
            logging.exception(
                "agent=%s bridge_error proxy=%s service=%s",
                token,
                format_peer(proxy_peername),
                format_peer(svc_peername),
            )

            await asyncio.gather(
                close_writer(svc_writer),
                close_writer(proxy_writer),
            )
            return

    except Exception as e:
        logging.error("agent=%s connection_error exception=%r retry_delay=%.2fs", token, e, retry_delay_seconds)

    finally:
        logging.info("agent=%s state=closed", token)

        # Close first: a failed connection must not stay open while its successor runs.
        await close_writer(proxy_writer)
        await close_writer(svc_writer)

        if not replacement_queued:
            # Failure before the bridge: back off before spawning the successor so
            # consecutive failures cannot accumulate open connections.
            await asyncio.sleep(reconnect_delay(retry_delay_seconds))
            await queue_replacement()


async def spawn_agents(queue: asyncio.Queue, *args):
    while True:
        await queue.get()  # Wait for a signal to spawn an agent
        token = uuid.uuid4()
        logging.debug("agent=%s spawn_requested queue_depth=%d", token, queue.qsize())
        asyncio.create_task(run_agent(token, *args, queue))


async def shutdown(loop, health_server):
    logging.debug("️( -_•)︻デ═一💥 killing...")

    await asyncio.sleep(0.5)  # Allow logs to flush

    if health_server:
        health_server.close()
        await health_server.wait_closed()

    tasks = {t for t in asyncio.all_tasks() if t is not asyncio.current_task()}

    for task in tasks:
        task.cancel()

    await asyncio.gather(*tasks, return_exceptions=True)
    loop.call_soon(loop.stop)


async def main():
    args = parse_args()
    configure_logging(args.log_level)

    loop = asyncio.get_running_loop()
    queue = asyncio.Queue()

    health_server = None

    try:
        # Start the initial set of agents
        await asyncio.gather(
            *[queue.put(1) for _ in range(args.min_threads)],
            return_exceptions=True
        )

        asyncio.create_task(
            spawn_agents(
                queue,
                args.proxy_host,
                args.proxy_port,
                args.service_host,
                args.service_port,
                args.ssl,
                args.cert,
                args.retry_delay_seconds,
                args.forward_idle_timeout,
                args.control_idle_timeout,
            )
        )

        health_server = await asyncio.start_server(
            handle_health, 'localhost', args.health_port
        )

        await asyncio.Event().wait()  # Keep the event loop running

    except asyncio.CancelledError:
        logging.debug("🛑 Agent was cancelled.")
        await shutdown(loop, health_server)


if __name__ == "__main__":
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        logging.debug("⌨️ Interrupted by user.")
