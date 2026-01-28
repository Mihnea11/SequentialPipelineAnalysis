import asyncio
import threading
import queue
from dataclasses import dataclass
from typing import Any, Optional, Callable


@dataclass
class RunnerState:
    thread: Optional[threading.Thread] = None
    stop_event: Optional[threading.Event] = None
    out_queue: Optional["queue.Queue[Any]"] = None


def start_background_loop(
    target_coro_factory: Callable[..., "asyncio.Future[Any]"],
    *factory_args,
    **factory_kwargs,
) -> RunnerState:
    stop_event = threading.Event()
    out_q: "queue.Queue[Any]" = queue.Queue(maxsize=5000)

    def _runner():
        asyncio.run(target_coro_factory(stop_event, out_q, *factory_args, **factory_kwargs))

    t = threading.Thread(target=_runner, daemon=True)
    t.start()

    return RunnerState(thread=t, stop_event=stop_event, out_queue=out_q)


def stop_background_loop(state: RunnerState) -> None:
    if state.stop_event is not None:
        state.stop_event.set()
