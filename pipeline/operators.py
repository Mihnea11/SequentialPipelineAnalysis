from typing import Callable, Iterable, Iterator, List

from core.models import Event


# -------------------------
# TYPE ALIASES (READABILITY)
# -------------------------

Predicate = Callable[[Event], bool]
Mapper = Callable[[Event], Event]


# -------------------------
# FILTER OPERATOR
# -------------------------

def filter_events(
    events: Iterable[Event],
    predicate: Predicate,
) -> Iterator[Event]:
    for event in events:
        if predicate(event):
            yield event


# -------------------------
# MAP / TRANSFORM OPERATOR
# -------------------------

def map_events(
    events: Iterable[Event],
    mapper: Mapper,
) -> Iterator[Event]:
    for event in events:
        yield mapper(event)


# -------------------------
# PIPELINE COMPOSITION
# -------------------------

def run_pipeline(
    events: Iterable[Event],
    operators: List[Callable[[Iterable[Event]], Iterable[Event]]],
) -> Iterator[Event]:
    stream: Iterable[Event] = events
    for operator in operators:
        stream = operator(stream)
    return iter(stream)
