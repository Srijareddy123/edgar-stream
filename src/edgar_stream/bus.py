"""Message bus: an in-process queue for demo and tests, Redpanda (Kafka API) for the stack."""

from __future__ import annotations

import contextlib
import queue
import time
import uuid
from dataclasses import dataclass
from typing import Any, Protocol

from .events import FilingEvent

TOPIC = "edgar.filings"
PARTITIONS = 3


@dataclass
class Message:
    event: FilingEvent
    raw: Any = None  # the Kafka message, used for offset commits


class Bus(Protocol):
    def publish(self, event: FilingEvent) -> None: ...
    def poll(self, max_n: int, timeout_s: float) -> list[Message]: ...
    def commit(self, msgs: list[Message]) -> None: ...
    def flush(self) -> None: ...
    def close(self) -> None: ...


class MemoryBus:
    def __init__(self) -> None:
        self._q: queue.Queue[FilingEvent] = queue.Queue()

    def publish(self, event: FilingEvent) -> None:
        self._q.put(event)

    def poll(self, max_n: int, timeout_s: float) -> list[Message]:
        out: list[Message] = []
        deadline = time.monotonic() + timeout_s
        while len(out) < max_n:
            try:
                out.append(Message(self._q.get(timeout=max(0.0, deadline - time.monotonic()))))
            except queue.Empty:
                break
        return out

    def commit(self, msgs: list[Message]) -> None:
        return None

    def flush(self) -> None:
        return None

    def close(self) -> None:
        return None


class KafkaBus:
    """Producer and consumer on one topic. Offsets are committed only after the warehouse commit."""

    def __init__(self, bootstrap: str, group: str | None = None, topic: str = TOPIC) -> None:
        from confluent_kafka import Consumer, Producer
        from confluent_kafka.admin import AdminClient, NewTopic

        self._topic = topic
        admin = AdminClient({"bootstrap.servers": bootstrap})
        for fut in admin.create_topics([NewTopic(topic, PARTITIONS, 1)]).values():
            with contextlib.suppress(Exception):  # topic already exists is the normal case
                fut.result()
        self._producer = Producer({"bootstrap.servers": bootstrap, "linger.ms": 5})
        self._consumer = Consumer(
            {
                "bootstrap.servers": bootstrap,
                "group.id": group or f"edgar-loader-{uuid.uuid4().hex[:8]}",
                "enable.auto.commit": False,
                "auto.offset.reset": "latest",
            }
        )
        self._consumer.subscribe([topic])
        self._wait_for_assignment()

    def _wait_for_assignment(self, timeout_s: float = 30.0) -> None:
        """Join the group before anything is published so a fresh run starts at the log end."""
        deadline = time.monotonic() + timeout_s
        while not self._consumer.assignment() and time.monotonic() < deadline:
            self._consumer.poll(0.2)

    def publish(self, event: FilingEvent) -> None:
        self._producer.produce(self._topic, key=str(event.cik), value=event.to_json())
        self._producer.poll(0)

    def poll(self, max_n: int, timeout_s: float) -> list[Message]:
        out: list[Message] = []
        for m in self._consumer.consume(num_messages=max_n, timeout=timeout_s):
            if m.error():
                continue
            out.append(Message(FilingEvent.from_json(m.value()), m))
        return out

    def commit(self, msgs: list[Message]) -> None:
        from confluent_kafka import TopicPartition

        latest: dict[tuple[str, int], int] = {}
        for m in msgs:
            if m.raw is not None:
                k = (m.raw.topic(), m.raw.partition())
                latest[k] = max(latest.get(k, -1), m.raw.offset())
        if latest:
            offsets = [TopicPartition(t, p, o + 1) for (t, p), o in latest.items()]
            self._consumer.commit(offsets=offsets, asynchronous=False)

    def flush(self) -> None:
        self._producer.flush()

    def close(self) -> None:
        self._consumer.close()
