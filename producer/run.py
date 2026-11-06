import logging
import random
import signal
import time

import requests
from confluent_kafka import Producer

from producer import config
from producer.sse import read_events
from producer.validate import InvalidEvent, parse_and_validate

log = logging.getLogger("producer")
stop_requested = False


def _request_stop(signum, frame):
    global stop_requested
    stop_requested = True
    log.info("Shutdown requested, finishing up...")


class Stats:
    def __init__(self):
        self.produced = self.invalid = self.delivered = 0
        self.delivery_errors = self.reconnects = 0
        self._last_time = time.monotonic()
        self._last_produced = 0

    def on_delivery(self, err, msg):
        if err is not None:
            self.delivery_errors += 1
            log.error("Delivery failed: %s", err)
        else:
            self.delivered += 1

    def maybe_report(self):
        now = time.monotonic()
        elapsed = now - self._last_time
        if elapsed >= config.STATS_INTERVAL_S:
            rate = (self.produced - self._last_produced) / elapsed
            log.info("rate=%.1f/s produced=%d delivered=%d invalid=%d "
                     "delivery_errors=%d reconnects=%d", rate, self.produced,
                     self.delivered, self.invalid, self.delivery_errors,
                     self.reconnects)
            self._last_time, self._last_produced = now, self.produced


def make_producer() -> Producer:
    return Producer({
        "bootstrap.servers": config.KAFKA_BOOTSTRAP,
        "client.id": "wiki-sse-producer",
        "enable.idempotence": True,    # retries can't create duplicates
        "acks": "all",
        "compression.type": "lz4",
        "linger.ms": 50,               # small wait so messages batch together
    })


def send(producer, topic, key, value, stats, headers=None):
    while True:
        try:
            producer.produce(topic, key=key, value=value, headers=headers,
                             on_delivery=stats.on_delivery)
            return
        except BufferError:            # local queue full: wait for it to drain
            producer.poll(0.5)


def handle(raw: str, producer, stats):
    try:
        event = parse_and_validate(raw)
    except InvalidEvent as e:
        stats.invalid += 1
        send(producer, config.DLQ_TOPIC, None, raw.encode(), stats,
             headers=[("error", str(e).encode())])
        return
    send(producer, config.TOPIC, event["wiki"].encode(), raw.encode(), stats)
    stats.produced += 1


def interruptible_sleep(seconds: float):
    end = time.monotonic() + seconds
    while not stop_requested and time.monotonic() < end:
        time.sleep(0.2)


def main():
    logging.basicConfig(level=logging.INFO,
                        format="%(asctime)s %(levelname)s %(name)s: %(message)s")
    signal.signal(signal.SIGINT, _request_stop)
    signal.signal(signal.SIGTERM, _request_stop)

    producer, stats = make_producer(), Stats()
    last_id, backoff = None, 1.0
    log.info("Starting: %s -> %s (%s)", config.STREAM_URL, config.TOPIC,
             config.KAFKA_BOOTSTRAP)

    while not stop_requested:
        try:
            for event_id, raw in read_events(config.STREAM_URL,
                                             config.USER_AGENT, last_id):
                backoff = 1.0                       # healthy connection: reset
                last_id = event_id or last_id
                handle(raw, producer, stats)
                producer.poll(0)                    # serve delivery callbacks
                stats.maybe_report()
                if stop_requested:
                    break
        except requests.RequestException as e:
            stats.reconnects += 1
            status = getattr(getattr(e, "response", None), "status_code", None)
            if status == 400:
                last_id = None                      # resume token rejected: start fresh
            wait = backoff + random.random()
            log.warning("Stream error (%s). Reconnecting in %.1fs", e, wait)
            interruptible_sleep(wait)
            backoff = min(backoff * 2, 60.0)
        else:
            if not stop_requested:
                log.warning("Stream closed by server. Reconnecting.")
                interruptible_sleep(1.0)

    remaining = producer.flush(15)
    log.info("Stopped. Unflushed messages: %d. Final: produced=%d delivered=%d "
             "invalid=%d delivery_errors=%d", remaining, stats.produced,
             stats.delivered, stats.invalid, stats.delivery_errors)


if __name__ == "__main__":
    main()