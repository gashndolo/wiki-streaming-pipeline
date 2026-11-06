import json
import time
import uuid

from confluent_kafka import Producer

from producer import config

WIKI = "pipeline_test"


def make_event(**overrides):
    event = {
        "type": "edit", "wiki": WIKI, "namespace": 0,
        "title": "Pipeline test page", "user": "pipeline-test-injector",
        "bot": False, "minor": False, "timestamp": int(time.time()),
        "length": {"old": 100, "new": 140}, "server_name": "test.example.org",
        "meta": {"id": f"inject-{uuid.uuid4()}", "domain": "test.example.org"},
    }
    event.update(overrides)
    return event


def main():
    p = Producer({"bootstrap.servers": config.KAFKA_BOOTSTRAP})

    def send(raw: str):
        p.produce(config.TOPIC, key=WIKI.encode(), value=raw.encode())

    good = json.dumps(make_event())
    send(good)
    send(good)                                                    # exact duplicate

    send(json.dumps(make_event(timestamp=86400)))                 # year 1970
    no_type = make_event()
    del no_type["type"]
    send(json.dumps(no_type))                                     # missing field
    send("this is not json")                                      # malformed
    p.flush(10)
    print("Injected 5 messages: 1 valid (x2 duplicate), 3 bad.")


if __name__ == "__main__":
    main()