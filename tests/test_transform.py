import json
from datetime import UTC, datetime

from spark_jobs.transform import parse_events, to_quarantine, to_silver

NOW = int(datetime.now(UTC).timestamp())


def make_event(**overrides):
    event = {
        "type": "edit", "wiki": "enwiki", "namespace": 0, "title": "Some page",
        "user": "someone", "bot": False, "minor": False, "timestamp": NOW,
        "length": {"old": 100, "new": 140}, "server_name": "en.wikipedia.org",
        "meta": {"id": "evt-1", "domain": "en.wikipedia.org"},
    }
    event.update(overrides)
    return event


def without(event, *path):
    event = json.loads(json.dumps(event))
    target = event
    for key in path[:-1]:
        target = target[key]
    del target[path[-1]]
    return event


def bronze_df(spark, raw_values):
    rows = [(0, i, datetime(2026, 1, 1), raw) for i, raw in enumerate(raw_values)]
    return spark.createDataFrame(
        rows, "kafka_partition int, kafka_offset long, ingested_at timestamp, raw_json string")


# (name, raw input, expected validation_error)
CASES = [
    ("valid event", make_event(), None),
    ("malformed json", "this is not json", "unparseable_json"),
    ("empty string", "", "unparseable_json"),
    ("missing meta.id", without(make_event(), "meta", "id"), "missing_event_id"),
    ("missing wiki", without(make_event(), "wiki"), "missing_wiki"),
    ("missing type", without(make_event(), "type"), "missing_type"),
    ("missing timestamp", without(make_event(), "timestamp"), "missing_timestamp"),
    ("year 1970", make_event(timestamp=86400), "implausible_time_past"),
    ("three days ahead", make_event(timestamp=NOW + 3 * 86400), "implausible_time_future"),
]


def test_validation_classification(spark):
    raws = [json.dumps(raw) if isinstance(raw, dict) else raw for _, raw, _ in CASES]
    rows = parse_events(bronze_df(spark, raws)).select("kafka_offset", "validation_error").collect()
    actual = {r.kafka_offset: r.validation_error for r in rows}
    for offset, (name, _, expected) in enumerate(CASES):
        assert actual[offset] == expected, f"{name}: expected {expected}, got {actual[offset]}"


def test_silver_keeps_only_valid_rows_and_derives_fields(spark):
    no_bot_flag = without(make_event(meta={"id": "evt-2"}), "bot")
    raws = [json.dumps(make_event()), json.dumps(no_bot_flag), "this is not json"]
    silver = to_silver(parse_events(bronze_df(spark, raws))).collect()

    assert len(silver) == 2
    by_id = {r.event_id: r for r in silver}
    assert by_id["evt-1"].byte_delta == 40
    assert by_id["evt-1"].wiki == "enwiki"
    assert by_id["evt-2"].is_bot is False        # missing bot flag defaults to False


def test_quarantine_holds_only_invalid_rows_with_reasons(spark):
    raws = [json.dumps(make_event()), "this is not json",
            json.dumps(without(make_event(), "type"))]
    quarantine = to_quarantine(parse_events(bronze_df(spark, raws))).collect()

    assert sorted(r.reason for r in quarantine) == ["missing_type", "unparseable_json"]
    assert all(r.raw_json for r in quarantine)   # original payload is preserved