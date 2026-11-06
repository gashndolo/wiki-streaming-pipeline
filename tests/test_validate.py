import json

import pytest

from producer.validate import InvalidEvent, parse_and_validate


def good_event(**overrides):
    event = {"type": "edit", "wiki": "enwiki", "timestamp": 1730000000,
             "meta": {"id": "abc-123"}, "user": "Someone", "bot": False}
    event.update(overrides)
    return event


def test_valid_event_passes():
    event = parse_and_validate(json.dumps(good_event()))
    assert event["wiki"] == "enwiki"


@pytest.mark.parametrize("raw, reason", [
    ("not json at all", "invalid_json"),
    ("[1, 2, 3]", "not_an_object"),
    (json.dumps({"type": "edit"}), "missing_field"),
    (json.dumps(good_event(wiki="")), "bad_wiki"),
    (json.dumps(good_event(timestamp="yesterday")), "bad_timestamp"),
    (json.dumps(good_event(meta={})), "missing_field: meta.id"),
])
def test_invalid_events_are_rejected(raw, reason):
    with pytest.raises(InvalidEvent) as excinfo:
        parse_and_validate(raw)
    assert reason in str(excinfo.value)