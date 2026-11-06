import json

REQUIRED_FIELDS = ("type", "wiki", "timestamp", "meta")


class InvalidEvent(ValueError):
    """Raised when an event can't be used downstream. The message is the reason."""


def parse_and_validate(raw: str) -> dict:
    try:
        event = json.loads(raw)
    except json.JSONDecodeError as e:
        raise InvalidEvent(f"invalid_json: {e.msg}") from e

    if not isinstance(event, dict):
        raise InvalidEvent("not_an_object")

    for field in REQUIRED_FIELDS:
        if field not in event:
            raise InvalidEvent(f"missing_field: {field}")

    if not isinstance(event["wiki"], str) or not event["wiki"]:
        raise InvalidEvent("bad_wiki")
    if not isinstance(event["meta"], dict) or "id" not in event["meta"]:
        raise InvalidEvent("missing_field: meta.id")
    if not isinstance(event["timestamp"], (int, float)):
        raise InvalidEvent("bad_timestamp")

    return event