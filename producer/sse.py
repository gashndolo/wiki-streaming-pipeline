from collections.abc import Iterator

import requests


def read_events(url: str, user_agent: str, last_event_id: str | None = None
                ) -> Iterator[tuple[str | None, str]]:
    """Yield (event_id, raw_data) pairs from a Server-Sent Events endpoint."""
    headers = {"User-Agent": user_agent, "Accept": "text/event-stream"}
    if last_event_id:
        headers["Last-Event-ID"] = last_event_id

    # (connect timeout, read timeout): a silent connection is treated as dead
    with requests.get(url, headers=headers, stream=True, timeout=(10, 120)) as resp:
        resp.raise_for_status()
        resp.encoding = "utf-8"

        event_id = None
        data_lines: list[str] = []
        for line in resp.iter_lines(decode_unicode=True):
            if line is None:
                continue
            if line == "":                       # blank line = end of one event
                if data_lines:
                    yield event_id, "\n".join(data_lines)
                data_lines = []
                continue
            if line.startswith(":"):             # comment / keepalive
                continue
            field, _, value = line.partition(":")
            value = value[1:] if value.startswith(" ") else value
            if field == "data":
                data_lines.append(value)
            elif field == "id":
                event_id = value