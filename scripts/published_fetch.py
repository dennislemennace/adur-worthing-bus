"""Read a published file the way a browser does: asking for it compressed.

From 8 November 2026 each .json in the published bucket is stored
gzip-compressed (Content-Encoding: gzip), about an eighth of its size. The
Worker passes it on as stored to a client that accepts gzip and unpacks it
for one that does not; unpacking a 10 MB file costs more CPU than the free
Workers plan allows a request, so our own scripts ask for gzip and unpack it
here, never relying on the Worker to.

The bytes returned are the file as published, which is what the sha256 values
in index.json are of. A .json.gz file (observations, the delay-map evidence)
is not content-encoded and comes back exactly as stored.
"""
import gzip
import urllib.request

USER_AGENT = "bus-reliability-build"
GZIP_MAGIC = b"\x1f\x8b"


def read_published(url, timeout=60, user_agent=USER_AGENT, opener=None):
    """The file at `url`, unpacked if it was sent compressed.

    Named, as every download here is: the Worker refuses Python's default
    User-Agent with a 403."""
    request = urllib.request.Request(url, headers={"User-Agent": user_agent, "Accept-Encoding": "gzip"})
    with (opener or urllib.request.urlopen)(request, timeout=timeout) as response:
        raw = response.read()
        encoding = (response.headers.get("Content-Encoding") or "").strip().lower()
    return gzip.decompress(raw) if encoding == "gzip" else raw


def unpack_stored(raw):
    """A .json object read from the bucket itself (the storage API does not
    undo Content-Encoding), unpacked if it was stored compressed. Only for
    .json objects: a .json.gz file is gzip by name and must stay as it is."""
    return gzip.decompress(raw) if raw[:2] == GZIP_MAGIC else raw
