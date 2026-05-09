from uuid import uuid4


def generate_sentinel_id() -> str:
    """Generate a unique correlation ID for a new execution trace.

    Creates a UUID v4 with a ``sent-`` prefix. Call this once at the entry
    point Lambda and propagate the returned value through every downstream
    payload so all events in the execution share the same ID.

    Returns:
        A string of the form ``sent-<uuid4>``, e.g.
        ``"sent-3f2a1b4c-8e9d-4f7a-b6c5-1d2e3f4a5b6c"``.

    Example:
        >>> sentinel_id = generate_sentinel_id()
        >>> assert sentinel_id.startswith("sent-")
    """
    return 'sent-' + str(uuid4())
