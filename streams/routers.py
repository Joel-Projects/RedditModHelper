def route_task(name, args, kwargs, options, task=None, **kw):
    if name == "streams.tasks.ingest_action_chunk":
        return {"queue": "actions", "priority": (2 if args[1] else 1) + (2 if args[2] else 0)}
