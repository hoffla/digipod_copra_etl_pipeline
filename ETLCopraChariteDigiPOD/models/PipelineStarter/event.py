subscribers = dict()


def subscribe(event_type: str, func):
    if not event_type in subscribers:
        subscribers[event_type] = []
    subscribers[event_type].append(func)


def post_event(event_type, *args, **kwargs):
    if not event_type in subscribers:
        return
    for func in subscribers[event_type]:
        return func(*args, **kwargs)
