# Alternative implementation for attrgetter, handles list indices
# from operator import attrgetter
def resolve_attr(obj, attr):
    for name in attr:
        if isinstance(name, int):
            obj = obj[name]
        else:
            obj = getattr(obj, name)
    return obj


def update_attr(obj, attr, value):
    for name in attr[0:-1]:
        if isinstance(name, int):
            obj = obj[name]
        else:
            obj = getattr(obj, name)

    name = attr[-1]
    if isinstance(name, int):
        obj[name] = value
    else:
        setattr(obj, name, value)

    return obj
