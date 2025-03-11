from netexio.attributes import update_attr, resolve_attr
from utils.refs import getRef

def split_path(path) -> list[str]:
    split = []
    for p in path.split('.'):
        if p.isnumeric():
            p = int(p)
        split.append(p)
    return split

def replace_with_reference_inplace(obj, path, klass=None):
    split = split_path(path)

    attribute = resolve_attr(obj, split)

    # This does the assumption that the caller knows references would be allowed as type
    update_attr(obj, split, getRef(attribute, klass=klass))
