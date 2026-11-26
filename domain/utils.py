from domain.netex.model import EntityStructure

def get_not_netex(clazz: type[EntityStructure]) -> bool:
    meta = getattr(clazz, "Meta", None)
    if meta:
        namespace = getattr(meta, "namespace", None)
        if namespace and namespace != 'http://www.netex.org.uk/netex':
            return True

    return False

def get_object_name(clazz: type[EntityStructure]) -> str:
    meta = vars(clazz).get("Meta")
    return getattr(meta, "name", clazz.__name__)
