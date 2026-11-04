from domain.netex.model import EntityStructure

def get_not_netex(clazz: type[EntityStructure]) -> bool:
    meta = getattr(clazz, "Meta", None)
    if meta:
        namespace = getattr(meta, "namespace", None)
        if namespace and namespace != 'http://www.netex.org.uk/netex':
            return True

    return False

def get_object_name(clazz: type[EntityStructure]) -> str:
    return getattr(getattr(clazz, "Meta", None), "name", str(clazz.__name__))
