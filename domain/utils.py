from domain.netex.model import EntityStructure

def get_object_name(clazz: type[EntityStructure]) -> str:
    return getattr(getattr(clazz, "Meta", None), "name", str(clazz.__name__))
