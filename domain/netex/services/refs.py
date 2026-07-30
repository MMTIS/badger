from typing import cast

from domain.netex import model
from domain.netex.model import CodespaceRefStructure, DataSourceRefStructure, VersionOfObjectRefStructure, NameOfClass
from domain.netex.services.model_typing import Tid, Tref


def getFakeRef(id: str, clazz: type[Tref], version: str | None, version_ref: str | None = None) -> Tref:
    assert id is not None, "A reference must start with a valid id"
    return clazz(
        ref=id,
        version=version if version_ref is None else None,
        version_ref=version_ref,
    )


def getRef(obj: Tid, clazz: type[Tref] | None = None) -> Tref | CodespaceRefStructure | DataSourceRefStructure:
    assert obj is not None, "A reference must be made from an existing object."

    if clazz is None:
        asobj = type(obj).__name__ + "Ref"  # Was: RefStructure
        clazz = cast(type[VersionOfObjectRefStructure], getattr(model, asobj))  # TODO: review

    assert clazz is not None, "Class is not none"

    if hasattr(obj, "id"):
        assert obj.id is not None, "Object does not have an id"
        instance = clazz(ref=obj.id)
    elif hasattr(obj, "ref"):
        assert obj.ref is not None, "Object does not have a ref"
        instance = clazz(ref=obj.ref)
    else:
        raise AssertionError("Object does not have an id or ref")

    if hasattr(instance, "order") and hasattr(obj, "order"):
        instance.order = obj.order

    name = type(obj).__name__
    if hasattr(obj, "Meta") and hasattr(obj.Meta, "name"):
        name = obj.Meta.name
    elif name.endswith("RefStructure"):
        name = name.replace("RefStructure", "Ref")

    if hasattr(instance, "version"):
        instance.version = getattr(obj, "version", None)

    kname = clazz.__name__
    meta_kname = clazz.__name__
    meta = getattr(clazz, "Meta", None)
    if meta and hasattr(meta, "name"):
        meta_kname = meta.name

    if issubclass(clazz, VersionOfObjectRefStructure) and not (kname.startswith(name) or meta_kname.startswith(name)):
        if hasattr(instance, "name_of_ref_class"):
            instance.name_of_ref_class = NameOfClass(name)
    return instance
