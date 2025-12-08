from typing import cast

from domain.netex import model
from domain.netex.model import CodespaceRefStructure, DataSourceRefStructure, VersionOfObjectRefStructure
from domain.netex.services.model_typing import Tid, Tref


def getFakeRef(id: str, klass: type[Tref], version: str | None, version_ref: str | None = None) -> Tref:
    assert id is not None, "A reference must start with a valid id"
    return klass(
        ref=id,
        version=version if version_ref is None else None,
        version_ref=version_ref,
    )


def getRef(
    obj: Tid, klass: type[Tref] | None = None
) -> Tref | CodespaceRefStructure | DataSourceRefStructure:
    assert obj is not None, "A reference must be made from an existing object."

    if klass is None:
        asobj = type(obj).__name__ + "Ref"  # Was: RefStructure
        klass = cast(type[VersionOfObjectRefStructure], getattr(model, asobj))  # TODO: review

    assert klass is not None, "Class is not none"

    if hasattr(obj, "id"):
        assert obj.id is not None, "Object does not have an id"
        instance = klass(ref=obj.id)
    elif hasattr(obj, "ref"):
        assert obj.ref is not None, "Object does not have a ref"
        instance = klass(ref=obj.ref)
    else:
        assert False, "Object does not have an id or ref"

    if hasattr(instance, "order") and hasattr(obj, "order"):
        instance.order = obj.order

    name = type(obj).__name__
    if hasattr(obj, "Meta") and hasattr(obj.Meta, "name"):
        name = obj.Meta.name
    elif name.endswith("RefStructure"):
        name = name.replace("RefStructure", "Ref")

    if hasattr(instance, "version"):
        instance.version = getattr(obj, "version", None)

    kname = klass.__name__
    meta_kname = klass.__name__
    meta = getattr(klass, "Meta", None)
    if meta and hasattr(meta, "name"):
        meta_kname = meta.name

    if issubclass(klass, VersionOfObjectRefStructure) and not (kname.startswith(name) or meta_kname.startswith(name)):
        if hasattr(instance, "name_of_ref_class"):
            instance.name_of_ref_class = name
    return instance
