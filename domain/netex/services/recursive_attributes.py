from functools import lru_cache
from typing import Generator, Any, Hashable
from domain.netex import model as netex
from domain.netex.services.model_typing import Tid
from storage.interface import Serializer

# from dataclasses import fields


@lru_cache(maxsize=None)
def _dc_field_names(cls: Hashable) -> tuple[str, ...]:
    return tuple(cls.__dataclass_fields__.keys())  # type: ignore
    # return tuple(f.name for f in fields(cls))


def recursive_attributes(obj: Tid, depth: list[int]) -> Generator[tuple[Any, tuple[int, ...]], None, None]:
    # We skip data_source_ref_attribute and  responsibility_set_ref_attribute later in the pipeline
    # data_source_ref_attribute = getattr(obj, "data_source_ref_attribute", None)
    # if data_source_ref_attribute:
    #     yield DataSourceRefStructure(ref=data_source_ref_attribute), depth + ["data_source_ref_attribute"]

    # responsibility_set_ref_attribute = getattr(obj, "responsibility_set_ref_attribute", None)
    # if responsibility_set_ref_attribute:
    #     yield ResponsibilitySetRef(ref=responsibility_set_ref_attribute), depth + ["responsibility_set_ref_attribute"]

    mydepth = depth
    mydepth.append(0)
    field_names = _dc_field_names(obj.__class__)  # type: ignore
    for col_idx, field_name in enumerate(field_names):
        mydepth[-1] = col_idx
        v = getattr(obj, field_name, None)
        if v is not None:
            if v.__class__ in netex.set_ref_types:  # type: ignore
                yield v, tuple(mydepth)

            else:
                if hasattr(v, "__dataclass_fields__") and v.__class__.__name__ in netex.set_all:  # type: ignore
                    if hasattr(v, "id"):
                        yield v, tuple(mydepth)
                    yield from recursive_attributes(v, mydepth)
                elif v.__class__ in (list, tuple):
                    mydepth.append(0)
                    for j, x in enumerate(v):
                        mydepth[-1] = j
                        if x is not None:
                            if x.__class__ in netex.set_ref_types:  # type: ignore
                                yield x, tuple(mydepth)  # TODO: mydepth result is incorrect when list() but not as iterator
                            elif hasattr(x, "__dataclass_fields__") and x.__class__.__name__ in netex.set_all:  # type: ignore
                                if hasattr(x, "id"):
                                    yield x, tuple(mydepth)
                                yield from recursive_attributes(x, mydepth)
                    mydepth.pop()
    mydepth.pop()


def only_references(deserialized: Tid, serializer: Serializer) -> Generator[tuple[type, str, str], None, None]:
    assert deserialized.id is not None, "deserialised.id must not be none"

    for obj, path in recursive_attributes(deserialized, []):
        if hasattr(obj, "ref"):
            assert obj.ref is not None, "Object ref must not be none"
            if obj.name_of_ref_class is None:
                # Hack, because NeTEx does not define the default name of ref class yet
                if obj.__class__.__name__.endswith("RefStructure"):
                    obj.name_of_ref_class = obj.__class__.__name__[0:-12]
                elif obj.__class__.__name__.endswith("Ref"):
                    obj.name_of_ref_class = obj.__class__.__name__[0:-3]

            if obj.name_of_ref_class not in serializer.name_object.keys():
                # log_once(logging.WARN, "unknown name_of_ref_class", "Reference Class cannot be found in serializer")
                continue

            yield (
                serializer.name_object[obj.name_of_ref_class],  # The object that the reference is towards
                obj.ref,
                getattr(obj, "version", "any"),
            )
