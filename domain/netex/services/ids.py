from domain.netex.model import Codespace
from domain.netex.services.model_typing import Tid
from domain.utils import get_object_name


def getId(codespace: Codespace, clazz: type[Tid], id: str) -> str:
    name = get_object_name(clazz)
    return "{}:{}:{}".format(codespace.xmlns, name, str(id).replace(":", "-"))
