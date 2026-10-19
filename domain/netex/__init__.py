import typing

if typing.TYPE_CHECKING:
    from .model import *  # noqa: F403


def __getattr__(name: str):
    if name == "model":
        import domain.netex.model as model

        globals()["model"] = model
        return model

    import domain.netex.model as model

    attr = getattr(model, name)
    globals()[name] = attr
    return attr


def __dir__():
    import domain.netex.model as model

    return dir(model)
