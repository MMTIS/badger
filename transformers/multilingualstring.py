from domain.netex.model import MultilingualString, AlternativeText, TextType, LocaleStructure, AlternativeTextsRelStructure, EntityStructure
from domain.netex.services.model_typing import Tid
from domain.netex.services.recursive_attributes import recursive_attributes_all
from collections.abc import Generator
from storage.mdbx.core.implementation import MdbxStorage
from mdbx.mdbx import TXN


class TransformMultilingualString:
    @staticmethod
    def obj_to_v1(deserialized: Tid, default_locale: LocaleStructure | None = None) -> tuple[Tid, bool]:
        # TODO: This function would walk over the class iteratively.
        # A general optimisation would be to precompute the paths within
        # a class to directly have a list (per class) of possible location targets
        is_changed = False
        for parent, obj, _path in recursive_attributes_all(deserialized, [], interesting_classes=(MultilingualString,)):
            _, alternative_texts, changed = TransformMultilingualString.to_v1(obj, default_locale)
            is_changed |= changed
            if changed and len(alternative_texts) > 0:
                if hasattr(parent, "alternative_texts"):
                    if parent.alternative_texts is None:
                        parent.alternative_texts = AlternativeTextsRelStructure(alternative_text=alternative_texts)
                    else:
                        parent.alternative_texts.alternative_text.extend(alternative_texts)
        return deserialized, is_changed

    @staticmethod
    def iter_to_v1(
        db: MdbxStorage, txn: TXN, default_locale: LocaleStructure | None = None, only_changed: bool = True
    ) -> Generator[EntityStructure, None, None]:
        # Within this function we are reading and writing towards the target database.
        # This effectively means that if we would need to resize for whatever reason,
        # we cannot hold the cursor since access has to be disabled.
        # We will first validate that we do have remaining capacity.

        clazz: type[EntityStructure]
        for clazz in set(db.db_names(txn).values()):
            obj: EntityStructure
            for _key, obj in db.iter_objects(txn, clazz):
                _new_obj, changed = TransformMultilingualString.obj_to_v1(obj, default_locale)
                if only_changed:
                    if changed:
                        yield obj
                else:
                    yield obj

    @staticmethod
    def to_v2(mls: MultilingualString) -> tuple[MultilingualString, bool]:
        if len(mls.content) > 0:
            if isinstance(mls.content[0], TextType):
                return mls, False

            else:
                assert isinstance(mls.content[0], str)
                mls.content = [TextType(value=mls.content[0])]
                return mls, True

        return mls, False

    @staticmethod
    def to_v1(mls: MultilingualString, ls: LocaleStructure | None = None) -> tuple[MultilingualString, list[AlternativeText], bool]:
        if len(mls.content) > 0:
            if isinstance(mls.content[0], TextType):
                content: str | None = None
                ats: list[AlternativeText] = []
                if len(mls.content) > 1:
                    if ls is not None and ls.default_language is not None:
                        for tt in mls.content:
                            assert isinstance(tt, TextType)
                            if tt.lang == ls.default_language:
                                content = tt.value
                            else:
                                ats.append(AlternativeText(text=MultilingualString(content=[tt.value]), order=1, use_for_language=tt.lang))
                    else:
                        assert isinstance(mls.content[0], TextType)
                        content = mls.content[0].value
                        for tt in mls.content[1:]:
                            assert isinstance(tt, TextType)
                            ats.append(AlternativeText(text=MultilingualString(content=[tt.value]), order=1, use_for_language=tt.lang))

                if content is None:
                    content = mls.content[0].value
                mls.content = [content]
                return mls, ats, True

        return mls, [], False
