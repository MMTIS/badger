import unittest
from domain.netex.model import MultilingualString, TextType, LocaleStructure, AlternativeTextsRelStructure, ScheduledStopPoint

from transformers.multilingualstring import TransformMultilingualString
from typing import cast

VALUE = "Hello World"
VALUE_NL = "Hallo Wereld"


class TestMultilingualString(unittest.TestCase):
    def test_multilingualstring(self) -> None:
        mls = MultilingualString(content=[VALUE])
        _m, changed = TransformMultilingualString.to_v2(mls)
        self.assertEqual(mls.content, [TextType(value=VALUE)])
        self.assertEqual(changed, True)

        _m, changed = TransformMultilingualString.to_v2(mls)
        self.assertEqual(mls.content, [TextType(value=VALUE)])
        self.assertEqual(changed, False)

        _m, alternative_texts, changed = TransformMultilingualString.to_v1(mls)
        self.assertEqual(mls.content, [VALUE])
        self.assertEqual(alternative_texts, [])
        self.assertEqual(changed, True)

        mls = MultilingualString(content=[TextType(value=VALUE), TextType(value=VALUE_NL, lang="nl")])
        _m, alternative_texts, changed = TransformMultilingualString.to_v1(mls)
        self.assertEqual(mls.content, [VALUE])
        self.assertEqual(alternative_texts[0].text.content, [VALUE_NL])
        self.assertEqual(alternative_texts[0].use_for_language, "nl")
        self.assertEqual(changed, True)

        mls = MultilingualString(content=[VALUE, TextType(value=VALUE), TextType(value=VALUE_NL, lang="nl")])
        _m, alternative_texts, changed = TransformMultilingualString.to_v1(mls)
        self.assertEqual(mls.content, [VALUE])
        self.assertEqual(alternative_texts[0].text.content, [VALUE])
        self.assertEqual(alternative_texts[1].text.content, [VALUE_NL])
        self.assertEqual(alternative_texts[1].use_for_language, "nl")
        self.assertEqual(changed, True)

        _m, alternative_texts, changed = TransformMultilingualString.to_v1(mls)
        self.assertEqual(mls.content, [VALUE])
        self.assertEqual(alternative_texts, [])
        self.assertEqual(changed, False)

        default_locale = LocaleStructure(default_language="nl")
        mls = MultilingualString(content=[TextType(value=VALUE), TextType(value=VALUE_NL, lang="nl")])
        _m, alternative_texts, changed = TransformMultilingualString.to_v1(mls, default_locale)
        self.assertEqual(mls.content, [VALUE_NL])
        self.assertEqual(alternative_texts[0].text.content, [VALUE])
        self.assertEqual(changed, True)

        ats = AlternativeTextsRelStructure(alternative_text=alternative_texts)
        _m, changed = TransformMultilingualString.to_v2(mls, ats)
        self.assertEqual(mls.content, [TextType(value=VALUE_NL), TextType(value=VALUE)])
        self.assertEqual(changed, True)

        mls = MultilingualString(content=[TextType(value=VALUE), TextType(value=VALUE_NL, lang="nl")])
        ssp = ScheduledStopPoint(id="1", version="1", name=mls)
        _ssp, changed = TransformMultilingualString.obj_to_v1(ssp)
        self.assertEqual(mls.content, [VALUE])
        self.assertEqual(cast(AlternativeTextsRelStructure, ssp.alternative_texts).alternative_text[0].text.content, [VALUE_NL])
        self.assertEqual(cast(AlternativeTextsRelStructure, ssp.alternative_texts).alternative_text[0].use_for_language, "nl")
        self.assertEqual(changed, True)

        _ssp, changed = TransformMultilingualString.obj_to_v2(ssp)
        self.assertEqual(cast(MultilingualString, ssp.name).content, [TextType(value=VALUE), TextType(value=VALUE_NL, lang="nl")])
        self.assertEqual(ssp.alternative_texts, None)
        self.assertEqual(changed, True)
