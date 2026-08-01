import unittest
from domain.netex.model import MultilingualString, TextType, LocaleStructure, AlternativeTextsRelStructure

from transformers.multilingualstring import TransformMultilingualString

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
