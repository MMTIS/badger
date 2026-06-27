from domain.netex.model import Authority, AuthorityRef, MultilingualString, Operator, TextType

from storage.mdbx.core.implementation import MdbxStorage

from tests.base import MdbxStorageTestCase


def _insert_authority_and_operator(storage: MdbxStorage) -> None:
    # Same id, different class: the lookup must disambiguate on the ref class.
    storage.insert_objects_on_queue(Authority, [Authority(id="1", version="1", name=MultilingualString(content=[TextType(value="test")]))])
    storage.insert_objects_on_queue(Operator, [Operator(id="1", version="1", name=MultilingualString(content=[TextType(value="test")]))])


class TestFetchByReference(MdbxStorageTestCase):
    def test_fetch_by_reference_with_name_of_ref_class(self) -> None:
        _insert_authority_and_operator(self.storage)

        with self.storage.env.ro_transaction() as txn_read:
            ref = AuthorityRef(ref="1", version="1")  # name_of_ref_class defaults to "Authority"
            authority = self.storage.load_object_by_reference(txn_read, ref)

        self.assertIsInstance(authority, Authority)
        assert authority is not None
        self.assertEqual(authority.id, "1")
        self.assertEqual(authority.version, "1")

    def test_fetch_by_reference_fallback_without_name_of_ref_class(self) -> None:
        _insert_authority_and_operator(self.storage)

        with self.storage.env.ro_transaction() as txn_read:
            ref = AuthorityRef(ref="1", version="1")
            # Deliberately drop the class hint to force the prefix-scan fallback.
            ref.name_of_ref_class = None  # type: ignore[assignment]
            result = self.storage.load_object_by_reference(txn_read, ref)

        # The fallback is a prefix scan over the id index: it returns whichever
        # object with this id comes first, regardless of class.
        self.assertIsNotNone(result)
        assert result is not None
        self.assertEqual(result.id, "1")
        self.assertIsInstance(result, (Authority, Operator))
