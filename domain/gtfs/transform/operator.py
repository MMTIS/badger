from typing import Generator

import duckdb

from domain.netex.model import (
    Operator,
    PrivateCodes,
    PrivateCode,
    MultilingualString,
    Locale,
    ContactStructure,
    LocaleStructure,
    LanguageUsageStructure,
    LanguageUseEnumeration,
    TextType,
    Codespace,
)
from domain.netex.services.ids import getId


def get_agency_id(codespace: Codespace, agency_id: str) -> str:
    if ':Operator:' in agency_id:
        return agency_id
    else:
        return getId(codespace, Operator, agency_id)


def getOperators(con: duckdb.DuckDBPyConnection, codespace: Codespace, version: str) -> Generator[Operator, None, None]:
    operators_sql = """SELECT agency_id, agency_name, agency_lang, agency_email, agency_timezone, agency_url, agency_phone FROM agency;"""

    with con.cursor() as cur:
        cur.execute(operators_sql)

        while True:
            row = cur.fetchone()
            if row is None:
                break

            (
                agency_id,
                agency_name,
                agency_lang,
                agency_email,
                agency_timezone,
                agency_url,
                agency_phone,
            ) = row

            language: str | None = agency_lang
            if language:
                languages = LocaleStructure.Languages(
                    language_usage=[LanguageUsageStructure(language=language, language_use=[LanguageUseEnumeration.NORMALLY_USED])]
                )
            else:
                languages = None

            operator = Operator(
                id=get_agency_id(codespace, agency_id),
                private_codes=PrivateCodes(private_code=[PrivateCode(value=agency_id, type_value="agency_id")]),
                version=version,
                name=MultilingualString(content=[TextType(value=agency_name)]),
                locale=Locale(time_zone=agency_timezone, languages=languages),
                customer_service_contact_details=ContactStructure(url=agency_url, phone=agency_phone, email=agency_email),
            )

            yield operator
