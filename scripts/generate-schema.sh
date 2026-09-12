#!/bin/sh

echo "📦 Updating the git submodules..."
git submodule update --remote

echo "⚙️  Generating the Python-code from the XML Schema..."
rm domain/netex/model/*.py
# Generate with pyxsdata (with --lazy-load for 70x faster imports) if installed, otherwise fallback to xsdata
if uv run python -c "import pyxsdata" 2>/dev/null; then
    uv run pyxsdata generate --lazy-load -c domain/netex/conf/xsdata.conf domain/netex/schema/xsd/NeTEx_publication.xsd
else
    uv run xsdata generate -c domain/netex/conf/xsdata.conf domain/netex/schema/xsd/NeTEx_publication.xsd
fi

