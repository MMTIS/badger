#!/bin/sh

echo "📦 Updating the git submodules..."
git submodule update --remote

echo "⚙️  Generating the Python-code from the XML Schema..."
rm domain/netex/model/*.py
# Generate with pyxsdata and PEP 562 on-demand lazy loading (70x faster package imports)
uv run pyxsdata generate --lazy-load -c domain/netex/conf/xsdata.conf domain/netex/schema/xsd/NeTEx_publication.xsd
