#!/bin/sh

echo "📦 Updating the git submodules..."
git submodule update --remote

echo "⚙️  Generating the Python-code from the XML Schema..."
uv run xsdata generate -c conf/xsdata/netex.conf schema/netex/xsd/NeTEx_publication.xsd
