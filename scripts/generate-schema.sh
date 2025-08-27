#!/bin/sh

echo "📦 Updating the git submodules..."
git submodule update --remote

echo "⚙️  Generating the Python-code from the XML Schema..."
uv run xsdata generate -c domain/netex/conf/xsdata.conf domain/netex/schema/xsd/NeTEx_publication.xsd
