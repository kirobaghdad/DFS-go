#!/usr/bin/env bash

set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
DOCS_DIR="${ROOT_DIR}/docs"
SOURCE_MD="${DOCS_DIR}/DFS_Guide.md"
TEMP_ODT="${DOCS_DIR}/DFS_Guide.odt"

pandoc "${SOURCE_MD}" -o "${TEMP_ODT}"
libreoffice -env:UserInstallation=file:///tmp/libreoffice-dfs-guide --headless --convert-to pdf "${TEMP_ODT}" --outdir "${DOCS_DIR}" >/dev/null
rm -f "${TEMP_ODT}"

echo "Built ${DOCS_DIR}/DFS_Guide.pdf"
