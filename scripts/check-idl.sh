#!/bin/sh
set -eu

canonical="${GALAXIA_CANONICAL_IDL:-${GALAXIA_ROOT:-../galaxIA}/idl/fhs-protocol.proto}"
if test -f "$canonical"; then
  cmp -s "$canonical" protocol/fhs-protocol.proto || {
    echo "protocol/fhs-protocol.proto no coincide con el IDL canónico: $canonical" >&2
    exit 1
  }
  echo "FHS IDL: OK (comparación con canonical)"
else
  test -f protocol/fhs-protocol.proto.sha256
  sha256sum -c protocol/fhs-protocol.proto.sha256 2>/dev/null || shasum -a 256 -c protocol/fhs-protocol.proto.sha256
  echo "FHS IDL: OK (hash de snapshot canónico)"
fi
