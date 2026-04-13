#!/usr/bin/env bash
# Capture a frozen snapshot of ~/.claude session data for import-perf benchmarking.
# Output: full.tar.zst, subset.tar.zst, MANIFEST.md in this directory.
# Idempotent: overwrites existing tarballs and manifest.

set -euo pipefail

here="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
cd "$here"

src_root="${HOME}/.claude"
projects_dir="${src_root}/projects"
history_file="${src_root}/history.jsonl"

if [[ ! -d "${projects_dir}" ]]; then
  echo "ERROR: ${projects_dir} does not exist" >&2
  exit 1
fi

staging="$(mktemp -d)"
trap 'rm -rf "${staging}"' EXIT

echo "Staging full corpus at ${staging}/full ..."
mkdir -p "${staging}/full"
cp -a "${projects_dir}" "${staging}/full/projects"
if [[ -f "${history_file}" ]]; then
  cp -a "${history_file}" "${staging}/full/history.jsonl"
fi

full_bytes="$(du -sb "${staging}/full" | awk '{print $1}')"
full_files="$(find "${staging}/full" -type f -name '*.jsonl' | wc -l)"
full_projects="$(find "${staging}/full/projects" -mindepth 1 -maxdepth 1 -type d | wc -l)"

echo "Creating full.tar.zst ..."
tar -C "${staging}/full" -cf - . | zstd -19 -T0 -o full.tar.zst --force
full_sha="$(sha256sum full.tar.zst | awk '{print $1}')"

echo "Selecting subset (largest projects, target min(5% of full, 100MB)) ..."
target_bytes=$(( full_bytes / 20 ))
max_bytes=$(( 100 * 1024 * 1024 ))
if (( target_bytes > max_bytes )); then
  target_bytes=${max_bytes}
fi

mkdir -p "${staging}/subset/projects"
if [[ -f "${staging}/full/history.jsonl" ]]; then
  cp -a "${staging}/full/history.jsonl" "${staging}/subset/history.jsonl"
fi

# List projects by size desc; add whole projects until we cross target_bytes.
mapfile -t sorted_projects < <(
  find "${staging}/full/projects" -mindepth 1 -maxdepth 1 -type d -print0 \
    | xargs -0 -I{} du -sb "{}" \
    | sort -rn \
    | awk '{print $2}'
)

accum=0
selected=0
for proj in "${sorted_projects[@]}"; do
  if (( accum >= target_bytes )); then
    break
  fi
  cp -a "${proj}" "${staging}/subset/projects/"
  proj_bytes="$(du -sb "${proj}" | awk '{print $1}')"
  accum=$(( accum + proj_bytes ))
  selected=$(( selected + 1 ))
done

subset_bytes="$(du -sb "${staging}/subset" | awk '{print $1}')"
subset_files="$(find "${staging}/subset" -type f -name '*.jsonl' | wc -l)"
subset_projects="${selected}"

echo "Creating subset.tar.zst ..."
tar -C "${staging}/subset" -cf - . | zstd -19 -T0 -o subset.tar.zst --force
subset_sha="$(sha256sum subset.tar.zst | awk '{print $1}')"

echo "Writing MANIFEST.md ..."
{
  echo "# Import Perf Corpus Snapshot Manifest"
  echo ""
  echo "Captured: $(date -u +%Y-%m-%dT%H:%M:%SZ)"
  echo "Host: $(uname -a)"
  echo "Filesystem (this directory): $(stat --file-system --format='%T' . 2>/dev/null || echo unknown)"
  echo ""
  echo "## Full corpus"
  echo "- path: full.tar.zst"
  echo "- uncompressed_bytes: ${full_bytes}"
  echo "- jsonl_file_count: ${full_files}"
  echo "- project_count: ${full_projects}"
  echo "- sha256: ${full_sha}"
  echo ""
  echo "## Subset"
  echo "- path: subset.tar.zst"
  echo "- selection: largest projects by bytes until >= min(5% full, 100MB)"
  echo "- uncompressed_bytes: ${subset_bytes}"
  echo "- jsonl_file_count: ${subset_files}"
  echo "- project_count: ${subset_projects}"
  echo "- sha256: ${subset_sha}"
} > MANIFEST.md

echo "Done."
echo "  full.tar.zst    $(du -h full.tar.zst | awk '{print $1}')   sha256=${full_sha}"
echo "  subset.tar.zst  $(du -h subset.tar.zst | awk '{print $1}') sha256=${subset_sha}"
