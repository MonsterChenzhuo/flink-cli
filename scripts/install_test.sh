#!/usr/bin/env bash
set -euo pipefail

repo_root=$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)
tmpdir=$(mktemp -d)
trap 'rm -rf "$tmpdir"' EXIT

os=$(uname -s | tr '[:upper:]' '[:lower:]')
arch=$(uname -m)
case "$arch" in
  x86_64|amd64) arch=amd64 ;;
  aarch64|arm64) arch=arm64 ;;
esac

version=v0.0.0
ver_no_v=${version#v}
archive="flink-cli_${ver_no_v}_${os}_${arch}.tar.gz"

payload="${tmpdir}/payload"
mkdir -p "${payload}/.claude/skills/flink" "${payload}/.claude/commands"
cat >"${payload}/flink-cli" <<'EOF'
#!/usr/bin/env bash
if [ "${1:-}" = "version" ]; then
  echo "v0.0.0-test"
else
  echo "flink-cli test binary"
fi
EOF
chmod +x "${payload}/flink-cli"
printf -- '---\nname: flink\n---\n' >"${payload}/.claude/skills/flink/SKILL.md"
printf '# /flink\n' >"${payload}/.claude/commands/flink.md"

(cd "$payload" && LC_ALL=C tar -czf "${tmpdir}/${archive}" .)
checksum=$(shasum -a 256 "${tmpdir}/${archive}" | awk '{print $1}')
printf '%s  %s\n' "$checksum" "$archive" >"${tmpdir}/checksums.txt"

stub_bin="${tmpdir}/bin"
mkdir -p "$stub_bin"
cat >"${stub_bin}/curl" <<EOF
#!/usr/bin/env bash
set -euo pipefail
out=""
while [ "\$#" -gt 0 ]; do
  case "\$1" in
    -o)
      out="\$2"
      shift 2
      ;;
    -*)
      shift
      ;;
    *)
      url="\$1"
      shift
      ;;
  esac
done
case "\${url:-}" in
  *"${archive}")
    cp "${tmpdir}/${archive}" "\$out"
    ;;
  *checksums.txt)
    cp "${tmpdir}/checksums.txt" "\$out"
    ;;
  *)
    printf 'unexpected curl url: %s\n' "\${url:-}" >&2
    exit 1
    ;;
esac
EOF
chmod +x "${stub_bin}/curl"

prefix="${tmpdir}/prefix"
skill_dir="${tmpdir}/claude-skill"
agents_skill_dir="${tmpdir}/agents-skill"
codex_skill_dir="${tmpdir}/codex-skill"
command_dir="${tmpdir}/commands"
mkdir -p "$prefix"

stderr="${tmpdir}/install.err"
set +e
PATH="${stub_bin}:$PATH" \
LANG=bad_locale \
LC_ALL=bad_locale \
VERSION="$version" \
PREFIX="$prefix" \
SKILL_DIR="$skill_dir" \
AGENTS_SKILL_DIR="$agents_skill_dir" \
CODEX_SKILL_DIR="$codex_skill_dir" \
COMMAND_DIR="$command_dir" \
"${repo_root}/scripts/install.sh" >/dev/null 2>"$stderr"
status=$?
set -e

if [ "$status" -ne 0 ]; then
  cat "$stderr" >&2
  exit "$status"
fi

if grep -q 'Failed to set default locale' "$stderr"; then
  cat "$stderr" >&2
  exit 1
fi

test -x "${prefix}/flink-cli"
test -f "${skill_dir}/SKILL.md"
test -f "${agents_skill_dir}/SKILL.md"
test -f "${codex_skill_dir}/SKILL.md"
test -f "${command_dir}/flink.md"

default_home="${tmpdir}/default-home"
mkdir -p "$default_home"
stderr_default="${tmpdir}/install-default.err"
set +e
PATH="${stub_bin}:$PATH" \
HOME="$default_home" \
VERSION="$version" \
NO_SKILL=1 \
NO_CODEX_SKILL=1 \
NO_COMMAND=1 \
env -u PREFIX "${repo_root}/scripts/install.sh" >/dev/null 2>"$stderr_default"
status=$?
set -e

if [ "$status" -ne 0 ]; then
  cat "$stderr_default" >&2
  exit "$status"
fi

test -x "${default_home}/.local/bin/flink-cli"
