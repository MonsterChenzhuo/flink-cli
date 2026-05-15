#!/usr/bin/env bash
# flink-cli installer / upgrader.
# Re-run the same command to upgrade to the latest release.
#
# Env overrides:
#   VERSION=v0.1.2                 pin a specific release (default: latest)
#   PREFIX=/usr/local/bin          install directory for the binary
#   SKILL_DIR=~/.claude/skills/flink install directory for the bundled Claude Code skill
#   AGENTS_SKILL_DIR=~/.agents/skills/flink install directory for the bundled Codex skill
#   CODEX_SKILL_DIR=~/.codex/skills/flink install directory for an extra Codex-local skill copy
#   COMMAND_DIR=~/.claude/commands   install directory for the bundled /flink slash command
#   NO_SUDO=1                      never use sudo; fail if PREFIX is not writable
#   NO_SKILL=1                     skip installing the bundled skill
#   NO_CODEX_SKILL=1               skip installing Codex skill copies
#   NO_COMMAND=1                   skip installing the bundled /flink slash command
#   REPO=MonsterChenzhuo/flink-cli override repo slug

set -euo pipefail

REPO="${REPO:-MonsterChenzhuo/flink-cli}"
PREFIX="${PREFIX:-/usr/local/bin}"
SKILL_DIR="${SKILL_DIR:-$HOME/.claude/skills/flink}"
AGENTS_SKILL_DIR="${AGENTS_SKILL_DIR:-$HOME/.agents/skills/flink}"
CODEX_SKILL_DIR="${CODEX_SKILL_DIR:-$HOME/.codex/skills/flink}"
COMMAND_DIR="${COMMAND_DIR:-$HOME/.claude/commands}"
VERSION="${VERSION:-}"

info() { printf '\033[1;34m==>\033[0m %s\n' "$*" >&2; }
warn() { printf '\033[1;33m!!\033[0m %s\n' "$*" >&2; }
die()  { printf '\033[1;31mxx\033[0m %s\n' "$*" >&2; exit 1; }

need() { command -v "$1" >/dev/null 2>&1 || die "missing required tool: $1"; }
tar_cmd() { LC_ALL=C LANG=C tar "$@"; }
need curl
need tar
need uname

os=$(uname -s | tr '[:upper:]' '[:lower:]')
case "$os" in
  linux|darwin) ;;
  *) die "unsupported OS: $os (only linux/darwin)" ;;
esac

arch=$(uname -m)
case "$arch" in
  x86_64|amd64) arch=amd64 ;;
  aarch64|arm64) arch=arm64 ;;
  *) die "unsupported arch: $arch (only amd64/arm64)" ;;
esac

if [ -z "$VERSION" ]; then
  info "resolving latest release from github.com/$REPO"
  redirect=$(curl -fsSLI -o /dev/null -w '%{url_effective}' \
    "https://github.com/${REPO}/releases/latest" 2>/dev/null || true)
  VERSION="${redirect##*/}"
  if [ -z "$VERSION" ] || [ "$VERSION" = "latest" ]; then
    api_resp=$(curl -fsSL "https://api.github.com/repos/${REPO}/releases/latest" 2>/dev/null || true)
    VERSION=$(awk -F'"' '/"tag_name":/ { print $4; exit }' <<<"$api_resp")
  fi
  [ -n "$VERSION" ] || die "could not determine latest release tag; pin with VERSION=vX.Y.Z"
fi
ver_no_v="${VERSION#v}"

tmpdir=$(mktemp -d)
trap 'rm -rf "$tmpdir"' EXIT
archive="flink-cli_${ver_no_v}_${os}_${arch}.tar.gz"
checksums="checksums.txt"
base="https://github.com/${REPO}/releases/download/${VERSION}"

info "downloading ${archive}"
curl -fsSL "${base}/${archive}" -o "${tmpdir}/${archive}"
curl -fsSL "${base}/${checksums}" -o "${tmpdir}/${checksums}" || warn "checksums file not found, skipping verification"

if [ -s "${tmpdir}/${checksums}" ]; then
  info "verifying checksum"
  expected=$(awk -v f="$archive" '$2==f {print $1}' "${tmpdir}/${checksums}")
  [ -n "$expected" ] || die "no checksum entry for ${archive}"
  if command -v sha256sum >/dev/null 2>&1; then
    actual=$(sha256sum "${tmpdir}/${archive}" | awk '{print $1}')
  else
    actual=$(shasum -a 256 "${tmpdir}/${archive}" | awk '{print $1}')
  fi
  [ "$expected" = "$actual" ] || die "checksum mismatch (expected $expected, got $actual)"
fi

info "extracting"
tar_cmd -xzf "${tmpdir}/${archive}" -C "${tmpdir}"
[ -x "${tmpdir}/flink-cli" ] || die "binary not found in archive"

sudo_cmd=""
if [ ! -w "$PREFIX" ] && [ "$(id -u)" -ne 0 ]; then
  if [ "${NO_SUDO:-0}" = "1" ]; then
    die "PREFIX=$PREFIX not writable and NO_SUDO=1"
  fi
  need sudo
  sudo_cmd="sudo"
fi

info "installing binary to ${PREFIX}/flink-cli"
$sudo_cmd install -d "$PREFIX"
$sudo_cmd install -m 0755 "${tmpdir}/flink-cli" "${PREFIX}/flink-cli"

skill_src="${tmpdir}/.claude/skills/flink"
if [ "${NO_SKILL:-0}" != "1" ] && [ -d "$skill_src" ]; then
  info "installing Claude Code skill to ${SKILL_DIR}"
  mkdir -p "$SKILL_DIR"
  (cd "$skill_src" && tar_cmd -cf - .) | (cd "$SKILL_DIR" && tar_cmd -xf -)
fi

if [ "${NO_CODEX_SKILL:-0}" != "1" ] && [ -d "$skill_src" ]; then
  info "installing Codex skill to ${AGENTS_SKILL_DIR}"
  mkdir -p "$AGENTS_SKILL_DIR"
  (cd "$skill_src" && tar_cmd -cf - .) | (cd "$AGENTS_SKILL_DIR" && tar_cmd -xf -)

  info "installing Codex-local skill copy to ${CODEX_SKILL_DIR}"
  mkdir -p "$CODEX_SKILL_DIR"
  (cd "$skill_src" && tar_cmd -cf - .) | (cd "$CODEX_SKILL_DIR" && tar_cmd -xf -)
fi

command_src="${tmpdir}/.claude/commands/flink.md"
if [ "${NO_COMMAND:-0}" != "1" ] && [ -f "$command_src" ]; then
  info "installing Claude Code slash command to ${COMMAND_DIR}/flink.md"
  mkdir -p "$COMMAND_DIR"
  install -m 0644 "$command_src" "${COMMAND_DIR}/flink.md"
fi

installed_version=$("${PREFIX}/flink-cli" version 2>/dev/null || echo "$VERSION")
info "done: ${installed_version}"
info "run: flink-cli --help"
