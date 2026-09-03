#!/usr/bin/env bash

set -Eeuo pipefail

readonly SCRIPT_NAME="$(basename "$0")"
readonly DEFAULT_MUDUO_REPOSITORY="https://github.com/chenshuo/muduo.git"
readonly DEFAULT_MUDUO_SOURCE="/tmp/kvcache-muduo-src"
readonly DEFAULT_MUDUO_BUILD="/tmp/kvcache-muduo-build"
readonly PROJECT_ROOT="$(cd -- "$(dirname -- "${BASH_SOURCE[0]}")/.." && pwd)"
readonly DEPENDENCY_FILE="$PROJECT_ROOT/environment-dependencies.txt"

skip_muduo=0
muduo_source="${MUDUO_SOURCE_DIR:-$DEFAULT_MUDUO_SOURCE}"
muduo_ref="${MUDUO_REF:-master}"
muduo_repository="${MUDUO_REPOSITORY:-$DEFAULT_MUDUO_REPOSITORY}"

usage() {
  cat <<EOF
Usage: $SCRIPT_NAME [options]

Install Ubuntu/Debian build dependencies and Muduo for KVCache.

Options:
  --skip-muduo          Install distro packages only; do not build Muduo.
  --muduo-source PATH   Use an existing Muduo checkout instead of cloning one.
  --muduo-ref REF       Git ref to checkout when cloning Muduo (default: master).
  --help                Show this help.

Environment overrides:
  MUDUO_SOURCE_DIR, MUDUO_REF, MUDUO_REPOSITORY
  MUDUO_BUILD_DIR

The script installs Muduo into /usr/local, matching CMakeLists.txt.
EOF
}

die() {
  printf 'error: %s\n' "$*"
  exit 1
}

while [[ $# -gt 0 ]]; do
  case "$1" in
    --skip-muduo)
      skip_muduo=1
      shift
      ;;
    --muduo-source)
      [[ $# -ge 2 ]] || die "--muduo-source requires a path"
      muduo_source="$2"
      shift 2
      ;;
    --muduo-ref)
      [[ $# -ge 2 ]] || die "--muduo-ref requires a git ref"
      muduo_ref="$2"
      shift 2
      ;;
    --help|-h)
      usage
      exit 0
      ;;
    *)
      die "unknown option: $1 (use --help for usage)"
      ;;
  esac
done

[[ -f /etc/os-release ]] || die "cannot identify the operating system"
[[ -f "$DEPENDENCY_FILE" ]] || die "dependency file not found: $DEPENDENCY_FILE"
# shellcheck disable=SC1091
source /etc/os-release
case "${ID:-}" in
  ubuntu|debian) ;;
  *) die "this installer supports Ubuntu/Debian; detected ${ID:-unknown}" ;;
esac

if [[ "${EUID}" -eq 0 ]]; then
  apt_cmd=(apt-get)
  install_cmd=(apt-get install -y)
else
  command -v sudo >/dev/null 2>&1 || die "sudo is required when not running as root"
  apt_cmd=(sudo apt-get)
  install_cmd=(sudo apt-get install -y)
fi

printf 'Installing system packages...\n'
mapfile -t apt_packages < <(sed -e 's/[[:space:]]*#.*//' -e '/^[[:space:]]*$/d' "$DEPENDENCY_FILE")
[[ "${#apt_packages[@]}" -gt 0 ]] || die "no packages found in $DEPENDENCY_FILE"
"${apt_cmd[@]}" update
"${install_cmd[@]}" "${apt_packages[@]}"

check_tool() {
  local tool="$1"
  command -v "$tool" >/dev/null 2>&1 || die "required command not found: $tool"
}

check_tool cmake
check_tool g++
check_tool protoc
check_tool python3

printf 'Detected CMake %s, GCC %s, Protobuf %s\n' \
  "$(cmake --version | sed -n '1s/^cmake version //p')" \
  "$(g++ -dumpfullversion -dumpversion)" \
  "$(protoc --version | sed -n 's/^libprotoc //p')"

if [[ "$skip_muduo" -eq 0 ]]; then
  if [[ -d /usr/local/include/muduo ]] &&
     { [[ -f /usr/local/lib/libmuduo_net.so ]] || [[ -f /usr/local/lib/libmuduo_net.a ]]; } &&
     { [[ -f /usr/local/lib/libmuduo_base.so ]] || [[ -f /usr/local/lib/libmuduo_base.a ]]; }; then
    printf 'Muduo is already installed under /usr/local; skipping rebuild.\n'
  else
    check_tool git
    if [[ -d "$muduo_source/.git" ]]; then
      printf 'Using existing Muduo checkout: %s\n' "$muduo_source"
    else
      [[ ! -e "$muduo_source" ]] || die "Muduo source path exists but is not a git checkout: $muduo_source"
      printf 'Cloning Muduo from %s\n' "$muduo_repository"
      git clone --depth 1 --branch "$muduo_ref" "$muduo_repository" "$muduo_source"
    fi

    muduo_build="${MUDUO_BUILD_DIR:-$DEFAULT_MUDUO_BUILD}"
    cmake -S "$muduo_source" -B "$muduo_build" \
      -DCMAKE_BUILD_TYPE=Release \
      -DMUDUO_BUILD_EXAMPLES=OFF \
      -DMUDUO_BUILD_TESTS=OFF \
      -DCMAKE_INSTALL_PREFIX=/usr/local
    cmake --build "$muduo_build" --parallel "$(nproc)"
    if [[ "${EUID}" -eq 0 ]]; then
      cmake --install "$muduo_build"
    else
      sudo cmake --install "$muduo_build"
    fi
  fi
else
  printf 'Skipping Muduo installation (--skip-muduo).\n'
fi

if command -v ldconfig >/dev/null 2>&1; then
  if [[ "${EUID}" -eq 0 ]]; then
    ldconfig
  else
    sudo ldconfig
  fi
fi

if [[ "$skip_muduo" -eq 0 ]]; then
  [[ -d /usr/local/include/muduo ]] ||
    die "Muduo headers were not installed under /usr/local/include/muduo"
  [[ -e /usr/local/lib/libmuduo_net.so || -e /usr/local/lib/libmuduo_net.a ]] ||
    die "libmuduo_net was not installed under /usr/local/lib"
  [[ -e /usr/local/lib/libmuduo_base.so || -e /usr/local/lib/libmuduo_base.a ]] ||
    die "libmuduo_base was not installed under /usr/local/lib"
fi

cat <<EOF

Dependencies are ready.
Next steps:
  cmake -S . -B cmake-build-debug -DCMAKE_BUILD_TYPE=Debug -DBUILD_TESTING=ON
  cmake --build cmake-build-debug --parallel "$(nproc)"
  ctest --test-dir cmake-build-debug --output-on-failure
EOF
