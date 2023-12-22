readCargoVariable() {
  declare variable="$1"
  declare Cargo_toml="$2"

  while read -r name equals value _; do
    if [[ $name = "$variable" && $equals = = ]]; then
      echo "${value//\"/}"
      return
    fi
  done < <(cat "$Cargo_toml")
  echo "Unable to locate $variable in $Cargo_toml" 1>&2
}

if [[ -n $RUST_STABLE_VERSION ]]; then
  stable_version="$RUST_STABLE_VERSION"
else
  # read rust version from rust-toolchain.toml file
  base="$(dirname "${BASH_SOURCE[0]}")"
  stable_version=$(readCargoVariable channel "$base/../rust-toolchain.toml")
fi

if [[ -n $RUST_NIGHTLY_VERSION ]]; then
  nightly_version="$RUST_NIGHTLY_VERSION"
else
  nightly_version=2023-11-16
fi

echo "stable_version: $stable_version"
echo "nightly_version: $nightly_version"

export rust_stable="$stable_version"
export rust_nightly=nightly-"$nightly_version"

[[ -z $1 ]] || (

  rustup_install() {
    declare toolchain=$1
    if ! cargo +"$toolchain" -V > /dev/null; then
      echo "$0: Missing toolchain? Installing...: $toolchain" >&2
      rustup install "$toolchain"
      cargo +"$toolchain" -V
    fi
  }

  set -e
  cd "$(dirname "${BASH_SOURCE[0]}")"
  case $1 in
  stable)
     rustup_install "$rust_stable"
     ;;
  nightly)
     rustup_install "$rust_nightly"
    ;;
  all)
     rustup_install "$rust_stable"
     rustup_install "$rust_nightly"
    ;;
  *)
    echo "$0: Note: ignoring unknown argument: $1" >&2
    ;;
  esac
)
