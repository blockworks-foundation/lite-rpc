#!/bin/bash

# Function to display help message
show_help() {
    echo "Usage: $0 [stable|nightly] [command]"
    echo
    echo "This script is used to run a specified cargo command with either the stable or nightly Rust toolchain."
    echo
    echo "Arguments:"
    echo "  stable    - Use the stable Rust toolchain."
    echo "  nightly   - Use the nightly Rust toolchain."
    echo "  command   - The cargo command to run (e.g., clippy, build)."
    echo
    echo "Examples:"
    echo "  $0 stable clippy    - Run clippy with the stable Rust version."
    echo "  $0 nightly build    - Build with the nightly Rust version."
}

# Function to run a specified cargo command with a specified toolchain
run_cargo_command() {
    local toolchain=$1
    local command=$2
    echo "Running cargo $command with Rust $toolchain version..."
    cargo +"$toolchain" "$command"
}

# Check if the first argument is -h or --help
if [[ $1 == "-h" || $1 == "--help" ]]; then
    show_help
    exit 0
fi

# Check if the second argument is not provided
if [[ -z $2 ]]; then
    echo "Error: No command specified."
    show_help
    exit 1
fi

# Path to the rust-version.sh script
rust_version_script="./ci/rust-version.sh"

# Check if rust-version.sh exists and is executable
if [[ -f "$rust_version_script" && -x "$rust_version_script" ]]; then
    # Source the rust-version.sh script
    source "$rust_version_script"
else
    echo "Error: rust-version.sh not found or not executable."
    exit 1
fi


# Main execution logic based on passed arguments
case $1 in
    stable)
        # Check if Rust stable version is set
        if [[ -z $rust_stable ]]; then
            echo "Error: Rust stable version is not set."
            exit 1
        fi
        run_cargo_command "$rust_stable" "$2"
        ;;
    nightly)
        # Check if Rust nightly version is set
        if [[ -z $rust_nightly ]]; then
            echo "Error: Rust nightly version is not set."
            exit 1
        fi
        run_cargo_command "$rust_nightly" "$2"
        ;;
    *)
        echo "Usage: $0 [stable|nightly] [command]"
        exit 1
        ;;
esac
