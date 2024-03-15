set -ex

find . -name '*.sh' -print0 | xargs -0 -n1 shellcheck -s bash -e SC2001

cargo clippy --tests -- -Dwarnings
