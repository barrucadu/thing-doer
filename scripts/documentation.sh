set -e

pushd docs
mdbook-admonish install
popd

sed 's#See \[the documentation\].*##' < README.md > docs/src/README.md

[ -d _site ] && rm -r _site
mdbook build docs
mv docs/book _site

cargo doc --no-deps --document-private-items --workspace
mv target/doc _site/packages

chmod -c -R +rX _site | while read -r line; do
    echo "::warning title=Invalid file permissions automatically fixed::$line"
done
