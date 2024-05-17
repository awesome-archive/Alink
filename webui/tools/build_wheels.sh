set -ex

base_dir=$(cd "$(dirname "$0")"; pwd -P)
open_project_root="$base_dir/.."
rm -rf "$base_dir"/wheels
mkdir -p "$base_dir"/wheels

for version in {9..13}; do
    echo 1."$version"
    cd "$open_project_root"/alink_patches_flink-1."${version}"/alink || exit
    mkdir -p python/target/package/pyalink/lib/
    rm -rf python/target/package/build
    rm -rf python/target/package/dist
    rm -rf python/target/package/pyalink.egg-info
    cp core/target/alink_core_*.jar python/target/package/pyalink/lib/
    cp python/target/alink_python_*.jar python/target/package/pyalink/lib/
    cd python/target/package
    python3 setup.py bdist_wheel
    cp dist/*.whl  "$base_dir"/wheels/
done
