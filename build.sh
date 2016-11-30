git submodule update --init --recursive
git submodule update --init --recursive
cmake -DCMAKE_BUILD_TYPE=Release .
chmod 755 lib/libconfig/configure
make
strip -s replicatord
