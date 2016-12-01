git submodule update --init --recursive
git submodule update --init --recursive
cmake -DCMAKE_BUILD_TYPE=Release .
make
strip -s replicatord
