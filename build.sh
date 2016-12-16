git submodule update --init --recursive
git submodule update --init --recursive
cmake -DCMAKE_BUILD_TYPE=Release -DYAML_CPP_BUILD_TOOLS=OFF -DYAML_CPP_BUILD_CONTRIB=OFF -DTARANTOOL_C_EMBEDDED=1 .
make
strip -s replicatord
