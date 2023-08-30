CURRENT_DIR=`pwd`

export CGO_CFLAGS="-I${CURRENT_DIR}/.deps/include"
export CGO_CXXFLAGS="-I${CURRENT_DIR}/.deps/include"
export CGO_LDFLAGS="-L${CURRENT_DIR}/.deps/lib -lrocksdb -lz -lbz2 -lsnappy -llz4 -lzstd"