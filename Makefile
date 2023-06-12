.PHONY: all clean format debug release duckdb_debug duckdb_release update
all: release
GEN=ninja

OSX_BUILD_UNIVERSAL_FLAG=
ifeq (${OSX_BUILD_UNIVERSAL}, 1)
	OSX_BUILD_UNIVERSAL_FLAG=-DOSX_BUILD_UNIVERSAL=1
endif

include .env

# BUILD_OUT_OF_TREE_EXTENSION
POSTGRES_SCANNER_PATH=${PWD}

clean:
	rm -rf build
	rm -rf postgres

pull:
	git submodule init
	git submodule update --recursive --remote
	cd duckdb && git checkout d58ab188ff171387ff7052bc54e78d5a17dacfeb


debug:
	mkdir -p build/debug && \
	cd build/debug && \
	cmake -DCMAKE_BUILD_TYPE=Debug ${OSX_BUILD_UNIVERSAL_FLAG} -DBUILD_TPCH_EXTENSION=1 -DBUILD_TPCDS_EXTENSION=1 -DEXTENSION_STATIC_BUILD=1 ../../duckdb/CMakeLists.txt -DEXTERNAL_EXTENSION_DIRECTORIES=${POSTGRES_SCANNER_PATH} -B. -S ../../duckdb && \
	cmake --build .


# release: pull
release:
	mkdir -p build/release && \
	cd build/release && \
	cmake -DCMAKE_BUILD_TYPE=Release ${OSX_BUILD_UNIVERSAL_FLAG} -DBUILD_TPCH_EXTENSION=1 -DBUILD_TPCDS_EXTENSION=1 -DEXTENSION_STATIC_BUILD=1 ../../duckdb/CMakeLists.txt -DEXTERNAL_EXTENSION_DIRECTORIES=${POSTGRES_SCANNER_PATH} -B. -S ../../duckdb && \
	cmake --build . --parallel


test: release
	./build/release/test/unittest --test-dir . "[postgres_scanner]"

bench: release
	export DYLD_LIBRARY_PATH=${DYLD_LIBRARY_PATH} &&\
	export LD_LIBRARY_PATH=${LD_LIBRARY_PATH} &&\
	$(CXX) -g -std=c++11 bench.cc -o bench -lduckdb -Lbuild/release/src -Iduckdb/src/include && \
	./bench ${TPCH_DB} ${S3_ACCESS_KEY} ${S3_SECRETE_KEY} ${PG_CONN} ${PG_EXTENSION_PATH_RELEASE} $(QPATH) ${LOCAL_FILE_PATH}

bench_tpch: release 
	export DYLD_LIBRARY_PATH=${DYLD_LIBRARY_PATH} &&\
	export LD_LIBRARY_PATH=${LD_LIBRARY_PATH} &&\
	$(CXX) -g -std=c++11 bench_tpch.cc -o bench_tpch -lduckdb -Lbuild/release/src -Iduckdb/src/include && \
	./bench_tpch ${TPCH_DB} ${S3_ACCESS_KEY} ${S3_SECRETE_KEY} ${PG_CONN} ${PG_EXTENSION_PATH_RELEASE} $(QPATH) ${LOCAL_FILE_PATH} $(NRUNS)

format: pull
	cp duckdb/.clang-format .
	clang-format --sort-includes=0 -style=file -i postgres_scanner.cpp
	clang-format --sort-includes=0 -style=file -i concurrency_test.cpp
	cmake-format -i CMakeLists.txt
