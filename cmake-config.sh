BLDDIR=/localdisk/work/amalakho/arrow-build2
rm -rf $BLDDIR
#    -DCMAKE_CXX_FLAGS_RELWITHDEBINFO:STRING=-O2 -g -DNDEBUG -march=native
#    -DCMAKE_CXX_FLAGS_RELWITHDEBINFO:STRING=-O2 -g -DNDEBUG -march=native
env \
    CXXFLAGS="-march=native -fvisibility-inlines-hidden -std=c++17 -fmessage-length=0 -ftree-vectorize -fPIC -fno-plt -O2 -ffunction-sections -pipe" \
    CFLAGS="-march=native -ftree-vectorize -fPIC -fno-plt -O2 -ffunction-sections -pipe" \
    LDFLAGS_USED="-Wl,-O2 -Wl,--sort-common -Wl,--as-needed -Wl,-z,relro -Wl,-z,now -Wl,--disable-new-dtags -Wl,--gc-sections -Wl,-rpath,${CONDA_PREFIX}/lib -Wl,-rpath-link,${CONDA_PREFIX}/lib -L${CONDA_PREFIX}/lib" \
  cmake -S cpp/ -B $BLDDIR/ -GNinja \
    -DCMAKE_BUILD_TYPE=RelWithDebInfo \
    -DARROW_DEPENDENCY_SOURCE:STRING=CONDA \
    -DCMAKE_INSTALL_PREFIX:PATH=$CONDA_PREFIX \
    -DCMAKE_INSTALL_RPATH:STRING='$ORIGIN/.' \
    -DARROW_GGDB_DEBUG:BOOL=ON \
    -DARROW_JEMALLOC:BOOL=ON \
    -DARROW_BUILD_BENCHMARKS:BOOL=ON \
    -DARROW_BUILD_EXAMPLES:BOOL=ON \
    -DCXX_SUPPORTS_SSE4_2:BOOL=OFF \
    -DARROW_USE_SIMD=ON \
    && cmake --build $BLDDIR --target install

