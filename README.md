# Single Node, Memory Only Emulator for Google Bigtable

This is a single-node, non-persistent emulator for Google's Bigtable.

It should pass all the integration tests in Google's C++ client
repository (google-cloud-cpp), except those that must run against
production Bigtable.

## Dependencies

The Bigtable-emulator depends on `google-cloud-cpp` (which the build
tools retrieve and build automatically) and the `abseil`
library. Other dependencies such as `GRPC` are provided by
`google-cloud-cpp`.

## Building

The Bigtable emulator can be built with `bazel` or `cmake`.

The `cmake` build can be used to produce a `compile_commands.json`
which can be used with most IDEs and modern language servers
(e.g. `clang`) to provide code completion and much more.

### Bazel

```shell
cd bigtable-emulator
bazel build ...
```
#### Running the Unit Tests With Bazel

```shell
bazel test ...
```

### Cmake

The following cmake command will produce a development build that can
be debugged and that produces a `compile_commands.json` in the
`build/` directory that you can symlink to the root directory to get
completion and many other modern features from `clangd` and other
similar tools.

```shell
CXX=g++ CC=gcc CFLAGS="-g3 -O0" CXXFLAGS="-std=c++14 -pedantic -pedantic-errors -g3 -O0" CMAKE_CXX_FLAGS_DEBUG="-std=c++14 -g3 -O0 -ggdb" cmake -DCMAKE_EXPORT_COMPILE_COMMANDS=1 -DCMAKE_BUILD_TYPE=Debug -H. -DGOOGLE_CLOUD_CPP_ENABLE=bigtable -Bbuild

make -j
```

#### Running the Unit Tests With Cmake

```shell
make test
```

## Running the Emulator

```shell
bigtable-emulator -p <port>
```

## Contributing changes

See [`CONTRIBUTING.md`](/CONTRIBUTING.md) for details on how to contribute to
this project, including how to build and test your changes as well as how to
properly format your code.

## Licensing

Apache 2.0; see [`LICENSE`](/LICENSE) for details.
