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

Building the Bigtable emulator requires `bazel`.

```shell
cd bigtable-emulator
bazel build ...
```
## Running the Unit Tests

```shell
bazel test ...
```

### Running the Emulator

```shell
bigtable-emulator -p <port>
```

## Contributing changes

See [`CONTRIBUTING.md`](/CONTRIBUTING.md) for details on how to contribute to
this project, including how to build and test your changes as well as how to
properly format your code.

## Licensing

Apache 2.0; see [`LICENSE`](/LICENSE) for details.
