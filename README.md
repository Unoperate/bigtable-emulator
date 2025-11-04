# Single node, Memory Only Emulator for Google Bigtable

This is a single-node, non-persistent emulator for Google's Bigtable.

It should pass all the integration tests in Google's C++ client
repository (google-cloud-cpp), except those that must run against
production Bigtable.

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

## Running clang-tidy with Bazel

```shell
bazel build --config clang-tidy //...
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
