# Introduction

Terark-Core is the foundation of TerarkDB and TerarkKV, it contains a series of algorithms that support storage engine development:

- Index algorithms
  - cspp trie
  - composite uint index
  - scgt
  - ...
- Value store algorithms
  - DictZipBlobStore
  - EntropyZipBlobStore
  - MixedLenBlobStore
  - ...

## Complile
1. Install libaio-dev first if you dont have it yet
2. `build.sh` will do the rest work
3. Note that the `CMakeLists.txt` here is just for IDE loading, please inspect Makefile for the actual complie details


## Unit Tests
Terark-Core contains a set of serious test cases under `./tests` and `./gtests`, you can run them all by using `test.sh` or `gtest.sh` to make sure your changes do not break previous code logic.
- test.sh
  - will be removed in the future
- gtest.sh
  - google test suite, will replace test.sh

## Integration
If you want to use Terark-Core in you own application, simple include the headers and link terark-core's three libraries.

Terark-Core's API detail:

TODO
