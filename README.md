# Introduction
- TerarkZip is a extension of [TerarkDB](https://github.com/bytedance/terarkdb) that provides TerarkZipTable for users.
- Users can also use TerarkZip as a compression and indexing algorithm library
- TerarkZip also provides a set of useful utilities including `rank-select`, `bitmap` etc.

# Features
- TerarkZipTable
- Indexing
  - Nested Lous Trie
- Compression
  - PA-Zip Compression
  - Entropy Compression

# Usage
## Method 1: CMake
- In your CMakeLists.txt
  - ADD_SUBDIRECTORY(terark-zip)
  - use `terark-zip` target anywhere you want
- If you cannot find include path
  - `GET_TARGET_PROPERTY(terark_zip_include terark-zip INCLUDE_DIRECTORIES)`
  - `INCLUDE_DIRECTORIES(${terark_zip_include})`

## Method 2: Static Library
- ./build.sh
- cd output
  - move `include` and `lib` directories to your project
