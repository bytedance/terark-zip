# 1. Introduction
- TerarkZip is a [TerarkDB](https://github.com/bytedance/terarkdb) submodule that provides TerarkZipTable dependencies.
- Users can also use TerarkZip as a compression and indexing algorithm library
- TerarkZip also provides a set of useful utilities including `rank-select`, `bitmap` etc.

# 2. Features
- Indexing
  - Nested Lous Trie
- Compression
  - PA-Zip Compression
  - Entropy Compression

# 3. Usage
## Method 1: CMake
- In your CMakeLists.txt
  - ADD_SUBDIRECTORY(terark-zip)
  - use `terark-zip` target anywhere you want

## Method 2: Static Library
- ./build.sh
- cd output
  - move `include` and `lib` directories to your project


## 4. License
- BSD 3-Clause License
