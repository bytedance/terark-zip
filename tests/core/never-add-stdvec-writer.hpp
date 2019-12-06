#include <terark/io/StdvecWriter.hpp>
#include <terark/io/MemStream.hpp>
#include <terark/io/DataIO.hpp>

int main() {
    using namespace terark;
    NativeDataOutput<StdvecWriter<std::string> > writer;
    writer << 1;
    writer << std::string("abc");

    NativeDataInput<MemIO> reader;
    int i;
    std::string s;
    reader >> i;
    reader >> s;
    assert(1 == i);
    assert("abc" == s);

    printf("%s done\n", argv[0]);
    return 0;
}
