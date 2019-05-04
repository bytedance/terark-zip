#include <stdio.h>
#include <terark/io/FileStream.hpp>
#include <terark/io/StreamBuffer.hpp>
#include <terark/io/DataInput.hpp>

int main(int argc, char* argv[]) {
	using namespace terark;
	NonOwnerFileStream fstdin(stdin);
	NativeDataInput<InputBuffer> dio(&fstdin);
	valvec<byte_t> buf;
	try {
		while (true) {
			dio >> buf;
			printf("%.*s\n", int(buf.size()), buf.data());
		}
	}
	catch (const EndOfFileException&) {
	}
	return 0;
}

