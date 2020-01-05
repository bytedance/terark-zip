#include <terark/util/process.hpp>
#include <terark/util/linebuf.hpp>

int main(int argc, char* argv[]) {
	using namespace terark;
    {
        printf("1 begin...\n");
        LineBuf line;
        ProcPipeStream pp("echo aaaa", "r");
        printf("reading result\n");
        //line.getline(pp);

        line.read_all(pp);
        line.chomp();
        assert(line.size() == 4);
        printf("read result = len=%zd : %s\n", line.n, line.p);
        assert(fstring(line) == "aaaa");
        printf("1 passed\n");
    }

    {
        printf("2 begin...\n");
        ProcPipeStream pp("cat > proc.test.tmp", "w");
        fprintf(pp, "%s\n", "bbbb");
        pp.close();

        LineBuf line;
        line.read_all("proc.test.tmp");
        line.chomp();
        assert(fstring(line) == "bbbb");
        printf("2 passed\n");
        ::remove("proc.test.tmp");
    }

    printf("3 begin...\n");
    try {
        ProcPipeStream pp("test-non-existed-file", "r");
        pp.close();
        assert(pp.err_code() != 0); // will not goes here
    }
    catch (const std::exception&) {
    }
    printf("3 passed\n");

    {
        printf("4 begin...\n");
        auto res = vfork_cmd("(echo aa; cat)", "bb");
    //  printf("res.size() = %zd: %s\n", res.size(), res.c_str());
        assert(res == "aa\nbb");
        printf("4 passed\n");
    }

	return 0;
}
