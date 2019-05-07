//
// Created by leipeng on 2019-05-07.
//

#include <terark/fsa/fsa_ext.hpp>

int main(int argc, char* argv[]) {
    using namespace terark;
    if (argc < 2) {
        fprintf(stderr, "usage: %s dfa-file\n", argv[0]);
        return 1;
    }
    try {
        std::unique_ptr<BaseDFA> dfa(BaseDFA::load_from(argv[1]));
        dfa_write_text(dfa.get(), stdout);
        return 0;
    }
    catch (const std::exception&) {
        return 2;
    }
}
