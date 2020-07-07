#include <terark/fsa/fsa.hpp>
#include <terark/util/fstrvec.hpp>

int main(int argc, char* argv[]) {
    using namespace terark;
    std::unique_ptr<MatchingDFA> dfa(MatchingDFA::load_mmap(0));
    fstrvecl fsv;
    ADFA_LexIteratorUP iter(dfa->adfa_make_iter());
    if (iter->seek_begin()) {
        do {
            fstring word = iter->word();
            fsv.push_back(word);
        } while (iter->incr());
    }
    for (size_t i = 0; i < fsv.size(); ++i) {
        fstring word = fsv[i];
        assert(iter->seek_lower_bound(word));
    }
    fprintf(stderr, "key num = %zd, key len sum = %zd\n", fsv.size(), fsv.strpool.size());
    return 0;
}
