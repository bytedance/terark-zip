//
// Created by leipeng on 2019-05-07.
//

#include "fsa_ext.hpp"

namespace terark {

    TERARK_DLL_EXPORT
    size_t dfa_write_text(const BaseDFA* dfa, FILE* fp) {
        auto adfa = dynamic_cast<const AcyclicPathDFA*>(dfa);
        if (!adfa) {
            THROW_STD(invalid_argument, "dfa is not an AcyclicPathDFA");
        }
        ADFA_LexIteratorUP iter(adfa->adfa_make_iter());
        bool hasNext = iter->seek_begin();
        size_t nth = 0;
        while (hasNext) {
            fstring word = iter->word();
            fprintf(fp, "%.*s\n", word.ilen(), word.data());
            hasNext = iter->incr();
            nth++;
        }
        return nth;
    }

}
