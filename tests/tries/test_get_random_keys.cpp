//
// Created by leipeng on 2019-05-16.
//

#include <terark/fsa/cspptrie.inl>
#include <terark/fsa/nest_trie_dawg.hpp>
#include <terark/util/autoclose.hpp>
#include <terark/util/linebuf.hpp>

using namespace terark;

void test_nlt(SortableStrVec strVec) {
    printf("test_nlt...\n");
    NestLoudsTrieConfig conf;
    NestLoudsTrieDAWG_IL_256 nlt;
    nlt.build_from(strVec, conf);
    SortableStrVec rkeys;
    nlt.get_random_keys(&rkeys, nlt.num_words());
    for (size_t i = 0; i < rkeys.size(); ++i) {
        fstring key = rkeys[i];
        size_t x = nlt.index(key);
        assert(x < nlt.num_words());
        if (x >= nlt.num_words())
            abort();
    }
    printf("test_nlt passed!\n\n");
}

void test_patricia(SortableStrVec strVec) {
    printf("test_patricia...\n");
    MainPatricia trie(sizeof(size_t));
    {
        Patricia::WriterTokenPtr token(new Patricia::WriterToken(&trie));
        for (size_t i = 0; i < strVec.size(); ++i) {
            fstring key = strVec[i];
            size_t seq_id = strVec.m_index[i].seq_id;
            trie.insert(key, &seq_id, &*token);
        }
        token->release();
    }
    strVec.sort();
    SortableStrVec rkeys;
    trie.dfa_get_random_keys(&rkeys, trie.num_words());

    Patricia::ReaderToken& token = *trie.acquire_tls_reader_token();
    for (size_t i = 0; i < rkeys.size(); ++i) {
        fstring key = rkeys[i];
        bool ret = trie.lookup(key, &token);
        assert(ret);
        size_t lo = strVec.lower_bound(key);
        size_t hi = strVec.upper_bound(key);
        assert(lo < hi);
        assert(hi <= strVec.size());
        assert(strVec[lo] == key);
        bool hasValue = false;
        for (size_t j = lo; j < hi; ++j) {
            if (token.value_of<size_t>() == strVec.m_index[j].seq_id) {
                hasValue = true;
            }
        }
        //printf("lo = %zd, hi = %zd, hi-lo = %zd\n", lo, hi, hi-lo);
        assert(hasValue);
        if (!ret || !hasValue)
            abort();
    }
    token.release();
    printf("test_patricia passed!\n\n");
}

int main(int argc, char* argv[]) {
    const char* fname = "fab-data.txt";
    if (argc >= 2) {
        fname = argv[1];
    }
    Auto_fclose fp(fopen(fname, "r"));
    if (!fp) {
        fprintf(stderr, "fopen(%s) = %s\n", fname, strerror(errno));
        return 1;
    }
    SortableStrVec strVec;
    LineBuf line;
    while (line.getline(fp) > 0) {
        line.chomp();
        strVec.push_back(line);
    }
    test_nlt(strVec);
    test_patricia(strVec);
    return 0;
}
