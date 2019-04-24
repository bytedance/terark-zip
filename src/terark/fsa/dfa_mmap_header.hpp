#ifndef __terark_fsa_dfa_mmap_header_hpp__
#define __terark_fsa_dfa_mmap_header_hpp__

#include "fsa.hpp"

namespace terark {

struct DFA_BlockDataEntry {
	uint64_t offset;
	uint64_t length;
	uint64_t endpos() const { return offset + length; }
};

struct DFA_MmapHeaderBase {
	enum { MAX_BLOCK_NUM = 12 };
	enum { current_version = 1 };
	uint8_t   magic_len; // 13
	char      magic[19]; // nark-dfa-mmap
	char      dfa_class_name[60];

	byte_t    is_dag;
	byte_t    num_blocks;
	uint16_t  kv_delim;
	uint32_t  header_size; // == sizeof(DFA_MmapHeader)
	uint32_t  version;
	uint32_t  state_size;

	uint64_t  file_size;
	uint64_t  total_states;
	uint64_t  zpath_states;
	uint64_t  numFreeStates;
	uint64_t  firstFreeState;
	uint64_t  transition_num;
	uint64_t  dawg_num_words;

	byte_t    ac_word_ext; // 0: no length; 1: length; 2: length+content
	byte_t    is_nlt_dawg_strpool; // for NestLoudsTrieBlobStore
	byte_t    louds_dfa_cross_dst_uintbits;
	byte_t    crc32cLevel; // 0: no crc; 1: header; 2: file
	uint32_t  file_crc32;
	uint64_t  zpath_length;
	byte_t    louds_dfa_cache_ptrbit;
	byte_t    padding2;
	uint16_t  louds_dfa_num_zpath_trie;
	uint32_t  louds_dfa_cache_states;
	uint64_t  gnode_states;
	uint32_t  atom_dfa_num;
	uint32_t  dfa_cluster_num;
	uint32_t  louds_dfa_min_zpath_id;
	uint32_t  louds_dfa_min_cross_dst;
	uint64_t  adfa_total_words_len; // for Acyclic DFA, U64_MAX for Cyclic DFA
	uint64_t  reserve1[10];

	DFA_BlockDataEntry blocks[MAX_BLOCK_NUM];
};
struct DFA_MmapHeader : DFA_MmapHeaderBase {
	char reserved[1020-sizeof(DFA_MmapHeaderBase)];
	uint32_t header_crc32;
};
BOOST_STATIC_ASSERT(sizeof(DFA_MmapHeader) == 1024);

typedef DFA_MmapHeader DFA_Stat;

} // namespace terark

#endif // __terark_fsa_dfa_mmap_header_hpp__
