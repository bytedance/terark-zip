
TERARK_EXT_LIBS := fsa

include ../../tools/fsa/Makefile.common

.PHONY : test
test : ${BINS_D}
	env ${DLL_PATH_VAR}=${LIB_DIR} ${DBG_DIR}/test_adfa_iter.exe -n    < fab-data.txt
	env ${DLL_PATH_VAR}=${LIB_DIR} ${DBG_DIR}/nest_louds_trie_test.exe < fab-data.txt
	env ${DLL_PATH_VAR}=${LIB_DIR} ${DBG_DIR}/test_dict_order_gen.exe  < fab-data.txt
