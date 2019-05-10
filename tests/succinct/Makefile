
TERARK_EXT_LIBS := fsa

SRCS += $(wildcard *.cpp)
#SRCS += $(wildcard extra/*.cpp)

include ../../tools/fsa/Makefile.common

.PHONY : test
test : ${BINS_D}
	env ${DLL_PATH_VAR}=${LIB_DIR} ${DBG_DIR}/rank_select_test.exe 10000
	env ${DLL_PATH_VAR}=${LIB_DIR} ${DBG_DIR}/sorted_uint_vec_test.exe 10000
	env ${DLL_PATH_VAR}=${LIB_DIR} ${DBG_DIR}/rank_select_unit_test.exe 10000
