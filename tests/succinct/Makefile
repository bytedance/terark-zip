
TERARK_EXT_LIBS := fsa

SRCS += $(wildcard *.cpp)
#SRCS += $(wildcard extra/*.cpp)

include ../../tools/fsa/Makefile.common

UNIT_TESTS := $(addsuffix .test,${BINS_D})

.PHONY : test
test : ${UNIT_TESTS}

${UNIT_TESTS} : %.test : %
	env ${DLL_PATH_VAR}=${LIB_DIR} $<
