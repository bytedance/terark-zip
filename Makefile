export SHELL=bash
DBG_FLAGS ?= -g3 -D_DEBUG
RLS_FLAGS ?= -O3 -DNDEBUG -g3
# 'AFR' means Assert For Release
AFR_FLAGS ?= -O2 -g3
WITH_BMI2 ?= $(shell bash ./cpu_has_bmi2.sh)
CMAKE_INSTALL_PREFIX ?= /usr

BOOST_INC ?= -Iboost-include

ifeq "$(origin LD)" "default"
  LD := ${CXX}
endif
ifeq "$(origin CC)" "default"
  CC := ${CXX}
endif

# Makefile is stupid to parsing $(shell echo ')')
tmpfile := $(shell mktemp -u compiler-XXXXXX)
COMPILER := $(shell ${CXX} tools/configure/compiler.cpp -o ${tmpfile}.exe && ./${tmpfile}.exe && rm -f ${tmpfile}*)
UNAME_MachineSystem := $(shell uname -m -s | sed 's:[ /]:-:g')
BUILD_NAME := ${UNAME_MachineSystem}-${COMPILER}-bmi2-${WITH_BMI2}
BUILD_ROOT := build/${BUILD_NAME}
ddir:=${BUILD_ROOT}/dbg
rdir:=${BUILD_ROOT}/rls
adir:=${BUILD_ROOT}/afr

TERARK_ROOT:=${PWD}
COMMON_C_FLAGS  += -Wformat=2 -Wcomment
COMMON_C_FLAGS  += -Wall -Wextra
COMMON_C_FLAGS  += -Wno-unused-parameter

gen_sh := $(dir $(lastword ${MAKEFILE_LIST}))gen_env_conf.sh

err := $(shell env BOOST_INC=${BOOST_INC} bash ${gen_sh} "${CXX}" ${COMPILER} ${BUILD_ROOT}/env.mk; echo $$?)
ifneq "${err}" "0"
   $(error err = ${err} MAKEFILE_LIST = ${MAKEFILE_LIST}, PWD = ${PWD}, gen_sh = ${gen_sh} "${CXX}" ${COMPILER} ${BUILD_ROOT}/env.mk)
endif

TERARK_INC := -Isrc -I3rdparty/zstd ${BOOST_INC}

include ${BUILD_ROOT}/env.mk

UNAME_System := $(shell uname | sed 's/^\([0-9a-zA-Z]*\).*/\1/')
ifeq (CYGWIN, ${UNAME_System})
  FPIC =
  # lazy expansion
  CYGWIN_LDFLAGS = -Wl,--out-implib=$@ \
				   -Wl,--export-all-symbols \
				   -Wl,--enable-auto-import
  DLL_SUFFIX = .dll.a
  CYG_DLL_FILE = $(shell echo $@ | sed 's:\(.*\)/lib\([^/]*\)\.a$$:\1/cyg\2:')
  COMMON_C_FLAGS += -D_GNU_SOURCE
else
  ifeq (Darwin,${UNAME_System})
    DLL_SUFFIX = .dylib
  else
    DLL_SUFFIX = .so
  endif
  FPIC = -fPIC
  CYG_DLL_FILE = $@
endif
override CFLAGS += ${FPIC}
override CXXFLAGS += ${FPIC}
override LDFLAGS += ${FPIC}

ifeq "$(shell a=${COMPILER};echo $${a:0:3})" "g++"
  ifeq (Linux, ${UNAME_System})
    override LDFLAGS += -rdynamic
  endif
  ifeq (${UNAME_System},Darwin)
    COMMON_C_FLAGS += -Wa,-q
  endif
  override CXXFLAGS += -time
  ifeq "$(shell echo ${COMPILER} | awk -F- '{if ($$2 >= 4.8) print 1;}')" "1"
    CXX_STD := -std=gnu++1y
  endif
  ifeq "$(shell echo ${COMPILER} | awk -F- '{if ($$2 >= 9.0) print 1;}')" "1"
    COMMON_C_FLAGS += -Wno-alloc-size-larger-than
  endif
endif

ifeq "${CXX_STD}" ""
  CXX_STD := -std=gnu++11
endif

# icc or icpc
ifeq "$(shell a=${COMPILER};echo $${a:0:2})" "ic"
  override CXXFLAGS += -xHost -fasm-blocks
  CPU = -xHost
else
  CPU = -march=haswell
  COMMON_C_FLAGS  += -Wno-deprecated-declarations
  ifeq "$(shell a=${COMPILER};echo $${a:0:5})" "clang"
    COMMON_C_FLAGS  += -fstrict-aliasing
  else
    COMMON_C_FLAGS  += -Wstrict-aliasing=3
  endif
endif

ifeq (${WITH_BMI2},1)
  CPU += -mbmi -mbmi2
else
  CPU += -mno-bmi -mno-bmi2
endif

ifneq (${WITH_TBB},)
  COMMON_C_FLAGS += -DTERARK_WITH_TBB=${WITH_TBB}
  override LIBS += -ltbb
endif

ifeq "$(shell a=${COMPILER};echo $${a:0:5})" "clang"
  COMMON_C_FLAGS += -fcolor-diagnostics
endif

#CXXFLAGS +=
#CXXFLAGS += -fpermissive
#CXXFLAGS += -fexceptions
#CXXFLAGS += -fdump-translation-unit -fdump-class-hierarchy

override CFLAGS += ${COMMON_C_FLAGS}
override CXXFLAGS += ${COMMON_C_FLAGS}
#$(error ${CXXFLAGS} "----" ${COMMON_C_FLAGS})

DEFS := -D_FILE_OFFSET_BITS=64 -D_LARGEFILE_SOURCE -D_LARGEFILE64_SOURCE
DEFS += -DDIVSUFSORT_API=
override CFLAGS   += ${DEFS}
override CXXFLAGS += ${DEFS}

override INCS := ${TERARK_INC} ${INCS}

LIBBOOST ?=
#LIBBOOST += -lboost_thread${BOOST_SUFFIX}
#LIBBOOST += -lboost_date_time${BOOST_SUFFIX}
#LIBBOOST += -lboost_system${BOOST_SUFFIX}

#LIBS += -ldl
#LIBS += -lpthread
#LIBS += ${LIBBOOST}

#extf = -pie
extf = -fno-stack-protector
#extf+=-fno-stack-protector-all
override CFLAGS += ${extf}
#override CFLAGS += -g3
override CXXFLAGS += ${extf}
#override CXXFLAGS += -g3
#CXXFLAGS += -fnothrow-opt

ifeq (, ${prefix})
  ifeq (root, ${USER})
    prefix := /usr
  else
    prefix := /home/${USER}
  endif
endif

#$(warning prefix=${prefix} LIBS=${LIBS})

#obsoleted_src =  \
#	$(wildcard src/obsoleted/terark/thread/*.cpp) \
#	$(wildcard src/obsoleted/terark/thread/posix/*.cpp) \
#	$(wildcard src/obsoleted/wordseg/*.cpp)
#LIBS += -liconv

ifneq "$(shell a=${COMPILER};echo $${a:0:5})" "clang"
  override LIBS += -lgomp
endif

c_src := \
   $(wildcard src/terark/c/*.c) \
   $(wildcard src/terark/c/*.cpp)

zip_src := \
    src/terark/io/BzipStream.cpp \
	src/terark/io/GzipStream.cpp

core_src := \
   $(wildcard src/terark/*.cpp) \
   $(wildcard src/terark/io/*.cpp) \
   $(wildcard src/terark/util/*.cpp) \
   $(wildcard src/terark/thread/*.cpp) \
   $(wildcard src/terark/succinct/*.cpp) \
   ${obsoleted_src}

core_src := $(filter-out ${zip_src}, ${core_src})

#ifeq (${UNAME_System},"DarwinWithoutC++11")
ifeq (1,1)
# lib boost-fiber can not be built by boost build
# include the source
core_src += \
  boost-include/libs/fiber/src/algo/algorithm.cpp \
  boost-include/libs/fiber/src/algo/round_robin.cpp \
  boost-include/libs/fiber/src/algo/shared_work.cpp \
  boost-include/libs/fiber/src/algo/work_stealing.cpp \
  boost-include/libs/fiber/src/barrier.cpp \
  boost-include/libs/fiber/src/condition_variable.cpp \
  boost-include/libs/fiber/src/context.cpp \
  boost-include/libs/fiber/src/fiber.cpp \
  boost-include/libs/fiber/src/future.cpp \
  boost-include/libs/fiber/src/mutex.cpp \
  boost-include/libs/fiber/src/properties.cpp \
  boost-include/libs/fiber/src/recursive_mutex.cpp \
  boost-include/libs/fiber/src/recursive_timed_mutex.cpp \
  boost-include/libs/fiber/src/timed_mutex.cpp \
  boost-include/libs/fiber/src/scheduler.cpp

  #BOOST_FIBER_DEP_LIBS := boost-include/stage/lib/libboost_thread.a
else
  BOOST_FIBER_DEP_LIBS := boost-include/stage/lib/libboost_fiber.a
endif
BOOST_FIBER_DEP_LIBS += \
  boost-include/stage/lib/libboost_context.a \
  boost-include/stage/lib/libboost_system.a

fsa_src := $(wildcard src/terark/fsa/*.cpp)
fsa_src += $(wildcard src/terark/zsrch/*.cpp)

zbs_src := $(wildcard src/terark/entropy/*.cpp)
zbs_src += $(wildcard src/terark/zbs/*.cpp)

zstd_src := $(wildcard 3rdparty/zstd/zstd/common/*.c)
zstd_src += $(wildcard 3rdparty/zstd/zstd/compress/*.c)
zstd_src += $(wildcard 3rdparty/zstd/zstd/decompress/*.c)
zstd_src += $(wildcard 3rdparty/zstd/zstd/deprecated/*.c)
zstd_src += $(wildcard 3rdparty/zstd/zstd/dictBuilder/*.c)
zstd_src += $(wildcard 3rdparty/zstd/zstd/legacy/*.c)

zbs_src += ${zstd_src}

#function definition
#@param:${1} -- targets var prefix, such as core | fsa | zbs
#@param:${2} -- build type: d | r | a
objs = $(addprefix ${${2}dir}/, $(addsuffix .o, $(basename ${${1}_src}))) \
       ${${2}dir}/${${2}dir}/git-version-${1}.o

zstd_d_o := $(call objs,zstd,d)
zstd_r_o := $(call objs,zstd,r)
zstd_a_o := $(call objs,zstd,a)

core_d_o := $(call objs,core,d)
core_r_o := $(call objs,core,r)
core_a_o := $(call objs,core,a)
core_d := ${BUILD_ROOT}/lib/libterark-core-${COMPILER}-d${DLL_SUFFIX}
core_r := ${BUILD_ROOT}/lib/libterark-core-${COMPILER}-r${DLL_SUFFIX}
core_a := ${BUILD_ROOT}/lib/libterark-core-${COMPILER}-a${DLL_SUFFIX}
static_core_d := ${BUILD_ROOT}/lib_static/libterark-core-${COMPILER}-d.a
static_core_r := ${BUILD_ROOT}/lib_static/libterark-core-${COMPILER}-r.a
static_core_a := ${BUILD_ROOT}/lib_static/libterark-core-${COMPILER}-a.a

fsa_d_o := $(call objs,fsa,d)
fsa_r_o := $(call objs,fsa,r)
fsa_a_o := $(call objs,fsa,a)
fsa_d := ${BUILD_ROOT}/lib/libterark-fsa-${COMPILER}-d${DLL_SUFFIX}
fsa_r := ${BUILD_ROOT}/lib/libterark-fsa-${COMPILER}-r${DLL_SUFFIX}
fsa_a := ${BUILD_ROOT}/lib/libterark-fsa-${COMPILER}-a${DLL_SUFFIX}
static_fsa_d := ${BUILD_ROOT}/lib_static/libterark-fsa-${COMPILER}-d.a
static_fsa_r := ${BUILD_ROOT}/lib_static/libterark-fsa-${COMPILER}-r.a
static_fsa_a := ${BUILD_ROOT}/lib_static/libterark-fsa-${COMPILER}-a.a

zbs_d_o := $(call objs,zbs,d)
zbs_r_o := $(call objs,zbs,r)
zbs_a_o := $(call objs,zbs,a)
zbs_d := ${BUILD_ROOT}/lib/libterark-zbs-${COMPILER}-d${DLL_SUFFIX}
zbs_r := ${BUILD_ROOT}/lib/libterark-zbs-${COMPILER}-r${DLL_SUFFIX}
zbs_a := ${BUILD_ROOT}/lib/libterark-zbs-${COMPILER}-a${DLL_SUFFIX}
static_zbs_d := ${BUILD_ROOT}/lib_static/libterark-zbs-${COMPILER}-d.a
static_zbs_r := ${BUILD_ROOT}/lib_static/libterark-zbs-${COMPILER}-r.a
static_zbs_a := ${BUILD_ROOT}/lib_static/libterark-zbs-${COMPILER}-a.a

core := ${core_d} ${core_r} ${core_a} ${static_core_d} ${static_core_r} ${static_core_a}
fsa  := ${fsa_d}  ${fsa_r}  ${fsa_a}  ${static_fsa_d}  ${static_fsa_r}  ${static_fsa_a}
zbs  := ${zbs_d}  ${zbs_r}  ${zbs_a}  ${static_zbs_d}  ${static_zbs_r}  ${static_zbs_a}

ALL_TARGETS = ${MAYBE_DBB_DBG} ${MAYBE_DBB_RLS} ${MAYBE_DBB_AFR} core fsa zbs
DBG_TARGETS = ${MAYBE_DBB_DBG} ${core_d} ${fsa_d} ${zbs_d}
RLS_TARGETS = ${MAYBE_DBB_RLS} ${core_r} ${fsa_r} ${zbs_r}
AFR_TARGETS = ${MAYBE_DBB_AFR} ${core_a} ${fsa_a} ${zbs_a}

.PHONY : default all core fsa zbs

default : fsa core zbs
all : ${ALL_TARGETS}
core: ${core}
fsa: ${fsa}
zbs: ${zbs}

OpenSources := $(shell find -H src 3rdparty -name '*.h' -o -name '*.hpp' -o -name '*.cc' -o -name '*.cpp' -o -name '*.c')

allsrc = ${core_src} ${fsa_src} ${zbs_src}
alldep = $(addprefix ${rdir}/, $(addsuffix .dep, $(basename ${allsrc}))) \
         $(addprefix ${adir}/, $(addsuffix .dep, $(basename ${allsrc}))) \
         $(addprefix ${ddir}/, $(addsuffix .dep, $(basename ${allsrc})))

.PHONY : dbg rls afr
dbg: ${DBG_TARGETS}
rls: ${RLS_TARGETS}
afr: ${AFR_TARGETS}

ifneq (${UNAME_System},Darwin)
${core_d} ${core_r} ${core_a} : LIBS += -lrt -lpthread
endif
${core_d} : LIBS := $(filter-out -lterark-core-${COMPILER}-d, ${LIBS})
${core_r} : LIBS := $(filter-out -lterark-core-${COMPILER}-r, ${LIBS})
${core_a} : LIBS := $(filter-out -lterark-core-${COMPILER}-a, ${LIBS})

${fsa_d} : LIBS := $(filter-out -lterark-fsa-${COMPILER}-d, -L${BUILD_ROOT}/lib -lterark-core-${COMPILER}-d ${LIBS})
${fsa_r} : LIBS := $(filter-out -lterark-fsa-${COMPILER}-r, -L${BUILD_ROOT}/lib -lterark-core-${COMPILER}-r ${LIBS})
${fsa_a} : LIBS := $(filter-out -lterark-fsa-${COMPILER}-a, -L${BUILD_ROOT}/lib -lterark-core-${COMPILER}-a ${LIBS})

${zbs_d} : LIBS := -L${BUILD_ROOT}/lib -lterark-fsa-${COMPILER}-d -lterark-core-${COMPILER}-d ${LIBS}
${zbs_r} : LIBS := -L${BUILD_ROOT}/lib -lterark-fsa-${COMPILER}-r -lterark-core-${COMPILER}-r ${LIBS}
${zbs_a} : LIBS := -L${BUILD_ROOT}/lib -lterark-fsa-${COMPILER}-a -lterark-core-${COMPILER}-a ${LIBS}

${zstd_d_o} ${zstd_r_o} ${zstd_a_o} : override CFLAGS += -Wno-sign-compare -Wno-implicit-fallthrough

${fsa_d} : $(call objs,fsa,d) ${core_d}
${fsa_r} : $(call objs,fsa,r) ${core_r}
${fsa_a} : $(call objs,fsa,a) ${core_a}
${static_fsa_d} : $(call objs,fsa,d)
${static_fsa_r} : $(call objs,fsa,r)
${static_fsa_a} : $(call objs,fsa,a)

${zbs_d} : $(call objs,zbs,d) ${fsa_d} ${core_d}
${zbs_r} : $(call objs,zbs,r) ${fsa_r} ${core_r}
${zbs_a} : $(call objs,zbs,a) ${fsa_a} ${core_a}
${static_zbs_d} : $(call objs,zbs,d)
${static_zbs_r} : $(call objs,zbs,r)
${static_zbs_a} : $(call objs,zbs,a)

${core_d}:${core_d_o} 3rdparty/base64/lib/libbase64.o boost-include/build-lib-for-terark.done
${core_r}:${core_r_o} 3rdparty/base64/lib/libbase64.o boost-include/build-lib-for-terark.done
${core_a}:${core_a_o} 3rdparty/base64/lib/libbase64.o boost-include/build-lib-for-terark.done
${static_core_d}:${core_d_o} 3rdparty/base64/lib/libbase64.o boost-include/build-lib-for-terark.done
${static_core_r}:${core_r_o} 3rdparty/base64/lib/libbase64.o boost-include/build-lib-for-terark.done
${static_core_a}:${core_a_o} 3rdparty/base64/lib/libbase64.o boost-include/build-lib-for-terark.done

ifeq (${UNAME_System},Darwin)
${core_d} ${core_r} ${core_a} : LIBS := -Wl,-all_load ${BOOST_FIBER_DEP_LIBS} -Wl,-noall_load ${LIBS}
else
${core_d} ${core_r} ${core_a} : LIBS := -Wl,--whole-archive ${BOOST_FIBER_DEP_LIBS} -Wl,--no-whole-archive ${LIBS}
endif

${static_core_d} ${static_core_r} ${static_core_a}: STATIC_FLATTEN_LIBS := ${BOOST_FIBER_DEP_LIBS}


define GenGitVersionSRC
${1}/git-version-core.cpp: ${core_src}
${1}/git-version-fsa.cpp: ${fsa_src}
${1}/git-version-zbs.cpp: ${zbs_src}
${1}/git-version-%.cpp: Makefile
	@mkdir -p $$(dir $$@)
	@rm -f $$@.tmp
	@echo '__attribute__ ((visibility ("default"))) const char*' \
		  'git_version_hash_info_'$$(patsubst git-version-%.cpp,%,$$(notdir $$@))\
		  '() { return R"StrLiteral(git_version_hash_info_is:' > $$@.tmp
	@env LC_ALL=C git log -n1 >> $$@.tmp
	@env LC_ALL=C git diff >> $$@.tmp
	@env LC_ALL=C $(CXX) --version >> $$@.tmp
	@echo INCS = ${INCS}           >> $$@.tmp
	@echo CXXFLAGS  = ${CXXFLAGS}  >> $$@.tmp
	@echo ${2} >> $$@.tmp # DBG_FLAGS | RLS_FLAGS | AFR_FLAGS
	@echo WITH_BMI2 = ${WITH_BMI2} >> $$@.tmp
	@echo WITH_TBB  = ${WITH_TBB}  >> $$@.tmp
	@echo compile_cpu_flag: $(CPU) >> $$@.tmp
	@#echo machine_cpu_flag: Begin  >> $$@.tmp
	@#bash ./cpu_features.sh        >> $$@.tmp
	@#echo machine_cpu_flag: End    >> $$@.tmp
	@echo ')''StrLiteral";}' >> $$@.tmp
	@#      ^^----- To prevent diff causing git-version compile fail
	@if test -f "$$@" && cmp "$$@" $$@.tmp; then \
		rm $$@.tmp; \
	else \
		mv $$@.tmp $$@; \
	fi
endef

$(eval $(call GenGitVersionSRC, ${ddir}, "DBG_FLAGS = ${DBG_FLAGS}"))
$(eval $(call GenGitVersionSRC, ${rdir}, "RLS_FLAGS = ${RLS_FLAGS}"))
$(eval $(call GenGitVersionSRC, ${adir}, "AFR_FLAGS = ${AFR_FLAGS}"))

3rdparty/base64/lib/libbase64.o:
	$(MAKE) -C 3rdparty/base64 clean; \
	$(MAKE) -C 3rdparty/base64 lib/libbase64.o \
		CFLAGS="-fPIC -std=c99 -O3 -Wall -Wextra -pedantic"
		#AVX2_CFLAGS=-mavx2 SSE41_CFLAGS=-msse4.1 SSE42_CFLAGS=-msse4.2 AVX_CFLAGS=-mavx

boost-include/build-lib-for-terark.done:
	cd boost-include \
		&& bash bootstrap.sh --with-libraries=fiber,context,system,filesystem \
		&& ./b2 -j8 cxxflags="-fPIC -std=gnu++14" cflags=-fPIC
	touch $@

%${DLL_SUFFIX}:
	@echo "----------------------------------------------------------------------------------"
	@echo "Creating dynamic library: $@"
	@echo BOOST_INC=${BOOST_INC} BOOST_SUFFIX=${BOOST_SUFFIX}
	@echo -e "OBJS:" $(addprefix "\n  ",$(sort $(filter %.o,$^)))
	@echo -e "LIBS:" $(addprefix "\n  ",${LIBS})
	mkdir -p ${BUILD_ROOT}/lib
	@rm -f $@
	${LD} -shared $(sort $(filter %.o,$^)) ${LDFLAGS} ${LIBS} -o ${CYG_DLL_FILE} ${CYGWIN_LDFLAGS}
	cd $(dir $@); ln -sf $(notdir $@) $(subst -${COMPILER},,$(notdir $@))
ifeq (CYGWIN, ${UNAME_System})
	@cp -l -f ${CYG_DLL_FILE} /usr/bin
endif

%.a:
	@echo "----------------------------------------------------------------------------------"
	@echo "Creating static library: $@"
	@echo BOOST_INC=${BOOST_INC} BOOST_SUFFIX=${BOOST_SUFFIX}
	@echo -e "OBJS:" $(addprefix "\n  ",$(sort $(filter %.o,$^) ${EXTRA_OBJECTS}))
	@echo -e "LIBS:" $(addprefix "\n  ",${LIBS})
	@mkdir -p $(dir $@)
	@rm -f $@
	@echo STATIC_FLATTEN_LIBS = "${STATIC_FLATTEN_LIBS}"
	@if test -n "${STATIC_FLATTEN_LIBS}" -a `uname` = Darwin; then \
		echo; \
		echo Mac OS does not support ar script, you need to add link libs:; \
		for f in ${STATIC_FLATTEN_LIBS}; do echo "    $$f"; done; \
	fi
	@echo
	@if test -n "${STATIC_FLATTEN_LIBS}" -a `uname` != Darwin; then \
		tmp=`mktemp -u XXXXXX.tmp.a`; \
		set -x; \
		${AR} rcs $$tmp $(filter %.o,$^) ${EXTRA_OBJECTS}; \
		(\
		echo open $$tmp; \
		for f in ${STATIC_FLATTEN_LIBS}; do \
		  echo addlib $$f; \
		done; \
		echo save; \
		echo end; \
		) | ar -M; \
		mv $$tmp $@; \
	else \
		${AR} rcs $@ $(filter %.o,$^) ${EXTRA_OBJECTS}; \
	fi
	cd $(dir $@); ln -sf $(notdir $@) $(subst -${COMPILER},,$(notdir $@))

.PHONY : install
install : core
	cp ${BUILD_ROOT}/lib/* ${prefix}/lib/

.PHONY : clean
clean:
	@for f in `find * -name "*${BUILD_NAME}*"`; do \
		echo rm -rf $${f}; \
		rm -rf $${f}; \
	done

.PHONY : cleanall
cleanall:
	@for f in `find * -name build`; do \
		echo rm -rf $${f}; \
		rm -rf $${f}; \
	done
	rm -rf pkg

.PHONY : depends
depends : ${alldep}

TarBallBaseName := terark-fsa_all-${BUILD_NAME}
TarBall := pkg/${TarBallBaseName}
.PHONY : pkg
.PHONY : tgz
pkg : ${TarBall}
tgz : ${TarBall}.tgz

${TarBall}: $(wildcard tools/general/*.cpp) \
			$(wildcard tools/fsa/*.cpp) \
			$(wildcard tools/zbs/*.cpp) \
			${core} ${fsa} ${zbs}
	+${MAKE} CHECK_TERARK_FSA_LIB_UPDATE=0 -C tools/fsa
	+${MAKE} CHECK_TERARK_FSA_LIB_UPDATE=0 -C tools/zbs
	+${MAKE} CHECK_TERARK_FSA_LIB_UPDATE=0 -C tools/general
	rm -rf ${TarBall}
	mkdir -p ${TarBall}/bin
	mkdir -p ${TarBall}/lib
	mkdir -p ${TarBall}/include/terark/entropy
	mkdir -p ${TarBall}/include/terark/thread
	mkdir -p ${TarBall}/include/terark/succinct
	mkdir -p ${TarBall}/include/terark/io/win
	mkdir -p ${TarBall}/include/terark/util
	mkdir -p ${TarBall}/include/terark/fsa
	mkdir -p ${TarBall}/include/terark/fsa/ppi
	mkdir -p ${TarBall}/include/terark/zbs
	mkdir -p ${TarBall}/include/zstd/common
	cp    src/terark/bits_rotate.hpp             ${TarBall}/include/terark
	cp    src/terark/bitfield_array.hpp          ${TarBall}/include/terark
	cp    src/terark/bitfield_array_access.hpp   ${TarBall}/include/terark
	cp    src/terark/bitmanip.hpp                ${TarBall}/include/terark
	cp    src/terark/bitmap.hpp                  ${TarBall}/include/terark
	cp    src/terark/config.hpp                  ${TarBall}/include/terark
	cp    src/terark/cxx_features.hpp            ${TarBall}/include/terark
	cp    src/terark/fstring.hpp                 ${TarBall}/include/terark
	cp    src/terark/histogram.hpp               ${TarBall}/include/terark
	cp    src/terark/int_vector.hpp              ${TarBall}/include/terark
	cp    src/terark/lcast.hpp                   ${TarBall}/include/terark
	cp    src/terark/*hash*.hpp                  ${TarBall}/include/terark
	cp    src/terark/heap_ext.hpp                ${TarBall}/include/terark
	cp    src/terark/mempool*.hpp                ${TarBall}/include/terark
	cp    src/terark/node_layout.hpp             ${TarBall}/include/terark
	cp    src/terark/num_to_str.hpp              ${TarBall}/include/terark
	cp    src/terark/parallel_lib.hpp            ${TarBall}/include/terark
	cp    src/terark/pass_by_value.hpp           ${TarBall}/include/terark
	cp    src/terark/rank_select.hpp             ${TarBall}/include/terark
	cp    src/terark/stdtypes.hpp                ${TarBall}/include/terark
	cp    src/terark/valvec.hpp                  ${TarBall}/include/terark
	cp    src/terark/entropy/*.hpp               ${TarBall}/include/terark/entropy
	cp    src/terark/io/*.hpp                    ${TarBall}/include/terark/io
	cp    src/terark/io/win/*.hpp                ${TarBall}/include/terark/io/win
	cp    src/terark/util/*.hpp                  ${TarBall}/include/terark/util
	cp    src/terark/fsa/*.hpp                   ${TarBall}/include/terark/fsa
	cp    src/terark/fsa/*.inl                   ${TarBall}/include/terark/fsa
	cp    src/terark/fsa/ppi/*.hpp               ${TarBall}/include/terark/fsa/ppi
	cp    src/terark/zbs/*.hpp                   ${TarBall}/include/terark/zbs
	cp    src/terark/thread/*.hpp                ${TarBall}/include/terark/thread
	cp    src/terark/succinct/*.hpp              ${TarBall}/include/terark/succinct
	cp    3rdparty/zstd/zstd/*.h                 ${TarBall}/include/zstd
	cp    3rdparty/zstd/zstd/common/*.h          ${TarBall}/include/zstd/common
ifeq (${PKG_WITH_DBG},1)
	cp -a ${BUILD_ROOT}/lib/libterark-{fsa,zbs,core}-*d${DLL_SUFFIX} ${TarBall}/lib
	cp -a ${BUILD_ROOT}/lib/libterark-{fsa,zbs,core}-*a${DLL_SUFFIX} ${TarBall}/lib
  ifeq (${PKG_WITH_STATIC},1)
	mkdir -p ${TarBall}/lib_static
	cp -a ${BUILD_ROOT}/lib_static/libterark-{fsa,zbs,core}-{${COMPILER}-,}d.a ${TarBall}/lib_static
	cp -a ${BUILD_ROOT}/lib_static/libterark-{fsa,zbs,core}-{${COMPILER}-,}a.a ${TarBall}/lib_static
  endif
endif
	cp -a ${BUILD_ROOT}/lib/libterark-{fsa,zbs,core}-*r${DLL_SUFFIX} ${TarBall}/lib
	echo $(shell date "+%Y-%m-%d %H:%M:%S") > ${TarBall}/package.buildtime.txt
	echo $(shell git log | head -n1) >> ${TarBall}/package.buildtime.txt
ifeq (${PKG_WITH_STATIC},1)
	mkdir -p ${TarBall}/lib_static
	cp -a ${BUILD_ROOT}/lib_static/libterark-{fsa,zbs,core}-{${COMPILER}-,}r.a ${TarBall}/lib_static
endif
	cp -L tools/*/rls/*.exe ${TarBall}/bin/

${TarBall}.tgz: ${TarBall}
	cd pkg; tar czf ${TarBallBaseName}.tgz ${TarBallBaseName}

.PONY: test
.PONY: test_dbg
.PONY: test_afr
.PONY: test_rls
test: test_dbg test_afr test_rls

test_dbg: ${zbs_d} ${fsa_d} ${core_d}
	+$(MAKE) -C tests/core        test_dbg  CHECK_TERARK_FSA_LIB_UPDATE=0
	+$(MAKE) -C tests/tries       test_dbg  CHECK_TERARK_FSA_LIB_UPDATE=0
	+$(MAKE) -C tests/succinct    test_dbg  CHECK_TERARK_FSA_LIB_UPDATE=0

test_afr: ${zbs_a} ${fsa_a} ${core_a}
	+$(MAKE) -C tests/core        test_afr  CHECK_TERARK_FSA_LIB_UPDATE=0
	+$(MAKE) -C tests/tries       test_afr  CHECK_TERARK_FSA_LIB_UPDATE=0
	+$(MAKE) -C tests/succinct    test_afr  CHECK_TERARK_FSA_LIB_UPDATE=0

test_rls: ${zbs_r} ${fsa_r} ${core_r}
	+$(MAKE) -C tests/core        test_rls  CHECK_TERARK_FSA_LIB_UPDATE=0
	+$(MAKE) -C tests/tries       test_rls  CHECK_TERARK_FSA_LIB_UPDATE=0
	+$(MAKE) -C tests/succinct    test_rls  CHECK_TERARK_FSA_LIB_UPDATE=0

ifneq ($(MAKECMDGOALS),cleanall)
ifneq ($(MAKECMDGOALS),clean)
-include ${alldep}
endif
endif

#@param ${1} file name suffix: cpp | cxx | cc
#@PARAM ${2} build dir       : ddir | rdir | adir
#@param ${3} debug flag      : DBG_FLAGS | RLS_FLAGS | AFR_FLAGS
define COMPILE_CXX
${2}/%.o: %.${1}
	@echo file: $$< "->" $$@
	@echo TERARK_INC=${TERARK_INC} BOOST_INC=${BOOST_INC} BOOST_SUFFIX=${BOOST_SUFFIX}
	@mkdir -p $$(dir $$@)
	${CXX} ${CXX_STD} ${CPU} -c ${3} ${CXXFLAGS} ${INCS} $$< -o $$@
${2}/%.s : %.${1}
	@echo file: $$< "->" $$@
	${CXX} -S -fverbose-asm ${CXX_STD} ${CPU} ${3} ${CXXFLAGS} ${INCS} $$< -o $$@
${2}/%.dep : %.${1}
	@echo file: $$< "->" $$@
	@echo INCS = ${INCS}
	mkdir -p $$(dir $$@)
	-${CXX} ${CXX_STD} ${3} -M -MT $$(basename $$@).o ${INCS} $$< > $$@
endef

define COMPILE_C
${2}/%.o : %.${1}
	@echo file: $$< "->" $$@
	mkdir -p $$(dir $$@)
	${CC} -c ${CPU} ${3} ${CFLAGS} ${INCS} $$< -o $$@
${2}/%.s : %.${1}
	@echo file: $$< "->" $$@
	${CC} -S -fverbose-asm ${CPU} ${3} ${CFLAGS} ${INCS} $$< -o $$@
${2}/%.dep : %.${1}
	@echo file: $$< "->" $$@
	@echo INCS = ${INCS}
	mkdir -p $$(dir $$@)
	-${CC} ${3} -M -MT $$(basename $$@).o ${INCS} $$< > $$@
endef

$(eval $(call COMPILE_CXX,cpp,${ddir},${DBG_FLAGS}))
$(eval $(call COMPILE_CXX,cxx,${ddir},${DBG_FLAGS}))
$(eval $(call COMPILE_CXX,cc ,${ddir},${DBG_FLAGS}))
$(eval $(call COMPILE_CXX,cpp,${rdir},${RLS_FLAGS}))
$(eval $(call COMPILE_CXX,cxx,${rdir},${RLS_FLAGS}))
$(eval $(call COMPILE_CXX,cc ,${rdir},${RLS_FLAGS}))
$(eval $(call COMPILE_CXX,cpp,${adir},${AFR_FLAGS}))
$(eval $(call COMPILE_CXX,cxx,${adir},${AFR_FLAGS}))
$(eval $(call COMPILE_CXX,cc ,${adir},${AFR_FLAGS}))
$(eval $(call COMPILE_C  ,c  ,${ddir},${DBG_FLAGS}))
$(eval $(call COMPILE_C  ,c  ,${rdir},${RLS_FLAGS}))
$(eval $(call COMPILE_C  ,c  ,${adir},${AFR_FLAGS}))

# disable buildin suffix-rules
.SUFFIXES:
