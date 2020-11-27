#!/bin/bash
#set -x

CXX=$1
COMPILER=$2
EnvConf=$3
echo COMPILER=$COMPILER 1>&2

#EnvConf=Make.env.conf-${COMPILER}

rm -f $EnvConf
mkdir -p `dirname $EnvConf`

if false; then # comment out BDB
if test -z "$BDB_HOME"; then
	hasbdb=0
	for dir in "" /usr /usr/local /opt "$HOME" "$HOME/opt"
	do
		if [ -f ${dir}/include/db.h ]; then
			BDB_VER=`sed -n 's/[# \t]*define.*DB_VERSION_STRING.*Berkeley DB \([0-9]*\.[0-9]*\).*:.*/\1/p' ${dir}/include/db.h`
			if [ -z "$BDB_VER" ]; then
				echo can not find version number in ${dir}/include/db.h, try next >&2
			else
				BDB_HOME=$dir
				hasbdb=1
				break
			fi
		fi
	done
else
	hasbdb=1
	BDB_VER=`sed -n 's/[# \t]*define.*DB_VERSION_STRING.*Berkeley DB \([0-9]*\.[0-9]*\).*:.*/\1/p' ${BDB_HOME}/include/db.h`
fi

#------------------------------------------------
if [ $hasbdb -eq 0 ]; then
	echo "couldn't found BerkeleyDB" 1>&2
else
	echo "found BerkeleyDB-${BDB_VER}" 1>&2
cat >> $EnvConf << EOF
	BDB_HOME := $BDB_HOME
	BDB_VER  := $BDB_VER
	MAYBE_BDB_DBG = \${bdb_util_d}
	MAYBE_BDB_RLS = \${bdb_util_r}
EOF
#------------------------------------------------
fi
fi # if false, comment out BDB

fname=is_cygwin_$$
cat > ${fname}.cpp << "EOF"
#include <stdio.h>
int main() {
  #ifdef __CYGWIN__
    printf("1");
  #else
    printf("0");
  #endif
    return 0;
}
EOF
if $CXX ${fname}.cpp -o ${fname}.exe; then
	IS_CYGWIN=`./${fname}.exe`
	echo IS_CYGWIN=$IS_CYGWIN >> $EnvConf
fi
rm -f ${fname}.*

if false; then
fname=has_inheriting_cons_$$
cat > ${fname}.cpp << "EOF"
struct A {
	A(int) {}
	A(int,int){}
};
struct B : public A {
	using A::A;
};
int main() {
	B b1(111);
	B b2(2,2);
	return 0;
}
EOF
rm -f src/terark/my_auto_config.hpp
touch src/terark/my_auto_config.hpp
if $CXX -std=c++11 ${fname}.cpp > /dev/null 2>&1; then
	echo '#define TERARK_HAS_INHERITING_CONSTRUCTORS' >> src/terark/my_auto_config.hpp
fi
rm -f ${fname}.cpp

if [ "$IS_CYGWIN" -eq 1 ]; then
	rm -f a.exe
else
	rm -f a.out
fi
fi # if false
