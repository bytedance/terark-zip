#include <terark/preproc.hpp>
#include <terark/fstring.hpp>
#include <stdio.h>
#include <string.h>
#include <ctype.h>

TERARK_ENUM_PLAIN(EPlain, Value1, Value2, Value3)
TERARK_ENUM_CLASS(EClass, Value1, Value2, Value3)

int main() {
    printf("TERARK_PP_ARG_N(a,b,c) = %s\n", TERARK_PP_STR(TERARK_PP_ARG_N(a,b,c)));
    printf("TERARK_PP_STR(a) = %s\n", TERARK_PP_STR(a));
    printf("TERARK_PP_STR(a, b) = %s, %s\n", TERARK_PP_STR(a, b));
    printf("TERARK_PP_STR(1,2,3,4,5,6,7,8,9,A,B,C,D,E,F,G,H,I,J,K,L,M) = "
                         "%s%s%s%s%s%s%s%s%s%s%s%s%s%s%s%s%s%s%s%s%s%s\n",
            TERARK_PP_STR(1,2,3,4,5,6,7,8,9,A,B,C,D,E,F,G,H,I,J,K,L,M));
    printf("TERARK_PP_STR(1,2,3,4,5,6,7,8,9,a,b,c,d,e,f,g,h,i,j,k,l,m,n,o,p,q,r,s,t,u,v,w,x,y,z,A,B,C,D,E,F,G,H,I,J,K,L,M,N,O,P,Q,R,S,T,U,V,W,X,Y,Z) = "
                         "%s%s%s%s%s%s%s%s%s%s%s%s%s%s%s%s%s%s%s%s%s%s%s%s%s%s%s%s%s%s%s%s%s%s%s%s%s%s%s%s%s%s%s%s%s%s%s%s%s%s%s%s%s%s%s%s%s%s%s%s%s\n",
            TERARK_PP_STR(1,2,3,4,5,6,7,8,9,a,b,c,d,e,f,g,h,i,j,k,l,m,n,o,p,q,r,s,t,u,v,w,x,y,z,A,B,C,D,E,F,G,H,I,J,K,L,M,N,O,P,Q,R,S,T,U,V,W,X,Y,Z));
    const char* count[] = {
            TERARK_PP_STR(1,2,3,4,5,6,7,8,9,a,b,c,d,e,f,g,h,i,j,k,l,m,n,o,p,q,r,s,t,u,v,w,x,y,z,A,B,C,D,E,F,G,H,I,J,K,L,M,N,O,P,Q,R,S,T,U,V,W,X,Y,Z)
    };
    printf("MAX_ARGS = %d\n", int(TERARK_PP_EXTENT(count)));

    long long _1 = 1, _12 = 0x12, _123 = 0x123, _1234 = 0x1234, _12345 = 0x12345;
    long long _123456789abcdef = 0x123456789abcdef;

    long long TERARK_PP_CAT(_) = 0;

    printf("_ = %#llx, _1 = %#llx, _12 = %#llx, _123 = %#llx, _1234 = %#llx, _1235 = %#llx\n",
        TERARK_PP_CAT(_),
        TERARK_PP_CAT(_,1),
        TERARK_PP_CAT(_,1,2),
        TERARK_PP_CAT(_,1,2,3),
        TERARK_PP_CAT(_,1,2,3,4),
        TERARK_PP_CAT(_,1,2,3,4,5));
    printf("_123456789abcdef = %#llx\n", TERARK_PP_CAT(_,1,2,3,4,5,6,7,8,9,a,b,c,d,e,f));
    printf("strlen(0123456789abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ) = %zd\n",
        strlen(TERARK_PP_STR(
            TERARK_PP_CAT(0,1,2,3,4,5,6,7,8,9,a,b,c,d,e,f,g,h,i,j,k,l,m,n,o,p,q,r,s,t,u,v,w,x,y,z,A,B,C,D,E,F,G,H,I,J,K,L,M,N,O,P,Q,R,S,T,U,V,W,X,Y,Z)
        )));
    printf("PP_CAT(0123456789abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ) = %s\n",
        TERARK_PP_STR(
            TERARK_PP_CAT(0,1,2,3,4,5,6,7,8,9,a,b,c,d,e,f,g,h,i,j,k,l,m,n,o,p,q,r,s,t,u,v,w,x,y,z,A,B,C,D,E,F,G,H,I,J,K,L,M,N,O,P,Q,R,S,T,U,V,W,X,Y,Z)
        ));

    printf("EPlain: num_of_EPlain(Value2) = %zd\n", num_of_EPlain());
    printf("EPlain: name_of_EPlain(Value2) = %s\n", name_of_EPlain(Value2));
    printf("EPlain: name_of_EPlain(Value3) = %s\n", name_of_EPlain(Value3));

    printf("EPlain: value_of_EPlain('Value1') = %d\n", value_of_EPlain("Value1"));
    printf("EPlain: value_of_EPlain('Value2') = %d\n", value_of_EPlain("Value2"));
    printf("EPlain: value_of_EPlain('Value3') = %d\n", value_of_EPlain("Value3"));

    try {
        printf("EPlain: value_of_EPlain('Unknown') = %d\n", value_of_EPlain("Unknown"));
        assert(false);
    }
    catch (const std::invalid_argument& ex) {
        printf("EPlain: catched: %s\n", ex.what());
    }

    printf("EClass: num_of_EClass(Value2) = %zd\n", num_of_EClass());
    printf("EClass: name_of_EClass(Value2) = %s\n", name_of_EClass(EClass::Value2));
    printf("EClass: name_of_EClass(Value3) = %s\n", name_of_EClass(EClass::Value3));

    printf("EClass: value_of_EClass('Value1') = %d\n", value_of_EClass("Value1"));
    printf("EClass: value_of_EClass('Value2') = %d\n", value_of_EClass("Value2"));
    printf("EClass: value_of_EClass('Value3') = %d\n", value_of_EClass("Value3"));

    try {
        printf("EClass: value_of_EClass('Unknown') = %d\n", value_of_EClass("Unknown"));
        assert(false);
    }
    catch (const std::invalid_argument& ex) {
        printf("EClass: catched: %s\n", ex.what());
    }

    return 0;
}
