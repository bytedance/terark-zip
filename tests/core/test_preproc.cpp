#include <terark/preproc.hpp>
#include <stdio.h>

int main() {
    printf("TERARK_PP_ARG_N(a,b,c) = %s\n", TERARK_PP_STR(TERARK_PP_ARG_N(a,b,c)));
    printf("TERARK_PP_STR(a) = %s\n", TERARK_PP_STR(a));
    printf("TERARK_PP_STR(a, b) = %s, %s\n", TERARK_PP_STR(a, b));
    printf("TERARK_PP_STR(1,2,3,4,5,6,7,8,9,A,B,C,D,E,F,G,H,I,J,K,L,M) = "
                         "%s%s%s%s%s%s%s%s%s%s%s%s%s%s%s%s%s%s%s%s%s%s\n",
            TERARK_PP_STR(1,2,3,4,5,6,7,8,9,A,B,C,D,E,F,G,H,I,J,K,L,M));
    printf("TERARK_PP_STR(1,2,3,4,5,6,7,8,9,a,b,c,d,e,f,g,h,i,j,k,l,m,n,o,p,q,r,s,t,u,v,w,x,y,z,A,B,C,D,E,F,G,H,I,J,K,L,M) = "
                         "%s%s%s%s%s%s%s%s%s%s%s%s%s%s%s%s%s%s%s%s%s%s%s%s%s%s%s%s%s%s%s%s%s%s%s%s%s%s%s%s%s%s%s%s%s%s%s%s\n",
            TERARK_PP_STR(1,2,3,4,5,6,7,8,9,a,b,c,d,e,f,g,h,i,j,k,l,m,n,o,p,q,r,s,t,u,v,w,x,y,z,A,B,C,D,E,F,G,H,I,J,K,L,M));
    const char* count[] = {
            TERARK_PP_STR(1,2,3,4,5,6,7,8,9,a,b,c,d,e,f,g,h,i,j,k,l,m,n,o,p,q,r,s,t,u,v,w,x,y,z,A,B,C,D,E,F,G,H,I,J,K,L,M)
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

    return 0;
}
