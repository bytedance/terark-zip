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
    return 0;
}
