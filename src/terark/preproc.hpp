#ifndef __terark_preproc_hpp__
#define __terark_preproc_hpp__

#define TERARK_PP_CAT2_1(a,b)    a##b
#define TERARK_PP_CAT2(a,b)      TERARK_PP_CAT2_1(a,b)
#define TERARK_PP_CAT3(a,b,c)    TERARK_PP_CAT2(TERARK_PP_CAT2(a,b),c)
#define TERARK_PP_CAT4(a,b,c,d)  TERARK_PP_CAT2(TERARK_PP_CAT3(a,b,c),d)

#define TERARK_PP_EXTENT(arr) (sizeof(arr)/sizeof(arr[0]))

#define TERARK_PP_ARG_X(_0,_1,_2,_3,_4,_5,_6,_7,_8,_9,a,b,c,d,e,f,g,h,i,j,k,l,m,n,o,p,q,r,s,t,u,v,w,x,y,z,A,B,C,E,F,G,H,I,J,K,L,M,N,...) N
#define TERARK_PP_ARG_N(...) \
        TERARK_PP_ARG_X("ignored", ##__VA_ARGS__, \
                                    M,L,K,J,I,H,G,F,E,D,C,B,A, \
            z,y,x,w,v,u,t,s,r,q,p,o,m,l,k,j,i,h,g,f,e,d,c,b,a, \
                                            9,8,7,6,5,4,3,2,1,0)

///@{
///@param c context
#define TERARK_PP_MAP_0(map,c)
#define TERARK_PP_MAP_1(map,c,x,...) map(c,x)
#define TERARK_PP_MAP_2(map,c,x,...) map(c,x),TERARK_PP_MAP_1(map,c,__VA_ARGS__)
#define TERARK_PP_MAP_3(map,c,x,...) map(c,x),TERARK_PP_MAP_2(map,c,__VA_ARGS__)
#define TERARK_PP_MAP_4(map,c,x,...) map(c,x),TERARK_PP_MAP_3(map,c,__VA_ARGS__)
#define TERARK_PP_MAP_5(map,c,x,...) map(c,x),TERARK_PP_MAP_4(map,c,__VA_ARGS__)
#define TERARK_PP_MAP_6(map,c,x,...) map(c,x),TERARK_PP_MAP_5(map,c,__VA_ARGS__)
#define TERARK_PP_MAP_7(map,c,x,...) map(c,x),TERARK_PP_MAP_6(map,c,__VA_ARGS__)
#define TERARK_PP_MAP_8(map,c,x,...) map(c,x),TERARK_PP_MAP_7(map,c,__VA_ARGS__)
#define TERARK_PP_MAP_9(map,c,x,...) map(c,x),TERARK_PP_MAP_8(map,c,__VA_ARGS__)
#define TERARK_PP_MAP_a(map,c,x,...) map(c,x),TERARK_PP_MAP_9(map,c,__VA_ARGS__)
#define TERARK_PP_MAP_b(map,c,x,...) map(c,x),TERARK_PP_MAP_a(map,c,__VA_ARGS__)
#define TERARK_PP_MAP_c(map,c,x,...) map(c,x),TERARK_PP_MAP_b(map,c,__VA_ARGS__)
#define TERARK_PP_MAP_d(map,c,x,...) map(c,x),TERARK_PP_MAP_c(map,c,__VA_ARGS__)
#define TERARK_PP_MAP_e(map,c,x,...) map(c,x),TERARK_PP_MAP_d(map,c,__VA_ARGS__)
#define TERARK_PP_MAP_f(map,c,x,...) map(c,x),TERARK_PP_MAP_e(map,c,__VA_ARGS__)
#define TERARK_PP_MAP_g(map,c,x,...) map(c,x),TERARK_PP_MAP_f(map,c,__VA_ARGS__)
#define TERARK_PP_MAP_h(map,c,x,...) map(c,x),TERARK_PP_MAP_g(map,c,__VA_ARGS__)
#define TERARK_PP_MAP_i(map,c,x,...) map(c,x),TERARK_PP_MAP_h(map,c,__VA_ARGS__)
#define TERARK_PP_MAP_j(map,c,x,...) map(c,x),TERARK_PP_MAP_i(map,c,__VA_ARGS__)
#define TERARK_PP_MAP_k(map,c,x,...) map(c,x),TERARK_PP_MAP_j(map,c,__VA_ARGS__)
#define TERARK_PP_MAP_l(map,c,x,...) map(c,x),TERARK_PP_MAP_k(map,c,__VA_ARGS__)
#define TERARK_PP_MAP_m(map,c,x,...) map(c,x),TERARK_PP_MAP_l(map,c,__VA_ARGS__)
#define TERARK_PP_MAP_n(map,c,x,...) map(c,x),TERARK_PP_MAP_m(map,c,__VA_ARGS__)
#define TERARK_PP_MAP_o(map,c,x,...) map(c,x),TERARK_PP_MAP_n(map,c,__VA_ARGS__)
#define TERARK_PP_MAP_p(map,c,x,...) map(c,x),TERARK_PP_MAP_o(map,c,__VA_ARGS__)
#define TERARK_PP_MAP_q(map,c,x,...) map(c,x),TERARK_PP_MAP_p(map,c,__VA_ARGS__)
#define TERARK_PP_MAP_r(map,c,x,...) map(c,x),TERARK_PP_MAP_q(map,c,__VA_ARGS__)
#define TERARK_PP_MAP_s(map,c,x,...) map(c,x),TERARK_PP_MAP_r(map,c,__VA_ARGS__)
#define TERARK_PP_MAP_t(map,c,x,...) map(c,x),TERARK_PP_MAP_s(map,c,__VA_ARGS__)
#define TERARK_PP_MAP_u(map,c,x,...) map(c,x),TERARK_PP_MAP_t(map,c,__VA_ARGS__)
#define TERARK_PP_MAP_v(map,c,x,...) map(c,x),TERARK_PP_MAP_u(map,c,__VA_ARGS__)
#define TERARK_PP_MAP_w(map,c,x,...) map(c,x),TERARK_PP_MAP_v(map,c,__VA_ARGS__)
#define TERARK_PP_MAP_x(map,c,x,...) map(c,x),TERARK_PP_MAP_w(map,c,__VA_ARGS__)
#define TERARK_PP_MAP_y(map,c,x,...) map(c,x),TERARK_PP_MAP_x(map,c,__VA_ARGS__)
#define TERARK_PP_MAP_z(map,c,x,...) map(c,x),TERARK_PP_MAP_y(map,c,__VA_ARGS__)
#define TERARK_PP_MAP_A(map,c,x,...) map(c,x),TERARK_PP_MAP_z(map,c,__VA_ARGS__)
#define TERARK_PP_MAP_B(map,c,x,...) map(c,x),TERARK_PP_MAP_A(map,c,__VA_ARGS__)
#define TERARK_PP_MAP_C(map,c,x,...) map(c,x),TERARK_PP_MAP_B(map,c,__VA_ARGS__)
#define TERARK_PP_MAP_D(map,c,x,...) map(c,x),TERARK_PP_MAP_C(map,c,__VA_ARGS__)
#define TERARK_PP_MAP_E(map,c,x,...) map(c,x),TERARK_PP_MAP_D(map,c,__VA_ARGS__)
#define TERARK_PP_MAP_F(map,c,x,...) map(c,x),TERARK_PP_MAP_E(map,c,__VA_ARGS__)
#define TERARK_PP_MAP_G(map,c,x,...) map(c,x),TERARK_PP_MAP_F(map,c,__VA_ARGS__)
#define TERARK_PP_MAP_H(map,c,x,...) map(c,x),TERARK_PP_MAP_G(map,c,__VA_ARGS__)
#define TERARK_PP_MAP_I(map,c,x,...) map(c,x),TERARK_PP_MAP_H(map,c,__VA_ARGS__)
#define TERARK_PP_MAP_J(map,c,x,...) map(c,x),TERARK_PP_MAP_I(map,c,__VA_ARGS__)
#define TERARK_PP_MAP_K(map,c,x,...) map(c,x),TERARK_PP_MAP_J(map,c,__VA_ARGS__)
#define TERARK_PP_MAP_L(map,c,x,...) map(c,x),TERARK_PP_MAP_K(map,c,__VA_ARGS__)
#define TERARK_PP_MAP_M(map,c,x,...) map(c,x),TERARK_PP_MAP_L(map,c,__VA_ARGS__)
///@}

/// @param map map function, can be a macro, called as map(c,x)
/// @param c   context
/// @param ... arg list to apply map function: map(c,x)
/// @returns comma seperated list: map(c,arg1), map(c,arg2), ...
#define TERARK_PP_MAP(map,c,...) TERARK_PP_CAT2(TERARK_PP_MAP_,\
  TERARK_PP_ARG_N(__VA_ARGS__))(map,c,##__VA_ARGS__)

/// @param dummy unused param 'context'
#define TERARK_PP_IDENTITY_MAP_OP(dummy, x) x

/// @param prefix is param 'c'(context) in TERARK_PP_MAP
#define TERARK_PP_PREPEND(prefix, x) prefix x

/// @{ TERARK_PP_STR is a use case of TERARK_PP_MAP
/// macro TERARK_PP_STR_2 is the 'map' function
/// TERARK_PP_STR(a)     will produce: "a"
/// TERARK_PP_STR(a,b,c) will produce: "a", "b", "c"
/// so TERARK_PP_STR is a generic stringize macro
#define TERARK_PP_STR_1(c,x) #x
#define TERARK_PP_STR_2(c,x) TERARK_PP_STR_1(c,x)
#define TERARK_PP_STR(...) TERARK_PP_MAP(TERARK_PP_STR_2,~, __VA_ARGS__)
/// @}

#endif // __terark_preproc_hpp__

