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
///@param m map function
///@param c context
#define TERARK_PP_MAP_0(m,c)
#define TERARK_PP_MAP_1(m,c,x,...) m(c,x)
#define TERARK_PP_MAP_2(m,c,x,...) m(c,x),TERARK_PP_MAP_1(m,c,__VA_ARGS__)
#define TERARK_PP_MAP_3(m,c,x,...) m(c,x),TERARK_PP_MAP_2(m,c,__VA_ARGS__)
#define TERARK_PP_MAP_4(m,c,x,...) m(c,x),TERARK_PP_MAP_3(m,c,__VA_ARGS__)
#define TERARK_PP_MAP_5(m,c,x,...) m(c,x),TERARK_PP_MAP_4(m,c,__VA_ARGS__)
#define TERARK_PP_MAP_6(m,c,x,...) m(c,x),TERARK_PP_MAP_5(m,c,__VA_ARGS__)
#define TERARK_PP_MAP_7(m,c,x,...) m(c,x),TERARK_PP_MAP_6(m,c,__VA_ARGS__)
#define TERARK_PP_MAP_8(m,c,x,...) m(c,x),TERARK_PP_MAP_7(m,c,__VA_ARGS__)
#define TERARK_PP_MAP_9(m,c,x,...) m(c,x),TERARK_PP_MAP_8(m,c,__VA_ARGS__)
#define TERARK_PP_MAP_a(m,c,x,...) m(c,x),TERARK_PP_MAP_9(m,c,__VA_ARGS__)
#define TERARK_PP_MAP_b(m,c,x,...) m(c,x),TERARK_PP_MAP_a(m,c,__VA_ARGS__)
#define TERARK_PP_MAP_c(m,c,x,...) m(c,x),TERARK_PP_MAP_b(m,c,__VA_ARGS__)
#define TERARK_PP_MAP_d(m,c,x,...) m(c,x),TERARK_PP_MAP_c(m,c,__VA_ARGS__)
#define TERARK_PP_MAP_e(m,c,x,...) m(c,x),TERARK_PP_MAP_d(m,c,__VA_ARGS__)
#define TERARK_PP_MAP_f(m,c,x,...) m(c,x),TERARK_PP_MAP_e(m,c,__VA_ARGS__)
#define TERARK_PP_MAP_g(m,c,x,...) m(c,x),TERARK_PP_MAP_f(m,c,__VA_ARGS__)
#define TERARK_PP_MAP_h(m,c,x,...) m(c,x),TERARK_PP_MAP_g(m,c,__VA_ARGS__)
#define TERARK_PP_MAP_i(m,c,x,...) m(c,x),TERARK_PP_MAP_h(m,c,__VA_ARGS__)
#define TERARK_PP_MAP_j(m,c,x,...) m(c,x),TERARK_PP_MAP_i(m,c,__VA_ARGS__)
#define TERARK_PP_MAP_k(m,c,x,...) m(c,x),TERARK_PP_MAP_j(m,c,__VA_ARGS__)
#define TERARK_PP_MAP_l(m,c,x,...) m(c,x),TERARK_PP_MAP_k(m,c,__VA_ARGS__)
#define TERARK_PP_MAP_m(m,c,x,...) m(c,x),TERARK_PP_MAP_l(m,c,__VA_ARGS__)
#define TERARK_PP_MAP_n(m,c,x,...) m(c,x),TERARK_PP_MAP_m(m,c,__VA_ARGS__)
#define TERARK_PP_MAP_o(m,c,x,...) m(c,x),TERARK_PP_MAP_n(m,c,__VA_ARGS__)
#define TERARK_PP_MAP_p(m,c,x,...) m(c,x),TERARK_PP_MAP_o(m,c,__VA_ARGS__)
#define TERARK_PP_MAP_q(m,c,x,...) m(c,x),TERARK_PP_MAP_p(m,c,__VA_ARGS__)
#define TERARK_PP_MAP_r(m,c,x,...) m(c,x),TERARK_PP_MAP_q(m,c,__VA_ARGS__)
#define TERARK_PP_MAP_s(m,c,x,...) m(c,x),TERARK_PP_MAP_r(m,c,__VA_ARGS__)
#define TERARK_PP_MAP_t(m,c,x,...) m(c,x),TERARK_PP_MAP_s(m,c,__VA_ARGS__)
#define TERARK_PP_MAP_u(m,c,x,...) m(c,x),TERARK_PP_MAP_t(m,c,__VA_ARGS__)
#define TERARK_PP_MAP_v(m,c,x,...) m(c,x),TERARK_PP_MAP_u(m,c,__VA_ARGS__)
#define TERARK_PP_MAP_w(m,c,x,...) m(c,x),TERARK_PP_MAP_v(m,c,__VA_ARGS__)
#define TERARK_PP_MAP_x(m,c,x,...) m(c,x),TERARK_PP_MAP_w(m,c,__VA_ARGS__)
#define TERARK_PP_MAP_y(m,c,x,...) m(c,x),TERARK_PP_MAP_x(m,c,__VA_ARGS__)
#define TERARK_PP_MAP_z(m,c,x,...) m(c,x),TERARK_PP_MAP_y(m,c,__VA_ARGS__)
#define TERARK_PP_MAP_A(m,c,x,...) m(c,x),TERARK_PP_MAP_z(m,c,__VA_ARGS__)
#define TERARK_PP_MAP_B(m,c,x,...) m(c,x),TERARK_PP_MAP_A(m,c,__VA_ARGS__)
#define TERARK_PP_MAP_C(m,c,x,...) m(c,x),TERARK_PP_MAP_B(m,c,__VA_ARGS__)
#define TERARK_PP_MAP_D(m,c,x,...) m(c,x),TERARK_PP_MAP_C(m,c,__VA_ARGS__)
#define TERARK_PP_MAP_E(m,c,x,...) m(c,x),TERARK_PP_MAP_D(m,c,__VA_ARGS__)
#define TERARK_PP_MAP_F(m,c,x,...) m(c,x),TERARK_PP_MAP_E(m,c,__VA_ARGS__)
#define TERARK_PP_MAP_G(m,c,x,...) m(c,x),TERARK_PP_MAP_F(m,c,__VA_ARGS__)
#define TERARK_PP_MAP_H(m,c,x,...) m(c,x),TERARK_PP_MAP_G(m,c,__VA_ARGS__)
#define TERARK_PP_MAP_I(m,c,x,...) m(c,x),TERARK_PP_MAP_H(m,c,__VA_ARGS__)
#define TERARK_PP_MAP_J(m,c,x,...) m(c,x),TERARK_PP_MAP_I(m,c,__VA_ARGS__)
#define TERARK_PP_MAP_K(m,c,x,...) m(c,x),TERARK_PP_MAP_J(m,c,__VA_ARGS__)
#define TERARK_PP_MAP_L(m,c,x,...) m(c,x),TERARK_PP_MAP_K(m,c,__VA_ARGS__)
#define TERARK_PP_MAP_M(m,c,x,...) m(c,x),TERARK_PP_MAP_L(m,c,__VA_ARGS__)
///@}

/// @param map map function, can be a macro, called as map(ctx,arg)
/// @param ctx context
/// @param ... arg list to apply map function: map(ctx,arg)
/// @returns comma seperated list: map(ctx,arg1), map(ctx,arg2), ...
#define TERARK_PP_MAP(map,ctx,...) TERARK_PP_CAT2 \
       (TERARK_PP_MAP_,TERARK_PP_ARG_N(__VA_ARGS__))(map,ctx,##__VA_ARGS__)

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

/// @param ... arg list to be stringized
#define TERARK_PP_STR(...) TERARK_PP_MAP(TERARK_PP_STR_2,~, __VA_ARGS__)
/// @}

#endif // __terark_preproc_hpp__

