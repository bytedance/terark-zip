#ifndef __terark_preproc_hpp__
#define __terark_preproc_hpp__

#define TERARK_PP_EMPTY

#define TERARK_PP_CAT2_1(a,b)    a##b
#define TERARK_PP_CAT2(a,b)      TERARK_PP_CAT2_1(a,b)
#define TERARK_PP_CAT3(a,b,c)    TERARK_PP_CAT2(TERARK_PP_CAT2(a,b),c)
#define TERARK_PP_CAT4(a,b,c,d)  TERARK_PP_CAT2(TERARK_PP_CAT3(a,b,c),d)

#define TERARK_PP_EXTENT(arr) (sizeof(arr)/sizeof(arr[0]))

#define TERARK_PP_IDENTITY_1(...) __VA_ARGS__
#define TERARK_PP_IDENTITY_2(...) TERARK_PP_IDENTITY_1(__VA_ARGS__)
#define TERARK_PP_IDENTITY(x,...) TERARK_PP_IDENTITY_2(x,##__VA_ARGS__)

#define TERARK_PP_ARG_X(_0,_1,_2,_3,_4,_5,_6,_7,_8,_9, \
           a,b,c,d,e,f,g,h,i,j,k,l,m,n,o,p,q,r,s,t,u,v,w,x,y,z, \
           A,B,C,D,E,F,G,H,I,J,K,L,M,N,O,P,Q,R,S,T,U,V,W,X,Y,Z,XX,...) XX
#define TERARK_PP_ARG_N(...) \
        TERARK_PP_ARG_X("ignored", ##__VA_ARGS__, \
            Z,Y,X,W,V,U,T,S,R,Q,P,O,N,M,L,K,J,I,H,G,F,E,D,C,B,A, \
            z,y,x,w,v,u,t,s,r,q,p,o,n,m,l,k,j,i,h,g,f,e,d,c,b,a, \
                                            9,8,7,6,5,4,3,2,1,0)

///@{
//#define TERARK_PP_CAT_0()       error "TERARK_PP_CAT" have at least 2 params
// allowing TERARK_PP_CAT take just 1 argument
#define TERARK_PP_CAT_0()
#define TERARK_PP_CAT_1_1(x)       x
#define TERARK_PP_CAT_1(x)       TERARK_PP_CAT_1_1(x)
#define TERARK_PP_CAT_2(x,y)     TERARK_PP_CAT2(x,y)
#define TERARK_PP_CAT_3(x,y,...) TERARK_PP_CAT2(x,TERARK_PP_CAT_2(y,__VA_ARGS__))
#define TERARK_PP_CAT_4(x,y,...) TERARK_PP_CAT2(x,TERARK_PP_CAT_3(y,__VA_ARGS__))
#define TERARK_PP_CAT_5(x,y,...) TERARK_PP_CAT2(x,TERARK_PP_CAT_4(y,__VA_ARGS__))
#define TERARK_PP_CAT_6(x,y,...) TERARK_PP_CAT2(x,TERARK_PP_CAT_5(y,__VA_ARGS__))
#define TERARK_PP_CAT_7(x,y,...) TERARK_PP_CAT2(x,TERARK_PP_CAT_6(y,__VA_ARGS__))
#define TERARK_PP_CAT_8(x,y,...) TERARK_PP_CAT2(x,TERARK_PP_CAT_7(y,__VA_ARGS__))
#define TERARK_PP_CAT_9(x,y,...) TERARK_PP_CAT2(x,TERARK_PP_CAT_8(y,__VA_ARGS__))
#define TERARK_PP_CAT_a(x,y,...) TERARK_PP_CAT2(x,TERARK_PP_CAT_9(y,__VA_ARGS__))
#define TERARK_PP_CAT_b(x,y,...) TERARK_PP_CAT2(x,TERARK_PP_CAT_a(y,__VA_ARGS__))
#define TERARK_PP_CAT_c(x,y,...) TERARK_PP_CAT2(x,TERARK_PP_CAT_b(y,__VA_ARGS__))
#define TERARK_PP_CAT_d(x,y,...) TERARK_PP_CAT2(x,TERARK_PP_CAT_c(y,__VA_ARGS__))
#define TERARK_PP_CAT_e(x,y,...) TERARK_PP_CAT2(x,TERARK_PP_CAT_d(y,__VA_ARGS__))
#define TERARK_PP_CAT_f(x,y,...) TERARK_PP_CAT2(x,TERARK_PP_CAT_e(y,__VA_ARGS__))
#define TERARK_PP_CAT_g(x,y,...) TERARK_PP_CAT2(x,TERARK_PP_CAT_f(y,__VA_ARGS__))
#define TERARK_PP_CAT_h(x,y,...) TERARK_PP_CAT2(x,TERARK_PP_CAT_g(y,__VA_ARGS__))
#define TERARK_PP_CAT_i(x,y,...) TERARK_PP_CAT2(x,TERARK_PP_CAT_h(y,__VA_ARGS__))
#define TERARK_PP_CAT_j(x,y,...) TERARK_PP_CAT2(x,TERARK_PP_CAT_i(y,__VA_ARGS__))
#define TERARK_PP_CAT_k(x,y,...) TERARK_PP_CAT2(x,TERARK_PP_CAT_j(y,__VA_ARGS__))
#define TERARK_PP_CAT_l(x,y,...) TERARK_PP_CAT2(x,TERARK_PP_CAT_k(y,__VA_ARGS__))
#define TERARK_PP_CAT_m(x,y,...) TERARK_PP_CAT2(x,TERARK_PP_CAT_l(y,__VA_ARGS__))
#define TERARK_PP_CAT_n(x,y,...) TERARK_PP_CAT2(x,TERARK_PP_CAT_m(y,__VA_ARGS__))
#define TERARK_PP_CAT_o(x,y,...) TERARK_PP_CAT2(x,TERARK_PP_CAT_n(y,__VA_ARGS__))
#define TERARK_PP_CAT_p(x,y,...) TERARK_PP_CAT2(x,TERARK_PP_CAT_o(y,__VA_ARGS__))
#define TERARK_PP_CAT_q(x,y,...) TERARK_PP_CAT2(x,TERARK_PP_CAT_p(y,__VA_ARGS__))
#define TERARK_PP_CAT_r(x,y,...) TERARK_PP_CAT2(x,TERARK_PP_CAT_q(y,__VA_ARGS__))
#define TERARK_PP_CAT_s(x,y,...) TERARK_PP_CAT2(x,TERARK_PP_CAT_r(y,__VA_ARGS__))
#define TERARK_PP_CAT_t(x,y,...) TERARK_PP_CAT2(x,TERARK_PP_CAT_s(y,__VA_ARGS__))
#define TERARK_PP_CAT_u(x,y,...) TERARK_PP_CAT2(x,TERARK_PP_CAT_t(y,__VA_ARGS__))
#define TERARK_PP_CAT_v(x,y,...) TERARK_PP_CAT2(x,TERARK_PP_CAT_u(y,__VA_ARGS__))
#define TERARK_PP_CAT_w(x,y,...) TERARK_PP_CAT2(x,TERARK_PP_CAT_v(y,__VA_ARGS__))
#define TERARK_PP_CAT_x(x,y,...) TERARK_PP_CAT2(x,TERARK_PP_CAT_w(y,__VA_ARGS__))
#define TERARK_PP_CAT_y(x,y,...) TERARK_PP_CAT2(x,TERARK_PP_CAT_x(y,__VA_ARGS__))
#define TERARK_PP_CAT_z(x,y,...) TERARK_PP_CAT2(x,TERARK_PP_CAT_y(y,__VA_ARGS__))
#define TERARK_PP_CAT_A(x,y,...) TERARK_PP_CAT2(x,TERARK_PP_CAT_z(y,__VA_ARGS__))
#define TERARK_PP_CAT_B(x,y,...) TERARK_PP_CAT2(x,TERARK_PP_CAT_A(y,__VA_ARGS__))
#define TERARK_PP_CAT_C(x,y,...) TERARK_PP_CAT2(x,TERARK_PP_CAT_B(y,__VA_ARGS__))
#define TERARK_PP_CAT_D(x,y,...) TERARK_PP_CAT2(x,TERARK_PP_CAT_C(y,__VA_ARGS__))
#define TERARK_PP_CAT_E(x,y,...) TERARK_PP_CAT2(x,TERARK_PP_CAT_D(y,__VA_ARGS__))
#define TERARK_PP_CAT_F(x,y,...) TERARK_PP_CAT2(x,TERARK_PP_CAT_E(y,__VA_ARGS__))
#define TERARK_PP_CAT_G(x,y,...) TERARK_PP_CAT2(x,TERARK_PP_CAT_F(y,__VA_ARGS__))
#define TERARK_PP_CAT_H(x,y,...) TERARK_PP_CAT2(x,TERARK_PP_CAT_G(y,__VA_ARGS__))
#define TERARK_PP_CAT_I(x,y,...) TERARK_PP_CAT2(x,TERARK_PP_CAT_H(y,__VA_ARGS__))
#define TERARK_PP_CAT_J(x,y,...) TERARK_PP_CAT2(x,TERARK_PP_CAT_I(y,__VA_ARGS__))
#define TERARK_PP_CAT_K(x,y,...) TERARK_PP_CAT2(x,TERARK_PP_CAT_J(y,__VA_ARGS__))
#define TERARK_PP_CAT_L(x,y,...) TERARK_PP_CAT2(x,TERARK_PP_CAT_K(y,__VA_ARGS__))
#define TERARK_PP_CAT_M(x,y,...) TERARK_PP_CAT2(x,TERARK_PP_CAT_L(y,__VA_ARGS__))
#define TERARK_PP_CAT_N(x,y,...) TERARK_PP_CAT2(x,TERARK_PP_CAT_M(y,__VA_ARGS__))
#define TERARK_PP_CAT_O(x,y,...) TERARK_PP_CAT2(x,TERARK_PP_CAT_N(y,__VA_ARGS__))
#define TERARK_PP_CAT_P(x,y,...) TERARK_PP_CAT2(x,TERARK_PP_CAT_O(y,__VA_ARGS__))
#define TERARK_PP_CAT_Q(x,y,...) TERARK_PP_CAT2(x,TERARK_PP_CAT_P(y,__VA_ARGS__))
#define TERARK_PP_CAT_R(x,y,...) TERARK_PP_CAT2(x,TERARK_PP_CAT_Q(y,__VA_ARGS__))
#define TERARK_PP_CAT_S(x,y,...) TERARK_PP_CAT2(x,TERARK_PP_CAT_R(y,__VA_ARGS__))
#define TERARK_PP_CAT_T(x,y,...) TERARK_PP_CAT2(x,TERARK_PP_CAT_S(y,__VA_ARGS__))
#define TERARK_PP_CAT_U(x,y,...) TERARK_PP_CAT2(x,TERARK_PP_CAT_T(y,__VA_ARGS__))
#define TERARK_PP_CAT_V(x,y,...) TERARK_PP_CAT2(x,TERARK_PP_CAT_U(y,__VA_ARGS__))
#define TERARK_PP_CAT_W(x,y,...) TERARK_PP_CAT2(x,TERARK_PP_CAT_V(y,__VA_ARGS__))
#define TERARK_PP_CAT_X(x,y,...) TERARK_PP_CAT2(x,TERARK_PP_CAT_W(y,__VA_ARGS__))
#define TERARK_PP_CAT_Y(x,y,...) TERARK_PP_CAT2(x,TERARK_PP_CAT_X(y,__VA_ARGS__))
#define TERARK_PP_CAT_Z(x,y,...) TERARK_PP_CAT2(x,TERARK_PP_CAT_Y(y,__VA_ARGS__))
///@}

///@param x at least one arg x
#define TERARK_PP_CAT(x,...) TERARK_PP_CAT2(x,TERARK_PP_CAT2 \
       (TERARK_PP_CAT_,TERARK_PP_ARG_N(__VA_ARGS__))(__VA_ARGS__))

///@{
///@param m map function
///@param c context
#define TERARK_PP_MAP_0(m,c)
#define TERARK_PP_MAP_1(m,c,x)     m(c,x)
#define TERARK_PP_MAP_2(m,c,x,y)   m(c,x),m(c,y)
#define TERARK_PP_MAP_3(m,c,x,y,z) m(c,x),m(c,y),m(c,z)
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
#define TERARK_PP_MAP_N(m,c,x,...) m(c,x),TERARK_PP_MAP_M(m,c,__VA_ARGS__)
#define TERARK_PP_MAP_O(m,c,x,...) m(c,x),TERARK_PP_MAP_N(m,c,__VA_ARGS__)
#define TERARK_PP_MAP_P(m,c,x,...) m(c,x),TERARK_PP_MAP_O(m,c,__VA_ARGS__)
#define TERARK_PP_MAP_Q(m,c,x,...) m(c,x),TERARK_PP_MAP_P(m,c,__VA_ARGS__)
#define TERARK_PP_MAP_R(m,c,x,...) m(c,x),TERARK_PP_MAP_Q(m,c,__VA_ARGS__)
#define TERARK_PP_MAP_S(m,c,x,...) m(c,x),TERARK_PP_MAP_R(m,c,__VA_ARGS__)
#define TERARK_PP_MAP_T(m,c,x,...) m(c,x),TERARK_PP_MAP_S(m,c,__VA_ARGS__)
#define TERARK_PP_MAP_U(m,c,x,...) m(c,x),TERARK_PP_MAP_T(m,c,__VA_ARGS__)
#define TERARK_PP_MAP_V(m,c,x,...) m(c,x),TERARK_PP_MAP_U(m,c,__VA_ARGS__)
#define TERARK_PP_MAP_W(m,c,x,...) m(c,x),TERARK_PP_MAP_V(m,c,__VA_ARGS__)
#define TERARK_PP_MAP_X(m,c,x,...) m(c,x),TERARK_PP_MAP_W(m,c,__VA_ARGS__)
#define TERARK_PP_MAP_Y(m,c,x,...) m(c,x),TERARK_PP_MAP_X(m,c,__VA_ARGS__)
#define TERARK_PP_MAP_Z(m,c,x,...) m(c,x),TERARK_PP_MAP_Y(m,c,__VA_ARGS__)
///@}

/// @param map map function, can be a macro, called as map(ctx,arg)
/// @param ctx context
/// @param ... arg list to apply map function: map(ctx,arg)
/// @returns comma seperated list: map(ctx,arg1), map(ctx,arg2), ...
/// @note at least zero args
#define TERARK_PP_MAP(map,ctx,...) TERARK_PP_CAT2 \
       (TERARK_PP_MAP_,TERARK_PP_ARG_N(__VA_ARGS__))(map,ctx,##__VA_ARGS__)

/// @param dummy unused param 'context'
#define TERARK_PP_IDENTITY_MAP_OP(dummy, x) x

/// @param prefix is param 'c'(context) in TERARK_PP_MAP
#define TERARK_PP_PREPEND(prefix, x) prefix x

/// @{ TERARK_PP_STR is a use case of TERARK_PP_MAP
/// macro TERARK_PP_STR_2 is the 'map' function
/// context of TERARK_PP_STR_2 is dummy
///
/// TERARK_PP_STR(a)     will produce: "a"
/// TERARK_PP_STR(a,b,c) will produce: "a", "b", "c"
/// so TERARK_PP_STR is a generic stringize macro
#define TERARK_PP_STR_1(c,x) #x
#define TERARK_PP_STR_2(c,x) TERARK_PP_STR_1(c,x)

/// @note context for calling TERARK_PP_MAP is dummy(noted as '~')
/// @param ... arg list to be stringized
#define TERARK_PP_STR(...) TERARK_PP_MAP(TERARK_PP_STR_2,~, __VA_ARGS__)
/// @}

#endif // __terark_preproc_hpp__

