#ifndef __terark_preproc_hpp__
#define __terark_preproc_hpp__

#include <boost/preprocessor/cat.hpp>

#define TERARK_PP_CAT2(a,b)      BOOST_PP_CAT(a,b)
#define TERARK_PP_CAT3(a,b,c)    BOOST_PP_CAT(BOOST_PP_CAT(a,b),c)
#define TERARK_PP_CAT4(a,b,c,d)  BOOST_PP_CAT(TERARK_PP_CAT3(a,b,c),d)

#endif // __terark_preproc_hpp__

