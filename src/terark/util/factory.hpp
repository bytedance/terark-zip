// created by leipeng 2019-12-13

#pragma once
#include "function.hpp"
#include <terark/fstring.hpp>
#include <terark/preproc.hpp>

namespace terark {

///@note on principle, the factory itself is stateless, but its product
/// can has states, sometimes we need factory of factory, in this case,
/// just let the factory being factoryable:
/// class SomeFactory : public Factoyable<SomeFactory> {
///    ...
/// };
template<class ProductPtr, class... CreatorArgs>
class Factoryable {
public:
    virtual ~Factoryable();
    static ProductPtr create(fstring name, CreatorArgs...);
    struct AutoReg {
        AutoReg(fstring name, function<ProductPtr(CreatorArgs...)> creator);
        struct Impl;
    };
};

///@param VarID var identifier
///@param Name string of factory name
///@param Creator
///@param Class ... can be qulified template such as:
///                     SomeNameSpace::SomeProductPtr<T1, T2, T3>
#define TERARK_FACTORY_REGISTER_IMPL(VarID, Name, Creator, Class, ...) \
  TERARK_PP_IDENTITY(Class,##__VA_ARGS__)::AutoReg \
    TERARK_PP_CAT(g_reg_factory_, VarID, __LINE__)(Name, Creator)

///@param Class can not be template such as SomeProductPtr<T1, T2, T3>,
///             can not be qulified class name such as SomeNameSpace::SomeClass
///@note if Class has some non-var char, such as "-,." ...
///         must use TERARK_FACTORY_REGISTER_IMPL to set an VarID
#define TERARK_FACTORY_REGISTER_EX(Class, Name, Creator) \
        TERARK_FACTORY_REGISTER_IMPL(Class, Name, Creator, Class)

#define TERARK_FACTORY_REGISTER(Class, Creator) \
        TERARK_FACTORY_REGISTER_EX(Class, TERARK_PP_STR(Class), Creator)

} // namespace terark
