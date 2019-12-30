/* vim: set tabstop=4 : */
#pragma once

#ifdef TERARK_FUNCTION_USE_BOOST
	#include <boost/function.hpp>
#else
	#include <functional>
#endif

#include <terark/config.hpp>
#include <terark/preproc.hpp>
#include <boost/noncopyable.hpp>

namespace terark {

#ifdef TERARK_FUNCTION_USE_BOOST
    using boost::bind;
    using boost::function;
	using boost::ref;
	using boost::cref;
	using boost::reference_wrapper
#else
    using std::bind;
	using std::function;
	using std::ref;
	using std::cref;
	using std::reference_wrapper;
#endif

	template<class FuncProto>
	class tfunc : public function<FuncProto> {
		typedef function<FuncProto> super;
	public:
		using super::super;
		template<class Functor>
		tfunc(const Functor* f) : super(ref(*f)) {}
	};

    template<class Functor, class... ArgList>
    auto bind(Functor* f, ArgList&&... args)
    ->decltype(bind(ref(*f), std::forward<ArgList>(args)...)) {
        return bind(ref(*f), std::forward<ArgList>(args)...);
    }

	template<class Func>
	class OnScopeExit {
		const Func& on_exit;
	public:
		OnScopeExit(const Func& f) : on_exit(f) {}
		~OnScopeExit() { on_exit(); }
	};
#define TERARK_SCOPE_EXIT(...) \
    auto BOOST_PP_CAT(func_on_exit_,__LINE__) = [&]() { __VA_ARGS__; }; \
    terark::OnScopeExit<decltype(BOOST_PP_CAT(func_on_exit_,__LINE__))> \
        BOOST_PP_CAT(call_on_exit_,__LINE__)(BOOST_PP_CAT(func_on_exit_,__LINE__))

    template<class R, class... Args>
    using c_callback_fun_t = R (*)(void*, Args...);

    template<class Lambda>
    struct c_callback_t {
        template<class R, class... Args>
        static R invoke(void* vlamb, Args... args) {
            return (*(Lambda*)vlamb)(std::forward<Args>(args)...);
        }

        template<class R, class... Args>
        operator c_callback_fun_t<R, Args...>() const {
            return &c_callback_t::invoke<R, Args...>;
        }
    };
    template<class Lambda>
    c_callback_t<Lambda> c_callback(Lambda&) {
       return c_callback_t<Lambda>();
    }

    template<class MemFuncType, MemFuncType MemFunc>
    struct mf_callback_t {
        template<class R, class Obj, class... Args>
        static R invoke(void* self, Args... args) {
            return (((Obj*)self)->*MemFunc)(std::forward<Args>(args)...);
        }
        template<class R, class... Args>
        operator c_callback_fun_t<R, Args...>() const {
            return &mf_callback_t::invoke<R, Args...>;
        }
    };

// do not need to pack <This, MemFunc> as a struct
#define TERARK_MEM_FUNC(MemFunc)  \
    mf_callback_t<decltype(MemFunc), MemFunc>()

//--------------------------------------------------------------------
// User/Application defined MemPool
class TERARK_DLL_EXPORT UserMemPool : boost::noncopyable {
    UserMemPool();
public:
    virtual ~UserMemPool();
    virtual void* alloc(size_t);
    virtual void* realloc(void*, size_t);
    virtual void  sfree(void*, size_t);
    static UserMemPool* SysMemPool();
};

///@param __VA_ARGS__ can be 'template some_member_func<1,2,3>()'
#define TERARK_FIELD(...) [](const auto& x) { return x.__VA_ARGS__; }
#define TERARK_FIELD_P(...) [](const auto& x) { return x->__VA_ARGS__; }

///@param d '.' or '->'
#define TERARK_CMP_FIELD_1(d,f) [](const auto& x, const auto& y) { return x d f < y d f; }
#define TERARK_CMP_FIELD_2(d,f1,f2) [](const auto& x, const auto& y) { \
    if (x d f1 < y d f1) return true; \
    else if (y d f1 < x d f1) return false; \
    return x d f2 < y d f2; }
#define TERARK_CMP_FIELD_3(d,f1,f2,f3) [](const auto& x, const auto& y) { \
    if (x d f1 < y d f1) return true; \
    else if (y d f1 < x d f1) return false; \
    else if (x d f2 < y d f2) return true; \
    else if (y d f2 < x d f2) return false; \
    return x d f3 < y d f3; }
#define TERARK_CMP_FIELD_4(d,f1,f2,f3,f4) [](const auto& x, const auto& y) { \
    if (x d f1 < y d f1) return true; \
    else if (y d f1 < x d f1) return false; \
    else if (x d f2 < y d f2) return true;  \
    else if (y d f2 < x d f2) return false; \
    else if (x d f3 < y d f3) return true;  \
    else if (y d f3 < x d f3) return false; \
    return x d f4 < y d f4; }

///@param __VA_ARGS__ at least 1 field
///@note max support 4 fields
#define TERARK_CMP_FIELD(...) \
  TERARK_PP_CAT2(TERARK_CMP_FIELD_,TERARK_PP_ARG_N(__VA_ARGS__))(.,__VA_ARGS__)

#define TERARK_CMP_FIELD_P(...) \
  TERARK_PP_CAT2(TERARK_CMP_FIELD_,TERARK_PP_ARG_N(__VA_ARGS__))(->,__VA_ARGS__)

} // namespace terark
