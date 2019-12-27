/* vim: set tabstop=4 : */
#pragma once

#ifdef TERARK_FUNCTION_USE_BOOST
	#include <boost/function.hpp>
#else
	#include <functional>
#endif

#include <terark/config.hpp>
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

} // namespace terark
