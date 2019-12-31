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

#define TERARK_COMPARATOR_OP(Name, expr) \
    struct Name { \
        template<class T> \
        bool operator()(const T& x, const T& y) const { return expr; } \
    }
TERARK_COMPARATOR_OP(CmpLT,   x < y );
TERARK_COMPARATOR_OP(CmpGT,   y < x );
TERARK_COMPARATOR_OP(CmpLE, !(y < x));
TERARK_COMPARATOR_OP(CmpGE, !(x < y));
TERARK_COMPARATOR_OP(CmpEQ,   x ==y );
TERARK_COMPARATOR_OP(CmpNE, !(x ==y));

struct cmp_placeholder{};
static cmp_placeholder cmp;

///{@
///@arg x is the free  arg
///@arg y is the bound arg
#define TERARK_BINDER_CMP_OP(BinderName, expr) \
    template<class T> \
    struct BinderName {  const T  y; \
        BinderName(const T& y1) : y(y1) {} \
        BinderName(      T&&y1) : y(std::move(y1)) {} \
        bool operator()(const T& x) const { return expr; } \
    }; \
    template<class T> \
    struct BinderName<T*> {  const T* y; \
        BinderName(const T* y1) : y(y1) {} \
        bool operator()(const T* x) const { return expr; } \
    }; \
    template<class T> \
    struct BinderName<std::reference_wrapper<const T> > { \
        const T& y; \
        BinderName(const T& y1) : y(y1) {} \
        bool operator()(const T& x) const { return expr; } \
    }
TERARK_BINDER_CMP_OP(BinderLT,   x < y );
TERARK_BINDER_CMP_OP(BinderGT,   y < x );
TERARK_BINDER_CMP_OP(BinderLE, !(y < x));
TERARK_BINDER_CMP_OP(BinderGE, !(x < y));
TERARK_BINDER_CMP_OP(BinderEQ,   x ==y );
TERARK_BINDER_CMP_OP(BinderNE, !(x ==y));
///@}

template<class KeyExtractor>
struct ExtractorLessT {
	KeyExtractor ex;
	template<class T>
	bool operator()(const T& x, const T& y) const { return ex(x) < ex(y); }
};
template<class KeyExtractor>
ExtractorLessT<KeyExtractor>
ExtractorLess(KeyExtractor ex) { return ExtractorLessT<KeyExtractor>{ex}; }

template<class KeyExtractor>
struct ExtractorGreaterT {
	KeyExtractor ex;
	template<class T>
	bool operator()(const T& x, const T& y) const { return ex(y) < ex(x); }
};
template<class KeyExtractor>
ExtractorGreaterT<KeyExtractor>
ExtractorGreater(KeyExtractor ex) { return ExtractorGreaterT<KeyExtractor>{ex}; }

template<class KeyExtractor>
struct ExtractorEqualT {
	KeyExtractor ex;
	template<class T>
	bool operator()(const T& x, const T& y) const { return ex(x) == ex(y); }
};
template<class KeyExtractor>
ExtractorEqualT<KeyExtractor>
ExtractorEqual(KeyExtractor ex) { return ExtractorEqualT<KeyExtractor>{ex}; }

template<class KeyExtractor, class KeyComparator>
struct ExtractorComparatorT {
	template<class T>
	bool operator()(const T& x, const T& y) const {
		return keyCmp(keyEx(x), keyEx(y));
	}
	KeyExtractor  keyEx;
	KeyComparator keyCmp;
};
template<class KeyExtractor, class Comparator>
ExtractorComparatorT<KeyExtractor, Comparator>
ExtractorComparator(KeyExtractor ex, Comparator cmp) {
	return ExtractorComparatorT<KeyExtractor, Comparator>{ex, cmp};
}

template<class Extractor1, class Extractor2>
struct CombineExtractor {
    Extractor1 ex1;
    Extractor2 ex2;
    template<class T>
    auto operator()(const T& x) const -> decltype(ex2(ex1(x))) {
        return ex2(ex1(x));
    }
};
template<class Extractor1>
struct CombinableExtractorT {
    Extractor1 ex1;

    ///@{
    /// operator+ as combine operator
    template<class Extractor2>
    CombineExtractor<Extractor1, Extractor2>
    operator+(const Extractor2& ex2) const {
        return CombineExtractor<Extractor1, Extractor2>{ex1, ex2};
    }
    template<class Extractor2>
    CombineExtractor<Extractor1, Extractor2>
    operator+(Extractor2&& ex2) const {
        return CombineExtractor<Extractor1, Extractor2>{ex1, std::move(ex2)};
    }
    ///@}

    ///@{
    /// operator| combine a comparator: less, greator, equal...
    template<class Comparator>
    ExtractorComparatorT<Extractor1, Comparator>
    operator|(const Comparator& cmp) const {
        return ExtractorComparator(ex1, cmp);
    }
    template<class Comparator>
    ExtractorComparatorT<Extractor1, Comparator>
    operator|(Comparator&& cmp) const {
        return ExtractorComparator(ex1, std::move(cmp));
    }
    ///@}

#define TERARK_CMP_OP(Name, op) \
    ExtractorComparatorT<Extractor1, Name> \
    operator op(cmp_placeholder) const { \
        return ExtractorComparator(ex1, Name()); \
    }

    TERARK_CMP_OP(CmpLT, < )
    TERARK_CMP_OP(CmpGT, > )
    TERARK_CMP_OP(CmpLE, <=)
    TERARK_CMP_OP(CmpGE, >=)
    TERARK_CMP_OP(CmpEQ, ==)
    TERARK_CMP_OP(CmpNE, !=)


#define TERARK_COMBINE_BIND_OP(Name, op) \
    template<class T> \
    CombineExtractor<Extractor1, Name<T> > operator op(const T& y) const { \
        return \
    CombineExtractor<Extractor1, Name<T> >{ex1, {y}}; } \
    template<class T> \
    CombineExtractor<Extractor1, Name<T> > operator op(T&& y) const { \
        return \
    CombineExtractor<Extractor1, Name<T> >{ex1, {std::move(y)}}; } \
    template<class T> \
    CombineExtractor<Extractor1, Name<std::reference_wrapper<const T> > > \
    operator op(std::reference_wrapper<const T> y) const { \
        return \
    CombineExtractor<Extractor1, Name<std::reference_wrapper<const T> > > \
        {ex1, {y.get()}}; }

    ///@{
    /// operators: <, >, <=, >=, ==, !=
    /// use bound operator as Extractor, Extractor is a transformer in this case
    TERARK_COMBINE_BIND_OP(BinderLT, < )
    TERARK_COMBINE_BIND_OP(BinderGT, > )
    TERARK_COMBINE_BIND_OP(BinderLE, <=)
    TERARK_COMBINE_BIND_OP(BinderGE, >=)
    TERARK_COMBINE_BIND_OP(BinderEQ, ==)
    TERARK_COMBINE_BIND_OP(BinderNE, !=)
    ///@}



    /// forward the extractor
    template<class T>
    auto operator()(const T& x) const -> decltype(ex1(x)) { return ex1(x); }
};
template<class Extractor1>
CombinableExtractorT<Extractor1>
CombinableExtractor(const Extractor1& ex1) {
    return CombinableExtractorT<Extractor1>{ex1};
}
template<class Extractor1>
CombinableExtractorT<Extractor1>
CombinableExtractor(Extractor1&& ex1) {
    return CombinableExtractorT<Extractor1>{std::move(ex1)};
}


///@param __VA_ARGS__ can be 'template some_member_func<1,2,3>()'
#define TERARK_GET(...) terark::CombinableExtractor([](const auto& x) { return x __VA_ARGS__; })
#define TERARK_FIELD(...) [](const auto& x) { return x __VA_ARGS__; }

///@param op '<' or '>'
#define TERARK_CMP_1(op,f) [](const auto& x, const auto& y) { return x f op y f; }
#define TERARK_CMP_2(op,f1,f2) [](const auto& x, const auto& y) { \
    if (x f1 op y f1) return true; \
    else if (y f1 op x f1) return false; \
    return x f2 op y f2; }
#define TERARK_CMP_3(op,f1,f2,f3) [](const auto& x, const auto& y) { \
    if (x f1 op y f1) return true; \
    else if (y f1 op x f1) return false; \
    else if (x f2 op y f2) return true; \
    else if (y f2 op x f2) return false; \
    return x f3 op y f3; }
#define TERARK_CMP_4(op,f1,f2,f3,f4) [](const auto& x, const auto& y) { \
    if (x f1 op y f1) return true; \
    else if (y f1 op x f1) return false; \
    else if (x f2 op y f2) return true;  \
    else if (y f2 op x f2) return false; \
    else if (x f3 op y f3) return true;  \
    else if (y f3 op x f3) return false; \
    return x f4 op y f4; }

///@param __VA_ARGS__ at least 1 field
///@note max support 4 fields
#define TERARK_CMP(op, ...) \
  TERARK_PP_CAT2(TERARK_CMP_,TERARK_PP_ARG_N(__VA_ARGS__))(op,__VA_ARGS__)

#define TERARK_CMP_EX_2(f1,o1) [](const auto& x, const auto& y) { return x f1 o1 y f1; }
#define TERARK_CMP_EX_4(f1,o1,f2,o2) [](const auto& x, const auto& y) { \
    if (x f1 o1 y f1) return true; \
    else if (y f1 o1 x f1) return false; \
    return x f2 o2 y f2; }
#define TERARK_CMP_EX_6(f1,o1,f2,o2,f3,o3) [](const auto& x, const auto& y) { \
    if (x f1 o1 y f1) return true; \
    else if (y f1 o1 x f1) return false; \
    else if (x f2 o2 y f2) return true; \
    else if (y f2 o2 x f2) return false; \
    return x f3 o3 y f3; }
#define TERARK_CMP_EX_8(f1,o1,f2,o2,f3,o3,f4,o4) [](const auto& x, const auto& y) { \
    if (x f1 o1 y f1) return true; \
    else if (y f1 o1 x f1) return false; \
    else if (x f2 o2 y f2) return true;  \
    else if (y f2 o2 x f2) return false; \
    else if (x f3 o3 y f3) return true;  \
    else if (y f3 o3 x f3) return false; \
    return x f4 o4 y f4; }

///@param __VA_ARGS__ at least 1 field
///@note max support 4 fields, sample usage: TERARK_CMP_EX(f1,>,f2,<,f3,<)
#define TERARK_CMP_EX(...) \
  TERARK_PP_CAT2(TERARK_CMP_EX_,TERARK_PP_ARG_N(__VA_ARGS__))(__VA_ARGS__)

} // namespace terark

using terark::cmp;
