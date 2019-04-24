#ifndef TERARK_FSA_DFA_ALGO_BASIC_HPP_
#define TERARK_FSA_DFA_ALGO_BASIC_HPP_
#pragma once

#include <terark/config.hpp>
#include <terark/valvec.hpp>

namespace terark {

class TERARK_DLL_EXPORT NonRecursiveDictionaryOrderToStateMapGenerator {
    valvec<size_t> m_stack;
    ///@param on_map function on_map(dictOrderNth, stateId)
    template<class DFA, class OnMap>
    size_t gen(const DFA& dfa, size_t root, OnMap on_map) {
        assert(root < dfa.total_states());
        assert(m_stack.empty());
        size_t nth = 0, sigma = dfa.get_sigma();
        m_stack.reserve(sigma * 2);
        m_stack.push_back(root);
        while (!m_stack.empty()) {
            size_t state = m_stack.pop_val();
            assert(state < dfa.total_states());
            if (dfa.is_term(state)) {
                on_map(nth, state);
                nth++;
            }
            size_t oldsize = m_stack.size();
            auto   ptrdest = m_stack.grow_no_init(sigma);
            size_t numdest = dfa.get_all_dest(state, ptrdest);
            m_stack.risk_set_size(oldsize + numdest);
            std::reverse(ptrdest, ptrdest + numdest);
        }
        return nth;
    }
public:
    template<class DFA, class OnMap>
    size_t operator()(const DFA& dfa, OnMap* on_map) {
        return gen<DFA, OnMap&>(dfa, initial_state, *on_map);
    }
    template<class DFA, class OnMap>
    size_t operator()(const DFA& dfa, OnMap on_map) {
        return gen<DFA, OnMap>(dfa, initial_state, on_map);
    }
    template<class DFA, class OnMap>
    size_t operator()(const DFA& dfa, size_t root, OnMap* on_map) {
        return gen<DFA, OnMap&>(dfa, root, *on_map);
    }
    template<class DFA, class OnMap>
    size_t operator()(const DFA& dfa, size_t root, OnMap on_map) {
        return gen<DFA, OnMap>(dfa, root, on_map);
    }
};

} // namespace terark

#endif /* TERARK_FSA_DFA_ALGO_BASIC_HPP_ */
