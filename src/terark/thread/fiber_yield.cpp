//
// Created by leipeng on 2019-08-26.
//

#include "fiber_yield.hpp"

namespace terark {

    void FiberYield::yield_slow() {
        init_in_fiber_thread();
        m_sched->yield(*m_active_context_pp);
    }

}
