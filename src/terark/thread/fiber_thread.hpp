//
// Created by leipeng on 2019-08-16.
//

#pragma once

#include <terark/util/function.hpp>
#include <thread>

namespace terark {

    class TERARK_DLL_EXPORT FiberThread {
        class Impl;
        Impl* m_impl;
    public:
        explicit
        FiberThread(function<void()>, size_t num_fibers = 16);

        template<class Fn, class... Arg>
        FiberThread(size_t num_fibers, Fn&& fn, Arg... arg)
          : FiberThread(bind(fn, arg...), num_fibers) {}

        ~FiberThread();
        bool joinable() noexcept { return nullptr != m_impl; }
        void join();
        void dettach() noexcept;
        std::thread::id get_id() const noexcept;
        void swap(FiberThread& y) noexcept { std::swap(m_impl, y.m_impl); }
    };
}

namespace std {
  void swap(terark::FiberThread&x,terark::FiberThread&y)noexcept{x.swap(y);}
}
