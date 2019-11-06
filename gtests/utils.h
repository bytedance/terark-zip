#pragma once

#define TIME_START(VAR_) \
        VAR_ = std::chrono::high_resolution_clock::now()
#define TIME_END(VAR_)  \
        VAR_ = std::chrono::high_resolution_clock::now()

#define PRINT_TIME(START, END) \
        auto duration__ = END - START; \
        auto ms__ = std::chrono::duration_cast<std::chrono::milliseconds>(duration__) \
        .count(); \
        std::cout << name " took " << ms__ << " ms" << std::endl
