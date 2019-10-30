//
// Created by leipeng on 2019-10-28.
//

#include <boost/fiber/fiber.hpp>
#include <boost/fiber/scheduler.hpp>
#include <boost/fiber/operations.hpp>

int main() {
    using namespace boost::fibers;
    auto fn = []() {
        scheduler* sched = context::active()->get_scheduler();
        auto largeFn = []() {
            char buf[256 * 1024] = {0};
            sprintf(buf, "fn: large stack");
            //printf("%s\n", buf);
        };
        for (size_t i = 0; i < 1024*1024; ++i) {
            sched->call_on_main_stack(largeFn);
        }
    };
    fiber f1(fn);
    boost::this_fiber::yield();
    f1.join();
    printf("call_on_main_stack passed\n");
    return 0;
}
