//
// Created by leipeng on 2019-08-28.
//

#include <boost/fiber/fiber.hpp>
#include <boost/fiber/fss.hpp>
#include <thread>

void clean_fss_int(int* p) {
    printf("destroy int = %d\n", *p);
}
static boost::fibers::fiber_specific_ptr<int> fs0(clean_fss_int);
static thread_local boost::fibers::fiber_specific_ptr<int> fs1(clean_fss_int);

int main(int argc, char* argv[]) {
    boost::fibers::fiber_specific_ptr<int> fs2(clean_fss_int);
    auto func = [&](const char* name) {
        printf("---- %s ----\n", name);
        boost::fibers::fiber_specific_ptr<int> fs3(clean_fss_int);
        if (fs0.get() == NULL) {
            fs0.reset(new int(0));
        }
        if (fs1.get() == NULL) {
            fs1.reset(new int(1));
        }
        if (fs2.get() == NULL) {
            fs2.reset(new int(2));
        }
        if (fs3.get() == NULL) {
            fs3.reset(new int(3));
        }
    };
    std::thread thr(func, "thread");
    thr.join();
    boost::fibers::fiber fb(func, "fiber");
    fb.join();
    return 0;
}
