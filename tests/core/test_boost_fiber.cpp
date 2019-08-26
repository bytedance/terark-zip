//
// Created by leipeng on 2019-08-23.
//

#include <boost/fiber/all.hpp>
#include <thread>
#include <stdio.h>
#include <unistd.h> // for usleep

int main() {
    boost::fibers::fiber fb;
    std::thread th([&]() {
        fb = boost::fibers::fiber([]{
             boost::this_fiber::yield();
             usleep(30000);
        });
        boost::this_fiber::yield();
        boost::this_fiber::yield();
    });
    usleep(10000);
    fb.join();
    th.join();
    return 0;
}
