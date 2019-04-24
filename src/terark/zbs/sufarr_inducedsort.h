#ifndef _SAIS_H
#define _SAIS_H 1

/**
 * 1.基本介绍:
 *   SAIS算法是一种基于诱导排序(induced sort)的后缀数组创建算法(SACA)。
 *   这个集成实现是基于libdivsufsort和{Ge Nong,Sen Zhang,Wai Hong Chan}于2009年发表的论文
 *   "Linear Suffix Array Construction by Almost Pure Induced Sorting"编写而成。
 *   其核心原理是使用{桶排，诱导排序}的高效来提高后缀数组创建的效率，
 *   而{后缀字符串集}特殊的性质允许{桶排，诱导排序}在这种特殊情况的使用。
 *   同时在实现中可以通过标记法有效的控制临时内存开销。
 *
 * 2.实际情况:
 *   单独测试时SAIS未能大幅领先divsufsort算法，单线程时稍稍领先(2GB data)，
 *   多线程资源争抢严重时测试结果十分混乱，相互矛盾。
 *   奇怪的是在实际工况表现过于良好。(SAIS集成于系统中运行时45%快于divsufsort)
 *
 * 3.原因分析:
 *   在测试中，造成其实际性能低下的原因除去本身算法常数偏大之外，更核心的原因是代码逻辑混乱粗糙，
 *   沿用了divsufsort的定义，却修改了divsufsort的设计逻辑。造成了现版本拙劣的常数。
 *   事实上应明显快于divsufsort，单线程应33%快于divsufsort(2GB data)。
 *   虽然此版本是我意欲测试大数据状态与真实工况数据状态的版本，性能还未达标，
 *   但我预估SAIS应16%快于divsufsort(2GB data)，而事实并不符合我的预估。
 *   此版本的SAIS仍可大幅优化，或应重构，以降低时间常数，提高性能。(个人意愿上我更愿意重构此部件)
 *   基层代码不应为了提高易读性，而刻意降低代码耦合程度，间接减低了程序效能。
 *   这种行为十分愚蠢。如果重新修订，应至少可以提升15%的性能。
 *
 *   而实际工况中的变量过多，虽然分离变量调试，也未能找出完整合理的解释。
 *
 * 4.维护更新:
 *   如果有人希望维护这个部件，应阅读上文中的论文，工程中的变量名与论文中一致或更加清晰。
 *   如果有人希望更新这个部件，请查找阅读最新最快的后缀数组创建算法(SACA)，尝试更新替换。
 *
 * LevisonChen 21:00 Dec.1.2017 @Terark Inc. Beijing. China.
 * LevisonChen 10:24 Dec.6.2017 @Terark Inc. Beijing. China.
 *
 */

#ifdef __cplusplus
extern "C" {
#endif

int sufarr_inducedsort(const unsigned char *source, int *suf_arr, int size);

int sufarr_inducedsort_int(const int *source, int *suf_arr, int size, int sigma);

int sufarr_inducedsort_bwt(const unsigned char *source, unsigned char *height, int *rank, int size);

int sufarr_inducedsort_int_bwt(const int *source, int *height, int *rank, int size, int sigma);

#ifdef __cplusplus
}
#endif

#endif