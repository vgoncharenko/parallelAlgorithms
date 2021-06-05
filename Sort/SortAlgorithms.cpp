//
//  SortAlgorithms.cpp
//  parallelAlgs
//
//  Created by Vitaliy on 31.05.2021.
//
#ifndef SortAlgorithms_hpp
#define SortAlgorithms_hpp

#include "QuickSort.cpp"
#include "BitonicSort.cpp"
#include "../measure.cpp"

//auto getVector(const uint64_t n) {
//    auto vector = new float [n];
//
//    for (int i = 0; i < n; ++i) {
//        vector[i] = random() / MAXFLOAT;
//    }
//
//    return vector;
//}

auto getVector(const uint64_t n) {
    auto vector = std::vector<float>(n);

    for (int i = 0; i < n; ++i) {
        vector[i] = random() / MAXFLOAT;
    }

    return vector;
}

template<typename T>
void verifyVectors(const float* expected, const T& actual, const uint64_t n) {
    for (int i = 0; i < n; ++i) {
        if (expected[i] != actual[i])
            std::cout << "error in index " << i << " expected=" << expected[i] << " actual=" << actual[i] << " diff=" << (expected[i]-actual[i]) << "\n";
    }
}

template<typename T>
void verifyVectors(const T& expected, const T& actual, const uint64_t n) {
    for (int i = 0; i < n; ++i) {
        if (expected[i] != actual[i])
            std::cout << "error in index " << i << " expected=" << expected[i] << " actual=" << actual[i] << " diff=" << (expected[i]-actual[i]) << "\n";
    }
}

template<typename I>
void testBubbleSerial(I vInBegin, const uint64_t n){
    for (int i = 0; i < n; ++i) {
        bool swappedFlag = false;
        for (auto it = vInBegin; it < vInBegin + n - i - 1; ++it) {
            auto next = it + 1;
            compareExchange(it, next, asc, std::ref(swappedFlag));
        }
        if (!swappedFlag) break;
    }
}

/**
 * O(p*(n/p)*log(n/p)) = O(n*log(n/p))
 */
template<typename I>
void testBubbleOddEvenTranspositionParallel(I vInBegin, const uint64_t n, const uint8_t threadCount){
    uint64_t batchSize = n/threadCount/2;
    uint64_t doubleBatchSize = batchSize*2;

    for (int i = 1; i <= threadCount; ++i) {
        std::vector<std::thread> sortStep;
        auto begin = (i%2) ? vInBegin : vInBegin + batchSize;
        for (auto it = begin; it <= begin + (n - doubleBatchSize - ((1-(i%2))*batchSize)); it+=doubleBatchSize) {
            std::thread t{[it, doubleBatchSize] {
                std::sort(it, it + doubleBatchSize);
            }};
            sortStep.push_back(move(t));
        }

        for_each(sortStep.begin(), sortStep.end(), [](std::thread &t) {
            t.join();
        });
    }
}

void sanityCheck()
{
//    std::vector<int> input = {3,5,8,9,10,12,14,20,95,90,60,40,35,23,18,0};
//    std::vector<float> input = {10,20,5,9,3,8,12,14,90,0,60,40,23,35,95,18};
    std::vector<float> input = {223,108,1010,117,18,101,111,115,116,103,105,113,114,10,20,5,9,3,8,12,14,90,0,60,40,23,35,95,106,112,118,119};
//    std::vector<float> input = {10,8,4,1,7,2,11,3,6};
//    std::vector<float> input = {7,13,18,2,17,1,14,20,6,10,15,9,3,16,19,4,11,12,5,8};
    std::vector<float> expected(input.size());
    std::copy(input.begin(), input.end(), expected.begin());
    std::sort(expected.begin(), expected.end());
    testQuickSortParallel(input.begin(), input.size(), 5);
    verifyVectors(expected, input, input.size());
}

void testVectorSort() {
//    sanityCheck();return;
    uint8_t iterations = 1;
    uint8_t threadsCount = 16;
    auto W = new uint64_t[iterations];
//    W[0] = 1024;
    //W[0] = 8589934592;
//    W[0] = 2147483648;
    W[0] = 1073741824;
//    W[0] = 67108864;
//    Fastest sequential
//    6.10807,
//    Quick parallel with more threads because of ceil:
//    1.38042,
//    Quick parallel with less threads because of round:
//    1.2775,

    uint8_t q = 2;
    for (int i = 1; i < iterations; ++i) {
        W[i] = W[i-1] * q;
    }

    std::vector<std::vector<float>> results(10, std::vector<float>());
    std::vector<std::string> labels(10);
    for (int i = 0; i < iterations; ++i) {
        //float vIn[] = {10,20,5,9,3,8,12,14,90,0,60,40,23,35,95,18,101,111,115,116,103,105,113,114,106,223,108,1010,117,112,118,119};
        int idx=0;
        auto vIn = getVector(W[i]);
#ifdef MY_TEST
        auto expected = std::vector<float>(vIn);
        std::sort(expected.begin(), expected.end());
#endif

//        labels[idx] = "Serial\n";
//        auto vOutSerial = new float [W[i]];
//        std::copy(vIn, vIn + W[i], vOutSerial);
//        measure([&W, &vOutSerial, i] {
//            testBitonicSortSerial(vOutSerial, W[i]);
//        }, results[idx++], 10, "seconds", "to_var");
//        verifyVectors(expected, vOutSerial, W[i]);
//        delete [] vOutSerial;

//        labels[idx] = "Bitonic Sort Parallel\n";
//        auto vOutBitonicParallel = new float [W[i]];
//        std::copy(vIn, vIn + W[i], vOutBitonicParallel);
//        measure([&W, &vOutBitonicParallel, threadsCount, i] {
//            testBitonicSortParallel(vOutBitonicParallel, W[i], threadsCount);
//        }, results[idx++], 10, "seconds", "to_var");
//        verifyVectors(expected, vOutBitonicParallel, W[i]);
//        delete [] vOutBitonicParallel;

//        labels[idx] = "Bitonic Sort Parallel Compare-Split Internal Buffer\n";
//        auto vOutBitonicCompareSplitWithInteralBufferParallel = new float [W[i]];
//        std::copy(vIn, vIn + W[i], vOutBitonicCompareSplitWithInteralBufferParallel);
//        measure([&W, &vOutBitonicCompareSplitWithInteralBufferParallel, threadsCount, i] {
//            testBitonicSortCompareSplitWithInternalBufferParallel(vOutBitonicCompareSplitWithInteralBufferParallel, W[i], threadsCount);
//        }, results[idx++], 10, "seconds", "to_var");
//        verifyVectors(expected, vOutBitonicCompareSplitWithInteralBufferParallel, W[i]);
//        delete [] vOutBitonicCompareSplitWithInteralBufferParallel;
//
//        labels[idx] = "Bitonic Sort Parallel Compare-Split With Copy\n";
//        auto vOutBitonicCompareSplitWithCopyParallel = new float [W[i]];
//        std::copy(vIn, vIn + W[i], vOutBitonicCompareSplitWithCopyParallel);
//        measure([&W, &vOutBitonicCompareSplitWithCopyParallel, threadsCount, i] {
//            testBitonicSortCompareSplitWithCopyParallel(vOutBitonicCompareSplitWithCopyParallel, W[i], threadsCount);
//        }, results[idx++], 10, "seconds", "to_var");
//        verifyVectors(expected, vOutBitonicCompareSplitWithCopyParallel, W[i]);
//        delete [] vOutBitonicCompareSplitWithCopyParallel;
//
//        labels[idx] = "Bitonic Sort Parallel Compare-Split Optimized\n";
//        auto vOutBitonicCompareSplitParallel = new float [W[i]];
//        std::copy(vIn, vIn + W[i], vOutBitonicCompareSplitParallel);
//        measure([&W, &vOutBitonicCompareSplitParallel, threadsCount, i] {
//            testBitonicSortCompareSplitParallel(vOutBitonicCompareSplitParallel, W[i], threadsCount);
//        }, results[idx++], 10, "seconds", "to_var");
//        verifyVectors(expected, vOutBitonicCompareSplitParallel, W[i]);
//        delete [] vOutBitonicCompareSplitParallel;

//        labels[idx] = "Bubble Sort Serial\n";
//        auto vOutBubbleSerial = new float [W[i]];
//        std::copy(vIn, vIn + W[i], vOutBubbleSerial);
//        measure([&W, &vOutBubbleSerial, i] {
//            testBubbleSerial(vOutBubbleSerial, W[i]);
//        }, results[idx++], 10, "seconds", "to_var");
//        verifyVectors(expected, vOutBubbleSerial, W[i]);
//        delete [] vOutBubbleSerial;

//        labels[idx] = "Bubble Sort Odd-Even Transposition Parallel\n";
//        auto vOutBubbleOddEvenTranspositionParallel = new float [W[i]];
//        std::copy(vIn, vIn + W[i], vOutBubbleOddEvenTranspositionParallel);
//        measure([&W, &vOutBubbleOddEvenTranspositionParallel, i, threadsCount] {
//            testBubbleOddEvenTranspositionParallel(vOutBubbleOddEvenTranspositionParallel, W[i], threadsCount);
//        }, results[idx++], 1, "seconds", "to_var");
//        //verifyVectors(expected, vOutBubbleOddEvenTranspositionParallel, W[i]);
//        delete [] vOutBubbleOddEvenTranspositionParallel;

/**
 * Very memory efficient:
 *      1) N=1073741824 => mem=4.5Gb and Ts=180.757s
 */
//        labels[idx] = "Fastest sequential \n";
//#ifdef MY_TEST
//        auto vOutFastestSerial = std::vector<float>(vIn);
//#else
//        auto vOutFastestSerial = vIn;
//#endif
//        measure([&W, &vOutFastestSerial, i] {
//            std::sort(vOutFastestSerial.begin(), vOutFastestSerial.begin()+W[i]);
//        }, results[idx++], 1, "seconds", "to_var");
//#ifdef MY_TEST
//        verifyVectors(expected, vOutFastestSerial, W[i]);
////        delete [] vOutFastestSerial;
//#endif

//        labels[idx] = "Quick sequential:\n";
//#ifdef MY_TEST
//        auto vOutQuickSerial = new float [W[i]];
//        std::copy(vIn, vIn + W[i], vOutQuickSerial);
//#else
//        auto vOutQuickSerial = vIn;
//#endif
//        measure([&W, &vOutQuickSerial, i] {
//            testQuickSerial(vOutQuickSerial, W[i]);
//        }, results[idx++], 1, "seconds", "to_var");
//#ifdef MY_TEST
//        verifyVectors(expected, vOutQuickSerial, W[i]);
//        delete [] vOutQuickSerial;
//#endif

/**
 * Very memory unefficient:
 *      pointers:
 *      0) N=67108864; p=16 => Tp=6.15065s;
 *      1) N=1073741824; p=16 => mem=43Gb; swap=30Gb; and Tp=180.757s;
 *      vector:
 *      0) N=67108864; p=16 => Tp=4.23539s;
 *      1) N=1073741824; p=16 => mem=32Gb; swap=20Gb; and Tp=51.2962s;
 */
//        labels[idx] = "Quick parallel:\n";
//#ifdef MY_TEST
//        auto vOutQuickParallel = std::vector<float>(vIn);
//#else
//        auto vOutQuickParallel = vIn;
//#endif
//        measure([&W, &vOutQuickParallel, i, threadsCount] {
//            testQuickSortParallel(vOutQuickParallel.begin(), W[i], threadsCount);
//        }, results[idx++], 1, "seconds", "to_var");
//#ifdef MY_TEST
//        verifyVectors(expected, vOutQuickParallel, W[i]);
//#endif

        /**
         *      vector:
         *      0) N=67108864; p=16 => Tp=7.23539s;
         *      1) N=1073741824; p=16 => mem=8Gb; Tp=165.769s;
         */
        labels[idx] = "Quick parallel with persistent thread pool:\n";
#ifdef MY_TEST
        auto vOutQuickPersistentThreadPoolParallel = std::vector<float>(vIn);
#else
        auto vOutQuickPersistentThreadPoolParallel = vIn;
#endif
        measure([&W, &vOutQuickPersistentThreadPoolParallel, i, threadsCount] {
            testQuickSortWithStableThreadPoolParallel(vOutQuickPersistentThreadPoolParallel.begin(), W[i], threadsCount);
        }, results[idx++], 1, "seconds", "to_var");
#ifdef MY_TEST
        verifyVectors(expected, vOutQuickPersistentThreadPoolParallel, W[i]);
        //delete [] vOutQuickPersistentThreadPoolParallel;
#endif

    }

    for (size_t i=0; i<results.size(); ++i) {
        std::cout << "\n" << labels[i];
        for (auto it: results[i]) {
            std::cout << it << ", ";
        }
    }
    
    delete [] W;
}

#endif /** SortAlgorithms_hpp */
