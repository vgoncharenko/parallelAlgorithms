//
//  measure.cpp
//  parallelAlgs
//
//  Created by Vitaliy on 31.05.2021.
//
#ifndef measure_h
#define measure_h

#include <chrono>
#include <iostream>
#include <vector>

template<typename Callable>
void measure(Callable f,
             std::vector<float> &resultBuffer,
             int iterations = 10,
             const std::string &timeScale = "nanoseconds",
             const std::string &outputType = "verbose") {
    std::vector<long> results(iterations);
    for (int i = 0; i < iterations; i++) {
        auto start = std::chrono::high_resolution_clock::now();
        f();
        auto stop = std::chrono::high_resolution_clock::now();
        auto duration = std::chrono::duration_cast<std::chrono::nanoseconds>(stop - start);
        results[i] = duration.count();
    }

    std::sort(results.begin(), results.end());

    double elapsedTime = results[iterations / 2];
    if (timeScale == "seconds") {
        elapsedTime /= 1e9;
    }
    if (outputType == "verbose") {
        std::cout << "Median of time taken by function: "
                  << elapsedTime << " " << timeScale << std::endl;
    } else if (outputType == "nonverbose") {
        std::cout << elapsedTime << ", ";
    } else if (outputType == "to_var") {
        resultBuffer.push_back(elapsedTime);
    }
}

template<typename Callable>
void measure(Callable f,
             int iterations = 10,
             const std::string &timeScale = "nanoseconds",
             const std::string &outputType = "verbose") {
    std::vector<float> resultBuffer;
    measure(f, resultBuffer, iterations, timeScale, outputType);
}

#endif /* measure_h */
