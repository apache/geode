/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

#define ROOT_NAME "testNativeCompareBasic"

#include "fw_helper.hpp"
#include "../../tests/cli/NativeWrapper/NativeType.cpp"

#define WARMUP_ITERS 1000000
#define TIMED_ITERS 50000000
#define TIMED_OBJSIZE 10

BEGIN_TEST(NATIVE_OPS_PERF)
  {
    NativeType obj;

    // warmup task
    bool res = true;
    for (int i = 1; i <= WARMUP_ITERS; ++i) {
      res &= obj.doOp(TIMED_OBJSIZE, 0, 0);
    }
    ASSERT(res, "Expected the object to be alive");

// timed task

#ifdef _WIN32
    LARGE_INTEGER freq, start, end;
    QueryPerformanceFrequency(&freq);
    QueryPerformanceCounter(&start);
#else
    ACE_Time_Value start, end;
    start = ACE_OS::gettimeofday();
#endif  //_WIN32

    for (int i = 1; i <= TIMED_ITERS; ++i) {
      res &= obj.doOp(TIMED_OBJSIZE, 0, 0);
    }

#ifdef _WIN32
    QueryPerformanceCounter(&end);
    double time = (double)(end.LowPart - start.LowPart) / (double)freq.LowPart;
    printf("Performance counter in native test with result %d is: %lf\n", res,
           time);
#else
    end = ACE_OS::gettimeofday();
    end -= start;
    printf("Time taken in native test with result %d is: %lu.%06lusecs\n", res,
           end.sec(), static_cast<unsigned long>(end.usec()));
#endif  //_WIN32
    ASSERT(res, "Expected the object to be alive");
  }
END_TEST(NATIVE_OPS_PERF)
