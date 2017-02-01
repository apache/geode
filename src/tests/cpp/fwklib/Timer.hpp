#pragma once

#ifndef APACHE_GEODE_GUARD_8d07450f18b766edac2c13c91b9680d4
#define APACHE_GEODE_GUARD_8d07450f18b766edac2c13c91b9680d4

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


#include <gfcpp/gf_base.hpp>
#include "fwklib/FwkLog.hpp"

#include <ace/Time_Value.h>
#include <ace/High_Res_Timer.h>

#ifdef _WIN32

#include <time.h>
#include <stdio.h>
#include <stdlib.h>
#include <windows.h>
#include <winbase.h>

#endif

#ifdef _SOLARIS

#include <time.h>
#include <stdio.h>
#include <strings.h>

#endif

namespace apache {
namespace geode {
namespace client {
namespace testframework {

#ifdef _WIN32
static const double NPM = 1000.0;
static const double MPS = NPM * 1000.0;
static const double NPS = MPS * 1000.0;
#else
static const int64_t NPM = 1000LL;
static const int64_t MPS = NPM * 1000;
static const int64_t NPS = MPS * 1000;
#endif

class HRTimer {
#ifdef _WIN32

  LARGE_INTEGER m_start;
  LARGE_INTEGER m_stop;
  LARGE_INTEGER m_freq;

 public:
  inline HRTimer() {
    QueryPerformanceFrequency(&m_freq);
    start();
  }
  inline void start() { QueryPerformanceCounter(&m_start); }
  inline int64_t elapsedNanos() {
    QueryPerformanceCounter(&m_stop);
    return (int64_t)((((double)(m_stop.QuadPart - m_start.QuadPart) * NPS) /
                      (double)m_freq.QuadPart) +
                     .5);
  }
  inline int64_t elapsedMicros() {
    QueryPerformanceCounter(&m_stop);
    return (int64_t)((((double)(m_stop.QuadPart - m_start.QuadPart) * MPS) /
                      (double)m_freq.QuadPart) +
                     .5);
  }

#endif

#ifdef _SOLARIS

  struct timeval m_start;
  struct timeval m_stop;
  struct timespec m_HRstart;
  struct timespec m_HRstop;
  bool m_useHR;

 public:
  inline HRTimer() : m_useHR(true) {
    if (0 == clock_getres(CLOCK_HIGHRES, &m_HRstop)) {
      m_useHR = true;
      //      FWKINFO( "HRTimer resolution reported as " << m_HRstop.tv_sec <<
      //                " seconds, " << m_HRstop.tv_nsec << " nanoseconds." );
    }
    start();
  }

  inline void start() {
    if (m_useHR) {
      clock_gettime(CLOCK_HIGHRES, &m_HRstart);
    } else {
      gettimeofday(&m_start, NULL);
    }
  }

  inline unsigned long long elapsedNanos() {
    if (m_useHR) {
      clock_gettime(CLOCK_HIGHRES, &m_HRstop);
      return (((unsigned long long)(m_HRstop.tv_sec - m_HRstart.tv_sec) * NPS) +
              (unsigned long long)m_HRstop.tv_nsec -
              (unsigned long long)m_HRstart.tv_nsec);
    } else {
      gettimeofday(&m_stop, NULL);
      return (((unsigned long long)(m_stop.tv_sec - m_start.tv_sec) * NPS) +
              ((unsigned long long)m_stop.tv_usec * NPM) -
              ((unsigned long long)m_start.tv_usec * NPM));
    }
  }

  inline unsigned long long elapsedMicros() {
    if (m_useHR) {
      clock_gettime(CLOCK_HIGHRES, &m_HRstop);
      return (
          (((unsigned long long)(m_HRstop.tv_sec - m_HRstart.tv_sec) * NPS) +
           (unsigned long long)m_HRstop.tv_nsec -
           (unsigned long long)m_HRstart.tv_nsec + (NPM / 2)) /
          NPM);
    } else {
      gettimeofday(&m_stop, NULL);
      return (((unsigned long long)(m_stop.tv_sec - m_start.tv_sec) * MPS) +
              (unsigned long long)m_stop.tv_usec -
              (unsigned long long)m_start.tv_usec);
    }
  }

#endif

// TODO refactor - why not use the ACE_High_Res_Timer for all these platforms?
#if defined(_LINUX) || defined(_MACOSX)
  ACE_High_Res_Timer m_timer;

 public:
  inline HRTimer() { start(); }

  inline void start() {
    m_timer.reset();
    m_timer.start();
  }

  inline int64_t elapsedNanos() {
    m_timer.stop();
    ACE_hrtime_t e;
    m_timer.elapsed_time(e);
    return static_cast<int64_t>(e);
  }

  inline int64_t elapsedMicros() {
    m_timer.stop();
    ACE_hrtime_t e;
    m_timer.elapsed_microseconds(e);
    return static_cast<int64_t>(e);
  }

#endif  // _LINUX || _MACOSX
};

}  // namespace testframework
}  // namespace client
}  // namespace geode
}  // namespace apache


#endif // APACHE_GEODE_GUARD_8d07450f18b766edac2c13c91b9680d4
