/*=========================================================================
 * Copyright (c) 2010-2014 Pivotal Software, Inc. All Rights Reserved.
 * This product is protected by U.S. and international copyright
 * and intellectual property laws. Pivotal products are covered by
 * one or more patents listed at http://www.pivotal.io/patents.
 *=========================================================================
 */

#include <gfcpp/gfcpp_globals.hpp>

#include <ace/Time_Value.h>
#include <ace/OS.h>

namespace test {
void dummyFunc() {}

void millisleep(uint32_t x) {
  ACE_Time_Value timeV(0);
  timeV.msec(static_cast<long>(x));
  ACE_OS::sleep(timeV);
}
}

#define SLEEP(x) test::millisleep(x);
#define LOG LOGDEBUG

#include "TallyListener.hpp"
#include "TallyLoader.hpp"
#include "TallyWriter.hpp"

#ifdef _WIN32
#define _T_DLL_EXPORT __declspec(dllexport)
#else
#define _T_DLL_EXPORT
#endif

extern "C" {

_T_DLL_EXPORT gemfire::CacheListener* createCacheListener() {
  TallyListener* tl = new TallyListener();
  tl->beQuiet(true);
  return tl;
}

_T_DLL_EXPORT gemfire::CacheLoader* createCacheLoader() {
  return new TallyLoader();
}

_T_DLL_EXPORT gemfire::CacheWriter* createCacheWriter() {
  return new TallyWriter();
}
}
