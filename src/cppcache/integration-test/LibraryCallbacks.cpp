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
}  // namespace test

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

_T_DLL_EXPORT apache::geode::client::CacheListener* createCacheListener() {
  TallyListener* tl = new TallyListener();
  tl->beQuiet(true);
  return tl;
}

_T_DLL_EXPORT apache::geode::client::CacheLoader* createCacheLoader() {
  return new TallyLoader();
}

_T_DLL_EXPORT apache::geode::client::CacheWriter* createCacheWriter() {
  return new TallyWriter();
}
}
