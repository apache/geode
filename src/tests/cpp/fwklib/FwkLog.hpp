#pragma once

#ifndef APACHE_GEODE_GUARD_09e162ef70ce6628f1f097a2c0333b34
#define APACHE_GEODE_GUARD_09e162ef70ce6628f1f097a2c0333b34

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

/**
  * @file    FwkLog.hpp
  * @since   1.0
  * @version 1.0
  * @see
  *
  */

// ----------------------------------------------------------------------------


#include <gfcpp/gf_base.hpp>

#include "FwkException.hpp"

#include <ace/ACE.h>
#include <ace/OS.h>
#include <ace/Task.h>

// kludge to compile on bar
#define _CPP_CMATH 1
#include <sstream>

// ----------------------------------------------------------------------------

namespace apache {
namespace geode {
namespace client {
namespace testframework {

const char* strnrchr(const char* str, const char tok, int32_t cnt);
const char* dirAndFile(const char* str);
void plog(const char* l, const char* s, const char* filename, int32_t lineno);
void dumpStack();
const char* getNodeName();

/* Macro for logging */
#ifdef DEBUG

#define FWKDEBUG(x)                                      \
  {                                                      \
    std::ostringstream os;                               \
    os << x;                                             \
    plog("Debug", os.str().c_str(), __FILE__, __LINE__); \
  }

#define FWKSLEEP(x) \
  { ACE_OS::sleep(ACE_Time_Value(x, 0)); }

#else

#define FWKDEBUG(x)

#define FWKSLEEP(x) \
  { ACE_OS::sleep(ACE_Time_Value(x, 0)); }

#endif

#define FWKINFO(x)                                      \
  {                                                     \
    std::ostringstream os;                              \
    os << x;                                            \
    plog("Info", os.str().c_str(), __FILE__, __LINE__); \
  }
#define FWKWARN(x)                                      \
  {                                                     \
    std::ostringstream os;                              \
    os << x;                                            \
    plog("Warn", os.str().c_str(), __FILE__, __LINE__); \
  }
#define FWKERROR(x)                                      \
  {                                                      \
    std::ostringstream os;                               \
    os << x;                                             \
    plog("Error", os.str().c_str(), __FILE__, __LINE__); \
  }
#define FWKSEVERE(x)                                      \
  {                                                       \
    std::ostringstream os;                                \
    os << x;                                              \
    plog("Severe", os.str().c_str(), __FILE__, __LINE__); \
  }
#define FWKEXCEPTION(x)                                              \
  {                                                                  \
    std::ostringstream os;                                           \
    os << x << " In file: " << __FILE__ << " at line: " << __LINE__; \
    throw FwkException(os.str().c_str());                            \
  }

#define WAITFORDEBUGGER(x)                                               \
  {                                                                      \
    plog("Info", "Waiting for debugger ...", __FILE__, __LINE__);        \
    for (int32_t i = x; i > 0; i--) ACE_OS::sleep(ACE_Time_Value(1, 0)); \
  }

#define DUMPSTACK(x) \
  {                  \
    FWKSEVERE(x);    \
    dumpStack();     \
  }

}  // namespace  testframework
}  // namespace client
}  // namespace geode
}  // namespace apache


#endif // APACHE_GEODE_GUARD_09e162ef70ce6628f1f097a2c0333b34
