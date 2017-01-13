/*=========================================================================
 * Copyright (c) 2002-2014 Pivotal Software, Inc. All Rights Reserved.
 * This product is protected by U.S. and international copyright
 * and intellectual property laws. Pivotal products are covered by
 * more patents listed at http://www.pivotal.io/patents.
 *=========================================================================
 */

/**
  * @file    FwkLog.hpp
  * @since   1.0
  * @version 1.0
  * @see
  *
  */

// ----------------------------------------------------------------------------

#ifndef __FWK_LOG_HPP__
#define __FWK_LOG_HPP__

#include <gfcpp/gf_base.hpp>

#include "FwkException.hpp"

#include <ace/ACE.h>
#include <ace/OS.h>
#include <ace/Task.h>

// kludge to compile on bar
#define _CPP_CMATH 1
#include <sstream>

// ----------------------------------------------------------------------------

namespace gemfire {
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
}  // namepace gemfire

#endif  // __FWK_LOG_HPP__
