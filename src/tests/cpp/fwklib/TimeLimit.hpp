/*=========================================================================
* Copyright (c) 2002-2014 Pivotal Software, Inc. All Rights Reserved.
 * This product is protected by U.S. and international copyright
 * and intellectual property laws. Pivotal products are covered by
 * more patents listed at http://www.pivotal.io/patents.
* All Rights Reserved.
*=========================================================================
*/

#ifndef __TimeLimit_hpp__
#define __TimeLimit_hpp__

#include "fwklib/FwkLog.hpp"
#include <ace/Time_Value.h>

namespace gemfire {
namespace testframework {

class TimeLimit {
 private:
  ACE_Time_Value m_timeLimit;

 public:
  TimeLimit(int32_t seconds, int32_t defaultWait = 0) {
    m_timeLimit = ACE_OS::gettimeofday() +
                  ACE_Time_Value(((seconds <= 0) ? defaultWait : seconds));
  }

  bool limitExceeded() {
    if (m_timeLimit <= ACE_OS::gettimeofday()) {
      return true;
    }
    return false;
  }
};

}  // namespace testframework
}  // namespace gemfire

#endif  // __TimeLimit_hpp__
