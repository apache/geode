#pragma once

#ifndef APACHE_GEODE_GUARD_1628cb6366f926a3cac7309d67d1a4d7
#define APACHE_GEODE_GUARD_1628cb6366f926a3cac7309d67d1a4d7

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


#include "fwklib/FwkLog.hpp"
#include <ace/Time_Value.h>

namespace apache {
namespace geode {
namespace client {
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
}
}
}


#endif // APACHE_GEODE_GUARD_1628cb6366f926a3cac7309d67d1a4d7
