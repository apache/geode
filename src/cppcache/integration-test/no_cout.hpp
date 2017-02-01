#pragma once

#ifndef GEODE_INTEGRATION_TEST_NO_COUT_H_
#define GEODE_INTEGRATION_TEST_NO_COUT_H_

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

#include <ace/OS.h>

namespace test {

class NOCout {
 private:
  bool m_needHeading;

  void heading() {
    if (m_needHeading) {
      fprintf(stdout, "[TEST %d] ", ACE_OS::getpid());
      m_needHeading = false;
    }
  }

 public:
  NOCout() : m_needHeading(true) {}

  ~NOCout() {}

  enum FLAGS { endl = 1, flush, hex, dec };

  NOCout& operator<<(const char* msg) {
    fprintf(stdout, "%s", msg);
    return *this;
  }

  NOCout& operator<<(void* v) {
    fprintf(stdout, "%p", v);
    return *this;
  }

  NOCout& operator<<(int v) {
    fprintf(stdout, "%d", v);
    return *this;
  }

  NOCout& operator<<(std::string& str) {
    fprintf(stdout, "%s", str.c_str());
    return *this;
  }

  NOCout& operator<<(FLAGS& flag) {
    if (flag == endl) {
      fprintf(stdout, "\n");
      m_needHeading = true;
    } else if (flag == flush) {
      fflush(stdout);
    }
    return *this;
  }
};

extern NOCout cout;
extern NOCout::FLAGS endl;
extern NOCout::FLAGS flush;
extern NOCout::FLAGS hex;
extern NOCout::FLAGS dec;
}  // namespace test

#endif // GEODE_INTEGRATION_TEST_NO_COUT_H_
