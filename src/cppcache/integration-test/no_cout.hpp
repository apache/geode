/*=========================================================================
 * Copyright (c) 2010-2014 Pivotal Software, Inc. All Rights Reserved.
 * This product is protected by U.S. and international copyright
 * and intellectual property laws. Pivotal products are covered by
 * one or more patents listed at http://www.pivotal.io/patents.
 *=========================================================================
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
}
