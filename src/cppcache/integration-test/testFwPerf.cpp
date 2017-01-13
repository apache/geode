/*=========================================================================
 * Copyright (c) 2010-2014 Pivotal Software, Inc. All Rights Reserved.
 * This product is protected by U.S. and international copyright
 * and intellectual property laws. Pivotal products are covered by
 * one or more patents listed at http://www.pivotal.io/patents.
 *=========================================================================
 */

#include "fw_dunit.hpp"
#include <gfcpp/GemfireCppCache.hpp>

#define ROOT_NAME "testFwPerf"

perf::PerfSuite perfSuite("FwPerf");

class LocalPutTask : public perf::Thread {
 private:
  int m_iters ATTR_UNUSED;

 public:
  LocalPutTask() : Thread() {}

  virtual void setup() {
    fprintf(stdout, "performed my setup...\n");
    fflush(stdout);
  }

  virtual void perftask() {
    ACE_OS::sleep(1);
    fprintf(stdout, "perffunc done.\n");
    fflush(stdout);
  }

  virtual void cleanup() {
    fprintf(stdout, "performed my cleanup...\n");
    fflush(stdout);
  }
};

// all creates, no map growth, no replaces.
DUNIT_TASK(s1p1, LocalPut)
  {
    int iters = 1;
    int threads = 4;

    LocalPutTask taskDef;
    perf::ThreadLauncher tl(threads, taskDef);
    tl.go();

    perfSuite.addRecord(fwtest_Name, iters * threads, tl.startTime(),
                        tl.stopTime());
  }
END_TASK(x)

DUNIT_TASK(s1p1, Finish)
  { perfSuite.save(); }
END_TASK(x)
