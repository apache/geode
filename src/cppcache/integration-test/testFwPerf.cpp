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

#include "fw_dunit.hpp"
#include <gfcpp/GeodeCppCache.hpp>

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
