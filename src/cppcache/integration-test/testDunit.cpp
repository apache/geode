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

#define ROOT_NAME "testDunit"

#include "fw_dunit.hpp"
#include <gfcpp/GeodeCppCache.hpp>

int getSlaveTest() {
  return dunit::globals()->getIntValue("test_alive_slaves");
}

// while this itself isn't thread/process safe, there shouldn't be concurrency
// in a dunit test anyway.
void incrementSlaveTest() {
  dunit::globals()->rebind("test_alive_slaves", getSlaveTest() + 1);
}

DUNIT_TASK(s1p1, One)
  {
    dunit::globals()->rebind("from1", 100);
    LOG("bound from1 = 100");
    incrementSlaveTest();
  }
END_TASK(One)

DUNIT_TASK(s1p2, Two)
  {
    ASSERT(dunit::globals()->getIntValue("from1") == 100, "expected 100");
    LOG("looked up from1, found 100");
    incrementSlaveTest();
  }
END_TASK(Two)

DUNIT_TASK(s2p1, Three)
  { incrementSlaveTest(); }
END_TASK(Three)

DUNIT_TASK(s2p2, Four)
  { incrementSlaveTest(); }
END_TASK(Four)

// Now test that none of the slaves are dead after executing their first
// task.

DUNIT_TASK(s1p1, Test1)
  { incrementSlaveTest(); }
END_TASK(Test1)

DUNIT_TASK(s1p2, Test2)
  { incrementSlaveTest(); }
END_TASK(Test2)

DUNIT_TASK(s2p1, Test3)
  { incrementSlaveTest(); }
END_TASK(Test3)

DUNIT_TASK(s2p2, Test4)
  { incrementSlaveTest(); }
END_TASK(Test4)

DUNIT_TASK(s1p1, TestA)
  {
    test::cout << "SlaveTest = " << getSlaveTest() << test::endl;
    ASSERT(getSlaveTest() == 8,
           "a previous slave must have failed undetected.");
    dunit::globals()->dump();
  }
END_TASK(TestA)
