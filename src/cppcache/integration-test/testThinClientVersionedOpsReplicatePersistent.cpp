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

#include "ThinClientVersionedOps.hpp"

DUNIT_MAIN
  {
    /*testServerGC, TestCase-3
     * Two clients, Two servers
     * One client connected to one server and another client connected to
     * another
     * server
     * both clients have registerAllKeys
     * Tombstone timeout on server reset to receive an early GC.
     * REPLICATE_PERSISTENT region. Destroy multiple keys, a GC is received and
     * it
     * should be handled properly.
     * For now, no error message is thrown. Log files need to be verified.
     */
    CALL_TASK(CreateServers_With_Locator_Disk);

    CALL_TASK(StartClient1);
    CALL_TASK(StartClient2);
    CALL_TASK(testServerGC);

    CALL_TASK(CloseClient1);
    CALL_TASK(CloseClient2);
    CALL_TASK(CloseServers_With_Locator);
  }
END_MAIN
