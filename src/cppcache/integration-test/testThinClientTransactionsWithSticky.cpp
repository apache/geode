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

#include "ThinClientTransactions.hpp"

DUNIT_MAIN
  {
    CALL_TASK(Alter_Client_Grid_Property_1);
    CALL_TASK(Alter_Client_Grid_Property_2);

    CALL_TASK(CreateLocator1);
    CALL_TASK(CreateServer1_With_Locator)

    CALL_TASK(CreateNonexistentServerRegion_Pooled_Locator_Sticky);
    CALL_TASK(CreateClient1PooledRegionWithSticky);
    CALL_TASK(CreateClient2PooledRegionWithSticky);

    CALL_TASK(CreateClient1Entries);
    CALL_TASK(CreateClient2Entries);
    CALL_TASK(UpdateClient1Entries);
    CALL_TASK(UpdateClient2Entries);
    CALL_TASK(CreateClient1EntryTwice);

    CALL_TASK(CreateClient1KeyThriceWithSticky);

    CALL_TASK(SuspendResumeInThread);
    CALL_TASK(SuspendResumeCommit);
    CALL_TASK(SuspendResumeRollback);
    CALL_TASK(SuspendTimeOut);

    CALL_TASK(CloseCache1);
    CALL_TASK(CloseCache2);
    CALL_TASK(CloseServer1);
    CALL_TASK(CloseLocator1);
  }
END_MAIN
