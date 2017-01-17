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

#include "ThinClientDurable.hpp"

DUNIT_MAIN
  {
    startServers();

    CALL_TASK(FeederInit);

    CALL_TASK(InitClient1Timeout30);
    CALL_TASK(InitClient2Timeout30);

    CALL_TASK(FeederUpdate1);

    // Verify that the clients receive the first set of events from feeder.
    CALL_TASK(VerifyFeederUpdate_1_C1);
    CALL_TASK(VerifyFeederUpdate_1_C2);

    CALL_TASK(DisconnectClient1);
    CALL_TASK(DisconnectClient2);

    CALL_TASK(FeederUpdate2);

    CALL_TASK(ReviveClient1Delayed);
    CALL_TASK(ReviveClient2AndWait);

    CALL_TASK(VerifyClient1KeepAliveFalse);
    CALL_TASK(VerifyClient2KeepAliveFalse);

    CALL_TASK(CloseFeeder);
    CALL_TASK(CloseClient1);
    CALL_TASK(CloseClient2);
    CALL_TASK(CloseServers);

    closeLocator();
  }
END_MAIN
