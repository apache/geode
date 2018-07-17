/*
 * Licensed to the Apache Software Foundation (ASF) under one or more contributor license
 * agreements. See the NOTICE file distributed with this work for additional information regarding
 * copyright ownership. The ASF licenses this file to You under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance with the License. You may obtain a
 * copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software distributed under the License
 * is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express
 * or implied. See the License for the specific language governing permissions and limitations under
 * the License.
 */
package org.apache.geode.test.dunit;

import static org.apache.geode.test.dunit.Invoke.invokeInEveryVM;

import org.apache.geode.admin.internal.AdminDistributedSystemImpl;
import org.apache.geode.distributed.DistributedSystem;
import org.apache.geode.distributed.internal.InternalDistributedSystem;
import org.apache.geode.internal.cache.GemFireCacheImpl;

public class Disconnect {

  public static void disconnectAllFromDS() {
    disconnectFromDS();
    invokeInEveryVM("disconnectFromDS", () -> disconnectFromDS());
  }

  /**
   * Disconnects this VM from the distributed system
   */
  public static void disconnectFromDS() {
    GemFireCacheImpl.testCacheXml = null;

    for (;;) {
      DistributedSystem ds = InternalDistributedSystem.getConnectedInstance();
      if (ds == null) {
        break;
      }
      try {
        ds.disconnect();
      } catch (Exception ignore) {
      }
    }

    AdminDistributedSystemImpl ads = AdminDistributedSystemImpl.getConnectedInstance();
    if (ads != null) {
      ads.disconnect();
    }
  }
}
