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
package org.apache.geode.internal.cache;

import static java.util.concurrent.TimeUnit.SECONDS;
import static org.apache.geode.test.awaitility.GeodeAwaitility.await;

import java.util.concurrent.TimeoutException;

import org.apache.geode.cache.Cache;
import org.apache.geode.test.dunit.DUnitBlackboard;
import org.apache.geode.test.dunit.VM;

public class SignalBounceOnRequestImageMessageObserver extends OnRequestImageMessageObserver {
  private static final String gateName = "bounce";

  public SignalBounceOnRequestImageMessageObserver(String regionName, Cache cache,
      DUnitBlackboard dUnitBlackboard) {
    super(regionName, () -> {
      dUnitBlackboard.signalGate(gateName);
      await().until(cache::isClosed);
    });
  }

  public static void waitThenBounce(DUnitBlackboard dUnitBlackboard, VM serverVM)
      throws TimeoutException, InterruptedException {
    dUnitBlackboard.waitForGate(gateName, 30, SECONDS);
    serverVM.bounceForcibly();
  }
}
