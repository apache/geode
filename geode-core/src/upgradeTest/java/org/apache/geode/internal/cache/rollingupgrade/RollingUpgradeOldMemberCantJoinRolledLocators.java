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
package org.apache.geode.internal.cache.rollingupgrade;

import java.util.Properties;

import org.junit.Test;

import org.apache.geode.test.dunit.Host;
import org.apache.geode.test.dunit.RMIException;
import org.apache.geode.test.dunit.VM;

public class RollingUpgradeOldMemberCantJoinRolledLocators extends RollingUpgrade2DUnitTestBase {


  /**
   * Demonstrate that an old process can't join a system that has upgraded locators. This is for
   * bugs #50510 and #50742.
   */
  @Test
  public void testOldMemberCantJoinRolledLocators() {
    VM oldServer = Host.getHost(0).getVM(oldVersion, 1);
    Properties props = getSystemProperties(); // uses the DUnit locator
    try {
      oldServer.invoke(invokeCreateCache(props));
    } catch (RMIException e) {
      Throwable cause = e.getCause();
      if ((cause instanceof AssertionError)) {
        cause = cause.getCause();
        if (cause != null && cause.getMessage() != null && !cause.getMessage().startsWith(
            "Rejecting the attempt of a member using an older version of the product to join the distributed system")) {
          throw e;
        }
      }
    }
  }

}
