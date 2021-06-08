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
package org.apache.geode.test.dunit.internal;

import static org.junit.Assert.fail;

import java.util.Arrays;
import java.util.Collection;

import org.apache.logging.log4j.Logger;
import org.junit.Rule;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import org.apache.geode.logging.internal.log4j.api.LogService;
import org.apache.geode.test.dunit.rules.ClientVM;
import org.apache.geode.test.dunit.rules.ClusterStartupRule;
import org.apache.geode.test.dunit.rules.MemberVM;
import org.apache.geode.test.junit.rules.VMProvider;
import org.apache.geode.test.junit.runners.CategoryWithParameterizedRunnerFactory;

@RunWith(Parameterized.class)
@Parameterized.UseParametersRunnerFactory(CategoryWithParameterizedRunnerFactory.class)
public class SuspiciousLogCheckDUnitTest {

  public SuspiciousLogCheckDUnitTest(String memberType) {
    this.memberType = memberType;
  }

  @Parameterized.Parameters(name = "memberType_{0}")
  public static Collection getMemberType() {
    return Arrays.asList("locator", "server", "client");
  }

  public String memberType;

  @Rule
  public ClusterStartupRule clusterStartupRule = new ClusterStartupRule();

  @Test
  public void whenLocatorIsStoppedSuspiciousLogsMustBeChecked() throws Exception {
    MemberVM locator = clusterStartupRule.startLocatorVM(0);
    int locatorPort = locator.getPort();
    VMProvider memberToCheck = null;
    int vmIndex = -1;
    switch (memberType) {
      case "locator":
        memberToCheck = locator;
        vmIndex = 0;
        break;
      case "server":
        MemberVM server =
            clusterStartupRule.startServerVM(1, s -> s.withConnectionToLocator(locatorPort));
        memberToCheck = server;
        vmIndex = 1;
        break;
      case "client":
        ClientVM client =
            clusterStartupRule.startClientVM(2, c -> c.withLocatorConnection(locatorPort));
        memberToCheck = client;
        vmIndex = 2;
        break;
      default:
        fail("Member type parameter is missing (accepted types are: locator,server,client)");
    }
    memberToCheck.invoke(() -> {
      Logger logger = LogService.getLogger();
      logger.fatal("Dummy fatal error message");
    });
    try {
      clusterStartupRule.stop(vmIndex);
      fail();
    } catch (AssertionError error) {
      // Assertion error is expected
    } catch (Exception exception) {
      fail();
    }
  }
}
