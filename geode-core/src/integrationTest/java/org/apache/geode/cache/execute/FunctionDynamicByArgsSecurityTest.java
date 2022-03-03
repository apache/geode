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

package org.apache.geode.cache.execute;

import java.util.Collection;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import org.junit.BeforeClass;
import org.junit.ClassRule;
import org.junit.Rule;
import org.junit.Test;

import org.apache.geode.cache.RegionShortcut;
import org.apache.geode.examples.SimpleSecurityManager;
import org.apache.geode.security.ResourcePermission;
import org.apache.geode.test.junit.rules.ConnectionConfiguration;
import org.apache.geode.test.junit.rules.GfshCommandRule;
import org.apache.geode.test.junit.rules.ServerStarterRule;


public class FunctionDynamicByArgsSecurityTest {
  private static final String RESULT_HEADER = "Message";

  @ClassRule
  public static ServerStarterRule server =
      new ServerStarterRule().withJMXManager().withSecurityManager(SimpleSecurityManager.class)
          .withRegion(RegionShortcut.PARTITION, "testRegion1")
          .withRegion(RegionShortcut.PARTITION, "testRegion2")
          .withAutoStart();

  @Rule
  public GfshCommandRule gfsh =
      new GfshCommandRule(server::getJmxPort, GfshCommandRule.PortType.jmxManager);

  private static final DynamicSecurityFunction function = new DynamicSecurityFunction();

  @BeforeClass
  public static void setupClass() {
    FunctionService.registerFunction(function);
  }

  @Test
  @ConnectionConfiguration(user = "DATAREADtestRegion1", password = "DATAREADtestRegion1")
  public void functionDynamicRequireExpectedPermission() throws Exception {
    gfsh.executeAndAssertThat(
        "execute function --id=" + function.getId() + " --arguments=testRegion1")
        .tableHasRowCount(1)
        .tableHasRowWithValues(RESULT_HEADER, "[successfully invoked with argument:testRegion1]")
        .statusIsSuccess();
    gfsh.executeAndAssertThat(
        "execute function --id=" + function.getId() + " --arguments=testRegion1,testRegion2")
        .tableHasRowCount(1)
        .tableHasRowWithValues(RESULT_HEADER,
            "Exception: DATAREADtestRegion1 not authorized for DATA:READ:testRegion2")
        .statusIsError();
  }

  static class DynamicSecurityFunction implements Function {
    @Override
    public void execute(FunctionContext context) {
      Object args = context.getArguments();
      String[] regions = (String[]) args;
      context.getResultSender()
          .lastResult("successfully invoked with argument:" + String.join(",", regions));
    }

    @Override
    public Collection<ResourcePermission> getRequiredPermissions(String regionName, Object args) {
      String[] regions = (String[]) args;
      return Stream.of(regions).map(s -> new ResourcePermission(ResourcePermission.Resource.DATA,
          ResourcePermission.Operation.READ, s)).collect(Collectors.toSet());
    }
  }
}
