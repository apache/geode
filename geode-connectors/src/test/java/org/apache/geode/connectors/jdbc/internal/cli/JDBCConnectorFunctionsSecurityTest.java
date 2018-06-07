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

package org.apache.geode.connectors.jdbc.internal.cli;

import java.util.HashMap;
import java.util.Map;

import org.junit.BeforeClass;
import org.junit.ClassRule;
import org.junit.Rule;
import org.junit.Test;
import org.junit.experimental.categories.Category;

import org.apache.geode.cache.execute.Function;
import org.apache.geode.cache.execute.FunctionContext;
import org.apache.geode.cache.execute.FunctionService;
import org.apache.geode.examples.SimpleSecurityManager;
import org.apache.geode.management.cli.CliFunction;
import org.apache.geode.management.internal.cli.functions.CliFunctionResult;
import org.apache.geode.test.junit.categories.IntegrationTest;
import org.apache.geode.test.junit.rules.ConnectionConfiguration;
import org.apache.geode.test.junit.rules.GfshCommandRule;
import org.apache.geode.test.junit.rules.ServerStarterRule;

class InheritsDefaultPermissionsJDBCFunction extends CliFunction<String> {

  InheritsDefaultPermissionsJDBCFunction() {
    super();
  }

  @Override
  public CliFunctionResult executeFunction(FunctionContext<String> context) {
    return new CliFunctionResult("some-member", true, "some-message");
  }
}


@Category({IntegrationTest.class, SecurityException.class})
public class JDBCConnectorFunctionsSecurityTest {
  @ClassRule
  public static ServerStarterRule server = new ServerStarterRule().withJMXManager()
      .withSecurityManager(SimpleSecurityManager.class).withAutoStart();

  @Rule
  public GfshCommandRule gfsh =
      new GfshCommandRule(server::getJmxPort, GfshCommandRule.PortType.jmxManager);

  private static Map<Function, String> functionStringMap = new HashMap<>();

  @BeforeClass
  public static void setupClass() {
    functionStringMap.put(new AlterConnectionFunction(), "*");
    functionStringMap.put(new AlterMappingFunction(), "*");
    functionStringMap.put(new CreateConnectionFunction(), "*");
    functionStringMap.put(new CreateMappingFunction(), "*");
    functionStringMap.put(new DestroyConnectionFunction(), "*");
    functionStringMap.put(new DestroyMappingFunction(), "*");
    functionStringMap.put(new InheritsDefaultPermissionsJDBCFunction(), "*");
    functionStringMap.keySet().forEach(FunctionService::registerFunction);
  }

  @Test
  @ConnectionConfiguration(user = "user", password = "user")
  public void functionRequireExpectedPermission() throws Exception {
    functionStringMap.entrySet().stream().forEach(entry -> {
      Function function = entry.getKey();
      String permission = entry.getValue();
      gfsh.executeAndAssertThat("execute function --id=" + function.getId())
          .tableHasRowCount("Message", 1)
          .tableHasRowWithValues("Message",
              "Exception: user not authorized for " + permission)
          .statusIsError();
    });
  }
}
