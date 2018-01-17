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

import org.junit.BeforeClass;
import org.junit.ClassRule;
import org.junit.Rule;
import org.junit.Test;
import org.junit.experimental.categories.Category;

import org.apache.geode.cache.execute.Function;
import org.apache.geode.cache.execute.FunctionContext;
import org.apache.geode.cache.execute.FunctionService;
import org.apache.geode.connectors.jdbc.internal.JdbcConnectorService;
import org.apache.geode.examples.SimpleSecurityManager;
import org.apache.geode.management.internal.cli.functions.CliFunctionResult;
import org.apache.geode.test.junit.categories.IntegrationTest;
import org.apache.geode.test.junit.rules.ConnectionConfiguration;
import org.apache.geode.test.junit.rules.GfshCommandRule;
import org.apache.geode.test.junit.rules.ServerStarterRule;

class InheritsDefaultPermissionsJDBCFunction extends JdbcCliFunction<String, CliFunctionResult> {

  InheritsDefaultPermissionsJDBCFunction() {
    super(new FunctionContextArgumentProvider(), new ExceptionHandler());
  }

  @Override
  CliFunctionResult getFunctionResult(JdbcConnectorService service,
      FunctionContext<String> context) {
    return new CliFunctionResult();
  }
}


@Category({IntegrationTest.class, SecurityException.class})
public class JDBCConnectorFunctionsSecurityTest {

  private static Function alterConnectionFunction = new AlterConnectionFunction();
  private static Function alterMappingFunction = new AlterMappingFunction();
  private static Function createConnectionFunction = new CreateConnectionFunction();
  private static Function createMappingFunction = new CreateMappingFunction();
  private static Function describeConnectionFunction = new DescribeConnectionFunction();
  private static Function describeMappingFunction = new DescribeMappingFunction();
  private static Function destroyConnectionFunction = new DestroyConnectionFunction();
  private static Function destroyMappingFunction = new DestroyMappingFunction();
  private static Function listConnectionFunction = new ListConnectionFunction();
  private static Function listMappingFunction = new ListMappingFunction();
  private static Function inheritsDefaultPermissionsFunction =
      new InheritsDefaultPermissionsJDBCFunction();

  @ClassRule
  public static ServerStarterRule server = new ServerStarterRule().withJMXManager()
      .withSecurityManager(SimpleSecurityManager.class).withAutoStart();

  @Rule
  public GfshCommandRule gfsh =
      new GfshCommandRule(server::getJmxPort, GfshCommandRule.PortType.jmxManager);

  @BeforeClass
  public static void setupClass() {
    FunctionService.registerFunction(alterConnectionFunction);
    FunctionService.registerFunction(alterMappingFunction);
    FunctionService.registerFunction(createConnectionFunction);
    FunctionService.registerFunction(createMappingFunction);
    FunctionService.registerFunction(describeConnectionFunction);
    FunctionService.registerFunction(describeMappingFunction);
    FunctionService.registerFunction(destroyConnectionFunction);
    FunctionService.registerFunction(destroyMappingFunction);
    FunctionService.registerFunction(listConnectionFunction);
    FunctionService.registerFunction(listMappingFunction);
    FunctionService.registerFunction(inheritsDefaultPermissionsFunction);
  }

  @Test
  @ConnectionConfiguration(user = "dataWrite", password = "dataWrite")
  public void testInvalidPermissionsForAlterConnectionFunction() {
    gfsh.executeAndAssertThat("execute function --id=" + alterConnectionFunction.getId())
        .containsOutput("not authorized for CLUSTER:MANAGE").statusIsError();
  }

  @Test
  @ConnectionConfiguration(user = "dataWrite", password = "dataWrite")
  public void testInvalidPermissionsForAlterMappingFunction() {
    gfsh.executeAndAssertThat("execute function --id=" + alterMappingFunction.getId())
        .containsOutput("not authorized for CLUSTER:MANAGE").statusIsError();
  }

  @Test
  @ConnectionConfiguration(user = "dataWrite", password = "dataWrite")
  public void testInvalidPermissionsForCreateConnectionFunction() {
    gfsh.executeAndAssertThat("execute function --id=" + createConnectionFunction.getId())
        .containsOutput("not authorized for CLUSTER:MANAGE").statusIsError();
  }

  @Test
  @ConnectionConfiguration(user = "dataWrite", password = "dataWrite")
  public void testInvalidPermissionsForCreateMappingFunction() {
    gfsh.executeAndAssertThat("execute function --id=" + createMappingFunction.getId())
        .containsOutput("not authorized for CLUSTER:MANAGE").statusIsError();
  }

  @Test
  @ConnectionConfiguration(user = "dataWrite", password = "dataWrite")
  public void testInvalidPermissionsForDescribeConnectionFunction() {
    gfsh.executeAndAssertThat("execute function --id=" + describeConnectionFunction.getId())
        .containsOutput("not authorized for CLUSTER:READ").statusIsError();
  }

  @Test
  @ConnectionConfiguration(user = "dataWrite", password = "dataWrite")
  public void testInvalidPermissionsForDescribeMappingFunction() {
    gfsh.executeAndAssertThat("execute function --id=" + describeMappingFunction.getId())
        .containsOutput("not authorized for CLUSTER:READ").statusIsError();
  }

  @Test
  @ConnectionConfiguration(user = "dataWrite", password = "dataWrite")
  public void testInvalidPermissionsForDestroyConnectionFunction() {
    gfsh.executeAndAssertThat("execute function --id=" + destroyConnectionFunction.getId())
        .containsOutput("not authorized for CLUSTER:MANAGE").statusIsError();
  }

  @Test
  @ConnectionConfiguration(user = "dataWrite", password = "dataWrite")
  public void testInvalidPermissionsForDestroyMappingFunction() {
    gfsh.executeAndAssertThat("execute function --id=" + destroyMappingFunction.getId())
        .containsOutput("not authorized for CLUSTER:MANAGE").statusIsError();
  }

  @Test
  @ConnectionConfiguration(user = "dataWrite", password = "dataWrite")
  public void testInvalidPermissionsForFunctionInheritingDefaultPermissions() {
    gfsh.executeAndAssertThat("execute function --id=" + inheritsDefaultPermissionsFunction.getId())
        .containsOutput("not authorized for CLUSTER:READ").statusIsError();
  }
}
