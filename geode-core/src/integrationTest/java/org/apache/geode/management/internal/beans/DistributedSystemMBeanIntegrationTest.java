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

package org.apache.geode.management.internal.beans;

import static org.assertj.core.api.Assertions.assertThat;

import java.text.SimpleDateFormat;
import java.time.LocalDate;
import java.util.Date;

import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.ClassRule;
import org.junit.Rule;
import org.junit.Test;

import org.apache.geode.cache.Region;
import org.apache.geode.cache.RegionShortcut;
import org.apache.geode.internal.logging.DateFormatter;
import org.apache.geode.management.DistributedSystemMXBean;
import org.apache.geode.management.model.Employee;
import org.apache.geode.test.junit.assertions.TabularResultModelAssert;
import org.apache.geode.test.junit.rules.GfshCommandRule;
import org.apache.geode.test.junit.rules.MBeanServerConnectionRule;
import org.apache.geode.test.junit.rules.ServerStarterRule;

public class DistributedSystemMBeanIntegrationTest {

  public static final String SELECT_ALL = "select * from /testRegion r where r.id=1";
  public static final String SELECT_ALL_BUT_LOCAL_DATE =
      "select name, address, startDate, endDate, title from /testRegion r where r.id=1";
  public static final String SELECT_FIELDS = "select id, title from /testRegion r where r.id=1";

  @ClassRule
  public static ServerStarterRule server = new ServerStarterRule()
      .withNoCacheServer()
      .withJMXManager()
      .withRegion(RegionShortcut.REPLICATE, "testRegion")
      .withAutoStart();

  @Rule
  public MBeanServerConnectionRule connectionRule =
      new MBeanServerConnectionRule(server::getJmxPort);

  @Rule
  public GfshCommandRule gfsh = new GfshCommandRule();

  private DistributedSystemMXBean bean;

  private static Date date;
  private static java.sql.Date sqlDate;
  private static LocalDate localDate;

  @BeforeClass
  public static void setupClass() throws Exception {
    Region<Object, Object> testRegion = server.getCache().getRegion("testRegion");
    localDate = LocalDate.of(2020, 1, 1);
    sqlDate = java.sql.Date.valueOf(localDate);
    date = new Date(sqlDate.getTime());
    Employee employee = new Employee();
    employee.setId(1);
    employee.setName("John");
    employee.setTitle("Manager");
    employee.setStartDate(date);
    employee.setEndDate(sqlDate);
    employee.setBirthday(localDate);
    testRegion.put(1, employee);
  }

  @Before
  public void setup() throws Exception {
    connectionRule.connect(server.getJmxPort());
    bean = connectionRule.getProxyMXBean(DistributedSystemMXBean.class);
  }

  // this is to make sure dates are formatted correctly and it does not honor the json annotations
  @Test
  public void queryAllUsingMBean() throws Exception {
    SimpleDateFormat formater = DateFormatter.createLocalizedDateFormat();
    String dateString = formater.format(date);
    String result = bean.queryData(SELECT_ALL, "server", 100);
    System.out.println(result);
    assertThat(result)
        .contains("id")
        .contains("title")
        .contains("address")
        .doesNotContain("Job Title")
        .contains("\"java.util.Date\",\"" + dateString + "\"")
        .contains("\"java.sql.Date\",\"" + dateString + "\"")
        .contains("\"java.time.LocalDate\",\"2020-01-01\"");
  }

  @Test
  public void queryFieldsUsingMbeanDoesNotHonorAnnotations() throws Exception {
    String result = bean.queryData(SELECT_FIELDS, "server", 100);
    System.out.println(result);
    assertThat(result).contains("id").contains("title");
  }

  // this is simply to document the current behavior of gfsh
  @Test
  public void queryAllUsingGfshDoesNotFormatDate() throws Exception {
    gfsh.connectAndVerify(server.getJmxPort(), GfshCommandRule.PortType.jmxManager);
    TabularResultModelAssert tabularAssert =
        gfsh.executeAndAssertThat("query --query='" + SELECT_ALL_BUT_LOCAL_DATE + "'")
            .statusIsSuccess()
            .hasTableSection();
    tabularAssert.hasColumns().asList().containsExactlyInAnyOrder("name", "address", "startDate",
        "endDate", "title");
    tabularAssert.hasColumn("startDate").containsExactly(date.getTime() + "");
    tabularAssert.hasColumn("endDate").containsExactly(sqlDate.getTime() + "");
  }

  // this is simply to document the current behavior of gfsh
  // gfsh refused to format the date objects as of jackson 2.12's fix#2683
  @Test
  public void queryAllUsingGfshRefusesToFormatLocalDate() throws Exception {
    gfsh.connectAndVerify(server.getJmxPort(), GfshCommandRule.PortType.jmxManager);
    gfsh.executeAndAssertThat("query --query='" + SELECT_ALL + "'")
        .statusIsError()
        .containsOutput(
            "Java 8 date/time type `java.time.LocalDate` not supported by default: add Module \"com.fasterxml.jackson.datatype:jackson-datatype-jsr310\"");
  }

  @Test
  public void queryFieldsUsingGfshDoesNotHonorAnnotations() throws Exception {
    gfsh.connectAndVerify(server.getJmxPort(), GfshCommandRule.PortType.jmxManager);
    gfsh.executeAndAssertThat("query --query='" + SELECT_FIELDS + "'")
        .statusIsSuccess()
        .hasTableSection().hasColumns().asList()
        .containsExactlyInAnyOrder("id", "title");
  }

  @Test
  public void documentZeroResultsBehavior() throws Exception {
    String result = bean.queryData("select * from /testRegion r where r.id=0", "server", 100);
    assertThat(result).isEqualTo("{\"result\":[{\"message\":\"No Data Found\"}]}");

    result = bean.queryData("select id, title from /testRegion r where r.id=0", "server", 100);
    assertThat(result).isEqualTo("{\"result\":[{\"message\":\"No Data Found\"}]}");
  }
}
