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

import org.junit.BeforeClass;
import org.junit.ClassRule;
import org.junit.Rule;
import org.junit.Test;

import org.apache.geode.cache.Region;
import org.apache.geode.cache.RegionShortcut;
import org.apache.geode.management.DistributedSystemMXBean;
import org.apache.geode.management.internal.json.QueryResultFormatter;
import org.apache.geode.test.junit.assertions.TabularResultModelAssert;
import org.apache.geode.test.junit.rules.GfshCommandRule;
import org.apache.geode.test.junit.rules.MBeanServerConnectionRule;
import org.apache.geode.test.junit.rules.ServerStarterRule;

public class DistributedSystemMBeanIntegrationTest {

  public static final String SELECT = "select * from /testRegion r where r.id=1";

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

  private static Date date;
  private static java.sql.Date sqlDate;
  private static LocalDate localDate;

  @BeforeClass
  public static void setupClass() throws Exception {
    Region<Object, Object> testRegion = server.getCache().getRegion("testRegion");
    localDate = LocalDate.of(2020, 1, 1);
    sqlDate = java.sql.Date.valueOf(localDate);
    date = new Date(sqlDate.getTime());
    Data data = new Data(1, date, sqlDate, localDate);
    testRegion.put(1, data);
  }

  // this is to make sure dates are formatted correctly
  @Test
  public void queryUsingMBean() throws Exception {
    connectionRule.connect(server.getJmxPort());
    SimpleDateFormat formater =
        new SimpleDateFormat(QueryResultFormatter.DATE_FORMAT_PATTERN);
    String dateString = formater.format(date);
    DistributedSystemMXBean bean = connectionRule.getProxyMXBean(DistributedSystemMXBean.class);
    String result = bean.queryData(SELECT, "server", 100);
    System.out.println(result);
    assertThat(result)
        .contains("\"java.util.Date\",\"" + dateString + "\"")
        .contains("\"java.sql.Date\",\"" + dateString + "\"")
        .contains("\"java.time.LocalDate\",\"2020-01-01\"");
  }

  // this is simply to document the current behavior of gfsh
  // gfsh doesn't attempt tp format the date objects as of now
  @Test
  public void queryUsingGfsh() throws Exception {
    gfsh.connectAndVerify(server.getJmxPort(), GfshCommandRule.PortType.jmxManager);
    TabularResultModelAssert tabularAssert =
        gfsh.executeAndAssertThat("query --query='" + SELECT + "'")
            .statusIsSuccess()
            .hasTableSection();
    tabularAssert.hasColumn("id").containsExactly("1");
    tabularAssert.hasColumn("date").containsExactly(date.getTime() + "");
    tabularAssert.hasColumn("sqlDate").containsExactly(sqlDate.getTime() + "");
    tabularAssert.hasColumn("localDate")
        .asList().asString().contains("\"year\":2020,\"month\":\"JANUARY\"");
  }

  public static class Data {
    private int id;
    private Date date;
    private java.sql.Date sqlDate;
    private LocalDate localDate;

    public Data() {}

    public Data(int id, Date date, java.sql.Date sqlDate, LocalDate localDate) {
      this.id = id;
      this.date = date;
      this.sqlDate = sqlDate;
      this.localDate = localDate;
    }

    public int getId() {
      return id;
    }

    public void setId(int id) {
      this.id = id;
    }

    public Date getDate() {
      return date;
    }

    public void setDate(Date date) {
      this.date = date;
    }

    public java.sql.Date getSqlDate() {
      return sqlDate;
    }

    public void setSqlDate(java.sql.Date sqlDate) {
      this.sqlDate = sqlDate;
    }

    public LocalDate getLocalDate() {
      return localDate;
    }

    public void setLocalDate(LocalDate localDate) {
      this.localDate = localDate;
    }
  }
}
