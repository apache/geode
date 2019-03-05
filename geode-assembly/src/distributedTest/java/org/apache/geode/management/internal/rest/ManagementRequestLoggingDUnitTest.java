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

package org.apache.geode.management.internal.rest;

import static org.assertj.core.api.Assertions.assertThat;

import java.util.List;
import java.util.Map;

import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.core.Appender;
import org.apache.logging.log4j.core.LogEvent;
import org.apache.logging.log4j.core.Logger;
import org.apache.logging.log4j.test.appender.ListAppender;
import org.junit.BeforeClass;
import org.junit.ClassRule;
import org.junit.Test;

import org.apache.geode.cache.configuration.RegionConfig;
import org.apache.geode.management.api.ClusterManagementResult;
import org.apache.geode.test.dunit.rules.ClusterStartupRule;
import org.apache.geode.test.dunit.rules.MemberVM;
import org.apache.geode.test.junit.rules.GeodeDevRestClient;

public class ManagementRequestLoggingDUnitTest {

  @ClassRule
  public static ClusterStartupRule cluster = new ClusterStartupRule();

  private static MemberVM locator, server;

  private static GeodeDevRestClient restClient;

  @BeforeClass
  public static void beforeClass() {
    locator = cluster.startLocatorVM(0, l -> l.withHttpService());
    server = cluster.startServerVM(1, locator.getPort());
    restClient =
        new GeodeDevRestClient("/geode-management/v2", "localhost", locator.getHttpPort(), false);
  }

  @Test
  public void checkRequestsAreLogged() throws Exception {
    locator.invoke(() -> {
      Logger logger = (Logger) LogManager.getLogger(ManagementLoggingFilter.class);
      logger.addAppender(new ListAppender("ListAppender"));
    });

    RegionConfig regionConfig = new RegionConfig();
    regionConfig.setName("customers");
    regionConfig.setType("REPLICATE");
    ObjectMapper mapper = new ObjectMapper();
    String json = mapper.writeValueAsString(regionConfig);

    ClusterManagementResult result =
        restClient.doPostAndAssert("/regions", json)
            .hasStatusCode(201)
            .getClusterManagementResult();

    assertThat(result.isSuccessful()).isTrue();

    locator.invoke(() -> {
      Logger logger = (Logger) LogManager.getLogger(ManagementLoggingFilter.class);
      Map<String, Appender> appenders = logger.getAppenders();
      ListAppender listAppender = (ListAppender) appenders.get("ListAppender");
      List<LogEvent> logEvents = listAppender.getEvents();

      assertThat(logEvents.size()).as("Expected LogEvents").isEqualTo(1);
      assertThat(logEvents.get(0).getMessage().getFormattedMessage())
          .startsWith("Management request:");

      logger.removeAppender(listAppender);
    });
  }
}
