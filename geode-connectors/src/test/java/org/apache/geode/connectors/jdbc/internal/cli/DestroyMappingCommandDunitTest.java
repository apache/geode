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

import static org.apache.geode.connectors.jdbc.internal.cli.DestroyMappingCommand.DESTROY_MAPPING;
import static org.apache.geode.connectors.jdbc.internal.cli.DestroyMappingCommand.DESTROY_MAPPING__REGION_NAME;
import static org.assertj.core.api.Assertions.assertThat;

import java.io.Serializable;

import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.experimental.categories.Category;

import org.apache.geode.connectors.jdbc.internal.InternalJdbcConnectorService;
import org.apache.geode.connectors.jdbc.internal.RegionMapping;
import org.apache.geode.connectors.jdbc.internal.RegionMappingBuilder;
import org.apache.geode.connectors.jdbc.internal.RegionMappingExistsException;
import org.apache.geode.distributed.internal.InternalLocator;
import org.apache.geode.internal.cache.InternalCache;
import org.apache.geode.management.internal.cli.util.CommandStringBuilder;
import org.apache.geode.test.dunit.rules.LocatorServerStartupRule;
import org.apache.geode.test.dunit.rules.MemberVM;
import org.apache.geode.test.junit.categories.DistributedTest;
import org.apache.geode.test.junit.rules.GfshCommandRule;
import org.apache.geode.test.junit.rules.serializable.SerializableTestName;

@Category(DistributedTest.class)
public class DestroyMappingCommandDunitTest implements Serializable {


  @Rule
  public transient GfshCommandRule gfsh = new GfshCommandRule();

  @Rule
  public LocatorServerStartupRule startupRule = new LocatorServerStartupRule();

  @Rule
  public SerializableTestName testName = new SerializableTestName();

  private MemberVM locator;
  private MemberVM server;

  private String regionName;

  @Before
  public void before() throws Exception {
    regionName = "testRegion";

    locator = startupRule.startLocatorVM(0);
    server = startupRule.startServerVM(1, locator.getPort());

    server.invoke(() -> createRegionMapping());

    gfsh.connectAndVerify(locator);
  }

  @Test
  public void destroysRegionMapping() throws Exception {
    CommandStringBuilder csb = new CommandStringBuilder(DESTROY_MAPPING);
    csb.addOption(DESTROY_MAPPING__REGION_NAME, "testRegion");

    gfsh.executeAndAssertThat(csb.toString()).statusIsSuccess();

    locator.invoke(() -> {
      String xml = InternalLocator.getLocator().getSharedConfiguration().getConfiguration("cluster")
          .getCacheXmlContent();
      assertThat(xml).contains("jdbc:connector-service");
    });

    server.invoke(() -> {
      InternalCache cache = LocatorServerStartupRule.getCache();
      RegionMapping mapping =
          cache.getService(InternalJdbcConnectorService.class).getMappingForRegion("testRegion");
      assertThat(mapping).isNull();
    });
  }

  private void createRegionMapping() throws RegionMappingExistsException {
    InternalCache cache = LocatorServerStartupRule.getCache();
    InternalJdbcConnectorService service = cache.getService(InternalJdbcConnectorService.class);

    service.createRegionMapping(new RegionMappingBuilder().withRegionName(regionName).build());

    assertThat(service.getMappingForRegion(regionName)).isNotNull();
  }

}
