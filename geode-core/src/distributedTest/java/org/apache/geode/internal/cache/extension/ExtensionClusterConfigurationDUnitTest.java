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
package org.apache.geode.internal.cache.extension;

import static org.apache.geode.test.dunit.Assert.assertNotNull;
import static org.apache.geode.test.dunit.Assert.assertTrue;
import static org.junit.Assert.assertEquals;

import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;

import org.apache.geode.cache.Cache;
import org.apache.geode.cache.Region;
import org.apache.geode.cache.RegionShortcut;
import org.apache.geode.internal.cache.InternalCache;
import org.apache.geode.internal.cache.extension.mock.MockCacheExtension;
import org.apache.geode.internal.cache.extension.mock.MockExtensionCommands;
import org.apache.geode.internal.cache.extension.mock.MockRegionExtension;
import org.apache.geode.internal.cache.xmlcache.XmlGenerator;
import org.apache.geode.internal.cache.xmlcache.XmlParser;
import org.apache.geode.management.internal.cli.util.CommandStringBuilder;
import org.apache.geode.management.internal.configuration.domain.XmlEntity;
import org.apache.geode.management.internal.i18n.CliStrings;
import org.apache.geode.test.dunit.rules.ClusterStartupRule;
import org.apache.geode.test.dunit.rules.MemberVM;
import org.apache.geode.test.junit.rules.GfshCommandRule;


public class ExtensionClusterConfigurationDUnitTest {

  @Rule
  public ClusterStartupRule clusterStartupRule = new ClusterStartupRule();

  @Rule
  public GfshCommandRule gfsh = new GfshCommandRule();

  private static final String REPLICATE_REGION = "ReplicateRegion1";

  private MemberVM locator;
  private MemberVM dataMember;

  @Before
  public void before() throws Exception {
    locator = clusterStartupRule.startLocatorVM(3);
  }

  /**
   * Tests for {@link Extension}, {@link Extensible}, {@link XmlParser}, {@link XmlGenerator},
   * {@link XmlEntity} as it applies to Extensions. Asserts that Mock Extension is created and
   * altered on region and cache.
   *
   * @since GemFire 8.1
   */
  @Test
  public void testCreateExtensions() throws Exception {

    // create caching member
    dataMember = clusterStartupRule.startServerVM(1, locator.getPort());

    // Connect Gfsh to locator.
    gfsh.connectAndVerify(locator);

    createRegion(REPLICATE_REGION, RegionShortcut.REPLICATE, null);
    createMockRegionExtension(REPLICATE_REGION, "value1");
    alterMockRegionExtension(REPLICATE_REGION, "value2");
    createMockCacheExtension("value1");
    alterMockCacheExtension("value2");

    // Start a new member which receives the shared configuration
    // Verify the config creation on this member
    MemberVM newMember = clusterStartupRule.startServerVM(2, locator.getPort());
    newMember.invoke(() -> {
      InternalCache cache = ClusterStartupRule.getCache();
      assertNotNull(cache);

      Region<?, ?> region1 = cache.getRegion(REPLICATE_REGION);
      assertNotNull(region1);

      // MockRegionExtension verification
      @SuppressWarnings("unchecked")
      // should only be one region extension
      final MockRegionExtension mockRegionExtension =
          (MockRegionExtension) ((Extensible<Region<?, ?>>) region1).getExtensionPoint()
              .getExtensions().iterator().next();
      assertNotNull(mockRegionExtension);
      assertEquals(1, mockRegionExtension.beforeCreateCounter.get());
      assertEquals(1, mockRegionExtension.onCreateCounter.get());
      assertEquals("value2", mockRegionExtension.getValue());

      // MockCacheExtension verification
      // should only be one cache extension
      final MockCacheExtension mockCacheExtension =
          (MockCacheExtension) cache.getExtensionPoint().getExtensions().iterator().next();
      assertNotNull(mockCacheExtension);
      assertEquals(1, mockCacheExtension.beforeCreateCounter.get());
      assertEquals(1, mockCacheExtension.onCreateCounter.get());
      assertEquals("value2", mockCacheExtension.getValue());
    });
  }

  /**
   * Tests for {@link Extension}, {@link Extensible}, {@link XmlParser}, {@link XmlGenerator},
   * {@link XmlEntity} as it applies to Extensions. Asserts that Mock Extension is created and
   * destroyed on region and cache.
   *
   * @since GemFire 8.1
   */
  @Test
  public void testDestroyExtensions() throws Exception {

    // create caching member
    dataMember = clusterStartupRule.startServerVM(1, locator.getPort());

    // Connect Gfsh to locator.
    gfsh.connectAndVerify(locator);

    createRegion(REPLICATE_REGION, RegionShortcut.REPLICATE, null);
    createMockRegionExtension(REPLICATE_REGION, "value1");
    destroyMockRegionExtension(REPLICATE_REGION);
    createMockCacheExtension("value1");
    destroyMockCacheExtension();

    // Start a new member which receives the shared configuration
    // Verify the config creation on this member
    MemberVM newMember = clusterStartupRule.startServerVM(2, locator.getPort());
    newMember.invoke(() -> {
      InternalCache cache = ClusterStartupRule.getCache();
      assertNotNull(cache);

      Region<?, ?> region1 = cache.getRegion(REPLICATE_REGION);
      assertNotNull(region1);

      // MockRegionExtension verification
      @SuppressWarnings("unchecked")
      final Extensible<Region<?, ?>> extensibleRegion = (Extensible<Region<?, ?>>) region1;
      // Should not be any region extensions
      assertTrue(!extensibleRegion.getExtensionPoint().getExtensions().iterator().hasNext());

      // MockCacheExtension verification
      final Extensible<Cache> extensibleCache = cache;
      // Should not be any cache extensions
      assertTrue(!extensibleCache.getExtensionPoint().getExtensions().iterator().hasNext());
    });
  }

  private void createRegion(String regionName, RegionShortcut regionShortCut, String group) {
    CommandStringBuilder csb = new CommandStringBuilder(CliStrings.CREATE_REGION);
    csb.addOption(CliStrings.CREATE_REGION__REGION, regionName);
    csb.addOption(CliStrings.CREATE_REGION__REGIONSHORTCUT, regionShortCut.name());
    csb.addOptionWithValueCheck(CliStrings.GROUP, group);
    gfsh.executeAndAssertThat(csb.toString()).statusIsSuccess();
  }

  private void createMockRegionExtension(final String regionName, final String value) {
    CommandStringBuilder csb =
        new CommandStringBuilder(MockExtensionCommands.CREATE_MOCK_REGION_EXTENSION);
    csb.addOption(MockExtensionCommands.OPTION_REGION_NAME, regionName);
    csb.addOption(MockExtensionCommands.OPTION_VALUE, value);
    gfsh.executeAndAssertThat(csb.toString()).statusIsSuccess();
  }

  private void alterMockRegionExtension(final String regionName, final String value) {
    CommandStringBuilder csb =
        new CommandStringBuilder(MockExtensionCommands.ALTER_MOCK_REGION_EXTENSION);
    csb.addOption(MockExtensionCommands.OPTION_REGION_NAME, regionName);
    csb.addOption(MockExtensionCommands.OPTION_VALUE, value);
    gfsh.executeAndAssertThat(csb.toString()).statusIsSuccess();
  }

  private void destroyMockRegionExtension(final String regionName) {
    CommandStringBuilder csb =
        new CommandStringBuilder(MockExtensionCommands.DESTROY_MOCK_REGION_EXTENSION);
    csb.addOption(MockExtensionCommands.OPTION_REGION_NAME, regionName);
    gfsh.executeAndAssertThat(csb.toString()).statusIsSuccess();
  }

  private void createMockCacheExtension(final String value) {
    CommandStringBuilder csb =
        new CommandStringBuilder(MockExtensionCommands.CREATE_MOCK_CACHE_EXTENSION);
    csb.addOption(MockExtensionCommands.OPTION_VALUE, value);
    gfsh.executeAndAssertThat(csb.toString()).statusIsSuccess();
  }

  private void alterMockCacheExtension(final String value) {
    CommandStringBuilder csb =
        new CommandStringBuilder(MockExtensionCommands.ALTER_MOCK_CACHE_EXTENSION);
    csb.addOption(MockExtensionCommands.OPTION_VALUE, value);
    gfsh.executeAndAssertThat(csb.toString()).statusIsSuccess();
  }

  private void destroyMockCacheExtension() {
    CommandStringBuilder csb =
        new CommandStringBuilder(MockExtensionCommands.DESTROY_MOCK_CACHE_EXTENSION);
    gfsh.executeAndAssertThat(csb.toString()).statusIsSuccess();
  }
}
