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
package org.apache.geode.internal.cache;

import static org.apache.geode.cache.Region.SEPARATOR;
import static org.apache.geode.distributed.ConfigurationProperties.CACHE_XML_FILE;
import static org.apache.geode.test.dunit.VM.getVM;
import static org.apache.geode.test.util.ResourceUtils.createTempFileFromResource;
import static org.assertj.core.api.Assertions.assertThat;

import java.util.Properties;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import org.apache.geode.cache.Region;
import org.apache.geode.test.dunit.VM;
import org.apache.geode.test.dunit.cache.CacheTestCase;
import org.apache.geode.test.dunit.internal.JUnit4DistributedTestCase;

/**
 * This class tests regions created by xml files
 */
public class PartitionedRegionCacheXMLExampleDUnitTest extends CacheTestCase {

  private static final String CACHE_XML_FILE_1 = "PartitionRegionCacheExample1.xml";
  private static final String PARTITIONED_REGION_1_NAME = "firstPartitionRegion";
  private static final String PARTITIONED_REGION_2_NAME = "secondPartitionedRegion";

  private static final String CACHE_XML_FILE_2 = "PartitionRegionCacheExample2.xml";
  private static final String PARTITIONED_SUBREGION_NAME =
      SEPARATOR + "root" + SEPARATOR + "PartitionedSubRegion";

  private String cacheXmlFileName;

  private VM vm0;
  private VM vm1;

  @Before
  public void setUp() throws Exception {
    vm0 = getVM(0);
    vm1 = getVM(1);
  }

  @After
  public void tearDown() throws Exception {
    disconnectAllFromDS();
  }

  @Test
  public void testExampleWithBothRootRegion() {
    cacheXmlFileName = CACHE_XML_FILE_1;

    vm0.invoke(() -> createCache(cacheXmlFileName));
    vm1.invoke(() -> createCache(cacheXmlFileName));
    vm0.invoke(this::validatePartitionedRegions);

    // Disconnecting and again creating the PR from xml file
    vm1.invoke(JUnit4DistributedTestCase::disconnectFromDS);
    vm1.invoke(() -> createCache(cacheXmlFileName));
    vm1.invoke(this::validatePartitionedRegions);
  }

  @Test
  public void testExampleWithSubRegion() {
    cacheXmlFileName = CACHE_XML_FILE_2;

    vm0.invoke(() -> createCache(cacheXmlFileName));
    vm1.invoke(() -> createCache(cacheXmlFileName));

    vm0.invoke(() -> {
      Region<String, String> region = getCache().getRegion(PARTITIONED_SUBREGION_NAME);
      assertThat(region).isInstanceOf(PartitionedRegion.class);
      assertThat(PartitionedRegionHelper.isSubRegion(region.getFullPath())).isTrue();

      Object value = region.get("1");
      assertThat(value).as("CacheLoader is not invoked").isNotNull();

      region.put("key1", "value1");
      value = region.get("key1");

      assertThat(value).isEqualTo("value1");
    });
  }

  private void createCache(String cacheXmlFileName) {
    Properties config = new Properties();
    String cacheXmlPath =
        createTempFileFromResource(getClass(), cacheXmlFileName).getAbsolutePath();
    config.setProperty(CACHE_XML_FILE, cacheXmlPath);

    getSystem(config);
    getCache();
  }

  private void validatePartitionedRegions() {
    Region<String, String> region1 = getCache().getRegion(PARTITIONED_REGION_1_NAME);
    assertThat(region1).isInstanceOf(PartitionedRegion.class);

    Object value = region1.get("1");
    assertThat(value).as("CacheLoader is not invoked").isNotNull();

    region1.put("key1", "value1");
    value = region1.get("key1");
    assertThat(value).isEqualTo("value1");

    Region<String, String> region2 = getCache().getRegion(PARTITIONED_REGION_2_NAME);
    assertThat(region2).isInstanceOf(PartitionedRegion.class);

    region2.put("key2", "value2");
    value = region2.get("key2");
    assertThat(value).isEqualTo("value2");
  }
}
