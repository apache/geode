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
package org.apache.geode.internal.cache.xmlcache;

import static org.apache.geode.distributed.ConfigurationProperties.OFF_HEAP_MEMORY_SIZE;
import static org.assertj.core.api.Assertions.assertThat;

import java.io.File;
import java.io.FileWriter;
import java.io.PrintWriter;
import java.util.Arrays;
import java.util.Collection;

import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;
import org.junit.rules.TestName;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import org.apache.geode.cache.Cache;
import org.apache.geode.cache.PartitionAttributes;
import org.apache.geode.cache.PartitionAttributesFactory;
import org.apache.geode.cache.Region;
import org.apache.geode.logging.internal.log4j.api.LogService;
import org.apache.geode.services.module.internal.impl.ServiceLoaderModuleService;
import org.apache.geode.test.junit.rules.ServerStarterRule;

@RunWith(Parameterized.class)
public class CacheXmlGeneratorIntegrationTest {

  @Rule
  public TestName testName = new TestName();

  @Rule
  public ServerStarterRule serverStarterRule = new ServerStarterRule();

  @Rule
  public TemporaryFolder temporaryFolder = new TemporaryFolder();

  @Parameterized.Parameter
  public PartitionAttributes partitionAttributes;

  @Parameterized.Parameters
  public static Collection<PartitionAttributes> partitionAttributes() {
    return Arrays.asList(
        new PartitionAttributesFactory().create(),
        new PartitionAttributesFactory().setLocalMaxMemory(16).create());
  }

  @Test
  public void generateXmlForPartitionRegionWithOffHeapWhenDistributedSystemDoesNotExistShouldWorkProperly()
      throws Exception {
    CacheCreation cacheCreation = new CacheCreation(new ServiceLoaderModuleService(
        LogService.getLogger()));
    RegionAttributesCreation attributes = new RegionAttributesCreation(cacheCreation);
    attributes.setOffHeap(true);
    attributes.setPartitionAttributes(partitionAttributes);
    cacheCreation.createVMRegion(testName.getMethodName(), attributes);
    File cacheXmlFile = temporaryFolder.newFile(testName.getMethodName() + ".xml");
    PrintWriter printWriter = new PrintWriter(new FileWriter(cacheXmlFile), true);
    CacheXmlGenerator.generate(cacheCreation, printWriter);

    serverStarterRule.withProperty(OFF_HEAP_MEMORY_SIZE, "32m")
        .withProperty("cache-xml-file", cacheXmlFile.getAbsolutePath()).startServer();

    Cache cache = serverStarterRule.getCache();
    assertThat(cache).isNotNull();
    Region<Object, Object> region = cache.getRegion(testName.getMethodName());
    assertThat(region).isNotNull();
    assertThat(region.getAttributes().getOffHeap()).isTrue();
    PartitionAttributes parsedAttributes = region.getAttributes().getPartitionAttributes();
    assertThat(parsedAttributes.getLocalMaxMemory())
        .isEqualTo(partitionAttributes.getLocalMaxMemory());
    assertThat(parsedAttributes.getTotalNumBuckets())
        .isEqualTo(partitionAttributes.getTotalNumBuckets());
    assertThat(parsedAttributes.getRedundantCopies())
        .isEqualTo(partitionAttributes.getRedundantCopies());
    assertThat(parsedAttributes.getPartitionResolver())
        .isEqualTo(partitionAttributes.getPartitionResolver());
    assertThat(parsedAttributes.getPartitionListeners())
        .isEqualTo(partitionAttributes.getPartitionListeners());
    assertThat(parsedAttributes.getFixedPartitionAttributes())
        .isEqualTo(partitionAttributes.getFixedPartitionAttributes());
  }
}
