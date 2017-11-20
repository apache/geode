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
package org.apache.geode.connectors.jdbc.internal.xml;

import static org.apache.geode.distributed.ConfigurationProperties.CACHE_XML_FILE;
import static org.assertj.core.api.Assertions.assertThat;

import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.io.PrintWriter;

import org.junit.After;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.rules.TemporaryFolder;

import org.apache.geode.cache.CacheFactory;
import org.apache.geode.connectors.jdbc.internal.ConnectionConfigBuilder;
import org.apache.geode.connectors.jdbc.internal.ConnectionConfiguration;
import org.apache.geode.connectors.jdbc.internal.InternalJdbcConnectorService;
import org.apache.geode.connectors.jdbc.internal.JdbcConnectorService;
import org.apache.geode.connectors.jdbc.internal.RegionMapping;
import org.apache.geode.internal.cache.InternalCache;
import org.apache.geode.internal.cache.xmlcache.CacheXmlGenerator;
import org.apache.geode.test.junit.categories.IntegrationTest;

@Category(IntegrationTest.class)
public class JdbcConnectorServiceXmlIntegrationTest {

  private InternalCache cache;
  private File cacheXml;
  private ConnectionConfiguration config1;
  private ConnectionConfiguration config2;
  private RegionMapping regionMapping1;
  private RegionMapping regionMapping2;

  @Rule
  public TemporaryFolder temporaryFolder = new TemporaryFolder();

  @Before
  public void setup() throws Exception {
    cache = (InternalCache) new CacheFactory().create();
    configureService();
    cacheXml = generateXml();
    cache.close();
  }

  @After
  public void tearDown() {
    cache.close();
  }

  @Test
  public void canRecreateJdbcConnectorServiceFromXml() throws Exception {
    cache =
        (InternalCache) new CacheFactory().set(CACHE_XML_FILE, cacheXml.getAbsolutePath()).create();

    JdbcConnectorService service =
        (JdbcConnectorService) cache.getExtensionPoint().getExtensions().iterator().next();
    assertThat(service.getConnectionConfig(config1.getName())).isEqualTo(config1);
    assertThat(service.getConnectionConfig(config2.getName())).isEqualTo(config2);
    assertThat(service.getMappingForRegion(regionMapping1.getRegionName()))
        .isEqualTo(regionMapping1);
    assertThat(service.getMappingForRegion(regionMapping2.getRegionName()))
        .isEqualTo(regionMapping2);
  }

  private void configureService() {
    InternalJdbcConnectorService service = cache.getService(InternalJdbcConnectorService.class);
    config1 = new ConnectionConfigBuilder().withName("connection1").withUrl("url1")
        .withUser("username1").withPassword("secret1")
        .withParameters(new String[] {"param1:value1", "param2:value2"}).build();
    config2 = new ConnectionConfigBuilder().withName("connection2").withUrl("url2")
        .withUser("username2").withPassword("secret2").build();
    service.createConnectionConfig(config1);
    service.createConnectionConfig(config2);

    RegionMappingBuilder regionMappingBuilder1 = new RegionMappingBuilder()
        .withRegionName("regionName1").withPdxClassName("pdxClassName1").withTableName("tableName1")
        .withConnectionConfigName("connection1").withPrimaryKeyInValue("true");
    regionMappingBuilder1.withFieldToColumnMapping("fieldName1", "columnMapping1");
    regionMappingBuilder1.withFieldToColumnMapping("fieldName2", "columnMapping2");
    regionMapping1 = regionMappingBuilder1.build();

    RegionMappingBuilder regionMappingBuilder2 = new RegionMappingBuilder()
        .withRegionName("regionName2").withPdxClassName("pdxClassName2").withTableName("tableName2")
        .withConnectionConfigName("connection2").withPrimaryKeyInValue("false");
    regionMappingBuilder1.withFieldToColumnMapping("fieldName3", "columnMapping3");
    regionMappingBuilder1.withFieldToColumnMapping("fieldName4", "columnMapping4");
    regionMapping2 = regionMappingBuilder2.build();

    service.addOrUpdateRegionMapping(regionMapping1);
    service.addOrUpdateRegionMapping(regionMapping2);
  }

  private File generateXml() throws IOException {
    File cacheXml = new File(temporaryFolder.getRoot(), "cache.xml");
    PrintWriter printWriter = new PrintWriter(new FileWriter(cacheXml));
    CacheXmlGenerator.generate(cache, printWriter, true, false, false);
    printWriter.flush();
    return cacheXml;
  }
}
