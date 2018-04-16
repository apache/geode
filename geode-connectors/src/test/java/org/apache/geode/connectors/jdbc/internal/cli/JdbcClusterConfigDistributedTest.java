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

import static java.util.concurrent.TimeUnit.MINUTES;
import static org.apache.geode.distributed.ConfigurationProperties.LOCATORS;
import static org.apache.geode.test.dunit.Disconnect.disconnectAllFromDS;
import static org.apache.geode.test.dunit.VM.getHostName;
import static org.apache.geode.test.dunit.VM.getVM;
import static org.assertj.core.api.Assertions.assertThat;
import static org.awaitility.Awaitility.await;
import static org.junit.Assert.assertTrue;
import static org.mockito.Mockito.mock;

import java.io.Serializable;
import java.util.Properties;

import org.junit.After;
import org.junit.Before;
import org.junit.ClassRule;
import org.junit.Rule;
import org.junit.Test;
import org.junit.experimental.categories.Category;

import org.apache.geode.cache.CacheFactory;
import org.apache.geode.connectors.jdbc.internal.JdbcConnectorService;
import org.apache.geode.connectors.jdbc.internal.TableMetaDataView;
import org.apache.geode.connectors.jdbc.internal.configuration.ConnectorService;
import org.apache.geode.distributed.Locator;
import org.apache.geode.distributed.internal.InternalLocator;
import org.apache.geode.internal.cache.InternalCache;
import org.apache.geode.management.cli.Result;
import org.apache.geode.test.dunit.VM;
import org.apache.geode.test.dunit.rules.DistributedRestoreSystemProperties;
import org.apache.geode.test.dunit.rules.DistributedTestRule;
import org.apache.geode.test.junit.categories.DistributedTest;
import org.apache.geode.test.junit.rules.serializable.SerializableTemporaryFolder;
import org.apache.geode.test.junit.rules.serializable.SerializableTestName;

@Category(DistributedTest.class)
@SuppressWarnings("serial")
public class JdbcClusterConfigDistributedTest implements Serializable {

  private transient InternalCache cache;
  private transient VM locator;

  private String regionName;
  private String connectionName;
  private String connectionUrl;
  private String tableName;
  private String pdxClass;
  private String locators;
  private String[] fieldMappings;
  private boolean keyInValue;

  @ClassRule
  public static DistributedTestRule distributedTestRule = new DistributedTestRule();

  @Rule
  public DistributedRestoreSystemProperties restoreSystemProperties =
      new DistributedRestoreSystemProperties();

  @Rule
  public SerializableTemporaryFolder temporaryFolder = new SerializableTemporaryFolder();

  @Rule
  public SerializableTestName testName = new SerializableTestName();

  @Before
  public void setUp() {
    regionName = "regionName";
    connectionName = "connection";
    connectionUrl = "url";
    tableName = "testTable";
    pdxClass = "myPdxClass";
    keyInValue = true;
    fieldMappings = new String[] {"field1:column1", "field2:column2"};

    locator = getVM(0);
    String locatorFolder = "vm-" + locator.getId() + "-" + testName.getMethodName();

    int port = locator.invoke(() -> {
      System.setProperty("user.dir", temporaryFolder.newFolder(locatorFolder).getAbsolutePath());
      Properties config = new Properties();
      config.setProperty(LOCATORS, "");
      InternalLocator locator = (InternalLocator) Locator.startLocatorAndDS(0, null, config);
      await().atMost(2, MINUTES).until(() -> assertTrue(locator.isSharedConfigurationRunning()));
      return Locator.getLocator().getPort();
    });
    locators = getHostName() + "[" + port + "]";

    cache = (InternalCache) new CacheFactory().set(LOCATORS, locators).create();

    locator.invoke(() -> {
      CreateConnectionCommand command = new CreateConnectionCommand();
      command.setCache(CacheFactory.getAnyInstance());
      Result result = command.createConnection(connectionName, connectionUrl, null, null, null);
      assertThat(result.getStatus()).isSameAs(Result.Status.OK);
      CreateMappingCommand mappingCommand = new CreateMappingCommand();
      mappingCommand.setCache(CacheFactory.getAnyInstance());
      result = mappingCommand.createMapping(regionName, connectionName, tableName, pdxClass,
          keyInValue, fieldMappings);

      assertThat(result.getStatus()).isSameAs(Result.Status.OK);
    });

    JdbcConnectorService service = cache.getService(JdbcConnectorService.class);
    validateRegionMapping(service.getMappingForRegion(regionName));

    cache.close();
  }

  @After
  public void tearDown() {
    disconnectAllFromDS();
  }

  @Test
  public void recreatesCacheFromClusterConfig() {
    cache = (InternalCache) new CacheFactory().set(LOCATORS, locators).create();

    JdbcConnectorService service = cache.getService(JdbcConnectorService.class);
    assertThat(service.getConnectionConfig(connectionName)).isNotNull();
    validateRegionMapping(service.getMappingForRegion(regionName));
  }

  private void validateRegionMapping(ConnectorService.RegionMapping regionMapping) {
    assertThat(regionMapping).isNotNull();
    assertThat(regionMapping.getRegionName()).isEqualTo(regionName);
    assertThat(regionMapping.getConnectionConfigName()).isEqualTo(connectionName);
    assertThat(regionMapping.getTableName()).isEqualTo(tableName);
    assertThat(regionMapping.getPdxClassName()).isEqualTo(pdxClass);
    assertThat(regionMapping.isPrimaryKeyInValue()).isEqualTo(keyInValue);
    assertThat(regionMapping.getColumnNameForField("field1", mock(TableMetaDataView.class)))
        .isEqualTo("column1");
    assertThat(regionMapping.getColumnNameForField("field2", mock(TableMetaDataView.class)))
        .isEqualTo("column2");
  }

}
