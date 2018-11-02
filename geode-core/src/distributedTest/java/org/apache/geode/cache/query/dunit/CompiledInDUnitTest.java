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
package org.apache.geode.cache.query.dunit;

import java.io.IOException;
import java.io.Serializable;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.Properties;
import java.util.stream.IntStream;

import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.junit.experimental.categories.Category;

import org.apache.geode.cache.AttributesFactory;
import org.apache.geode.cache.Cache;
import org.apache.geode.cache.CacheException;
import org.apache.geode.cache.DataPolicy;
import org.apache.geode.cache.PartitionAttributes;
import org.apache.geode.cache.PartitionAttributesFactory;
import org.apache.geode.cache.Region;
import org.apache.geode.cache.Scope;
import org.apache.geode.cache.query.Query;
import org.apache.geode.cache.query.QueryService;
import org.apache.geode.cache.query.SelectResults;
import org.apache.geode.cache.server.CacheServer;
import org.apache.geode.cache30.CacheSerializableRunnable;
import org.apache.geode.distributed.ConfigurationProperties;
import org.apache.geode.test.dunit.Host;
import org.apache.geode.test.dunit.LogWriterUtils;
import org.apache.geode.test.dunit.SerializableRunnable;
import org.apache.geode.test.dunit.VM;
import org.apache.geode.test.dunit.cache.internal.JUnit4CacheTestCase;
import org.apache.geode.test.junit.categories.OQLQueryTest;

@Category({OQLQueryTest.class})
public class CompiledInDUnitTest extends JUnit4CacheTestCase {

  final String rootRegionName = "root";
  final String regionName = "PdxTest";
  final String regName = "/" + rootRegionName + "/" + regionName;
  private Host host;
  private VM vm0;
  private VM vm1;
  private VM client;

  public CompiledInDUnitTest() {}

  @Before
  public void setup() {
    host = Host.getHost(0);
    vm0 = host.getVM(0);
    vm1 = host.getVM(1);
    client = host.getVM(3);
  }

  @After
  public void closeVMs() {
    closeClient(vm0);
    closeClient(vm1);
    closeClient(client);
  }

  @Override
  public Properties getDistributedSystemProperties() {
    Properties properties = super.getDistributedSystemProperties();
    properties.put(ConfigurationProperties.SERIALIZABLE_OBJECT_FILTER,
        "org.apache.geode.cache.query.dunit.**");
    return properties;
  }


  protected void startBridgeServer(int port, boolean notifyBySubscription) throws IOException {

    Cache cache = getCache();
    CacheServer server = cache.addCacheServer();
    server.setPort(port);
    server.start();
    int bridgeServerPort = server.getPort();
  }

  protected void configAndStartBridgeServer() {
    try {
      startBridgeServer(0, false);
    } catch (Exception ex) {
      Assert.fail("While starting CacheServer" + ex);
    }
  }

  private void createPartitionRegion(final boolean isAccessor) {
    AttributesFactory factory = new AttributesFactory();
    PartitionAttributesFactory paf = new PartitionAttributesFactory();
    if (isAccessor) {
      paf.setLocalMaxMemory(0);
    }
    PartitionAttributes prAttr = paf.setTotalNumBuckets(20).setRedundantCopies(0).create();
    factory.setPartitionAttributes(prAttr);
    createRegion(this.regionName, this.rootRegionName, factory.create());
  }


  private void createReplicateRegion() {
    AttributesFactory factory = new AttributesFactory();
    factory.setScope(Scope.DISTRIBUTED_ACK);
    factory.setDataPolicy(DataPolicy.REPLICATE);

    createRegion(this.regionName, this.rootRegionName, factory.create());
  }

  public void closeClient(VM client) {
    SerializableRunnable closeCache = new CacheSerializableRunnable("Close Client") {
      public void run2() throws CacheException {
        LogWriterUtils.getLogWriter().info("### Close Client. ###");
        try {
          closeCache();
          disconnectFromDS();
        } catch (Exception ex) {
          LogWriterUtils.getLogWriter().info("### Failed to get close client. ###");
        }
      }
    };

    client.invoke(closeCache);
  }


  @Test
  public void whenMultipleEnumBindParametersAreUsedWithInQueryAndMapIndexIsPresentReturnCorrectResults()
      throws CacheException {
    final int numberOfEntries = 10;
    final int numExpectedResults = numberOfEntries / 2;
    final String queryString =
        "select * from " + regName + " where getMapField['1'] in SET ($1,$2)";

    vm0.invoke(new CacheSerializableRunnable("Create cache server") {
      public void run2() throws CacheException {
        configAndStartBridgeServer();
        createReplicateRegion();
        createIndex("myIndex", "ts.getMapField[*]", regName + " ts");
        createEntries(numberOfEntries, regionName);
      }
    });

    Object[] bindArguments = new Object[] {DayEnum.MONDAY, DayEnum.TUESDAY};
    vm1.invoke(executeQueryWithIndexOnReplicateRegion(numExpectedResults, queryString,
        bindArguments, "myIndex", "ts.getMapField[*]", regName + " ts"));
  }

  @Test
  public void whenMultipleEnumBindParametersAreUsedWithInQueryReturnCorrectResults()
      throws CacheException {
    final Host host = Host.getHost(0);
    VM vm0 = host.getVM(0);
    VM vm1 = host.getVM(1);
    VM client = host.getVM(3);
    final int numberOfEntries = 10;
    final int numExpectedResults = numberOfEntries / 2;
    final String queryString =
        "select * from " + regName + " where getMapField['1'] in SET ($1,$2)";

    vm0.invoke(new CacheSerializableRunnable("Create cache server") {
      public void run2() throws CacheException {
        configAndStartBridgeServer();
        createReplicateRegion();
        createEntries(numberOfEntries, regionName);
      }
    });

    Object[] bindArguments = new Object[] {DayEnum.MONDAY, DayEnum.TUESDAY};
    vm1.invoke(executeQueryOnReplicateRegion(numExpectedResults, queryString, bindArguments));
  }

  @Test
  public void whenASingleEnumBindParameterIsUsedWithInQueryAndMapIndexIsPresentReturnCorrectResults()
      throws CacheException {
    final int numberOfEntries = 10;
    final int numExpectedResults = numberOfEntries / 2;
    final String queryString = "select * from " + regName + " where getMapField['1'] in SET ($1)";

    vm0.invoke(new CacheSerializableRunnable("Create cache server") {
      public void run2() throws CacheException {
        configAndStartBridgeServer();
        createReplicateRegion();
        createIndex("myIndex", "ts.getMapField[*]", regName + " ts");
        createEntries(numberOfEntries, regionName);
      }
    });

    Object[] bindArguments = new Object[] {DayEnum.MONDAY};
    vm1.invoke(executeQueryWithIndexOnReplicateRegion(numExpectedResults, queryString,
        bindArguments, "myIndex", "ts.getMapField[*]", regName + " ts"));
  }

  @Test
  public void whenASingleEnumBindParameterIsUsedWithInQueryReturnCorrectResults()
      throws CacheException {
    final int numberOfEntries = 10;
    final int numExpectedResults = numberOfEntries / 2;
    final String queryString = "select * from " + regName + " where getMapField['1'] in SET ($1)";

    vm0.invoke(new CacheSerializableRunnable("Create cache server") {
      public void run2() throws CacheException {
        configAndStartBridgeServer();
        createReplicateRegion();
        createEntries(numberOfEntries, regionName);
      }
    });

    Object[] bindArguments = new Object[] {DayEnum.MONDAY};
    vm1.invoke(executeQueryOnReplicateRegion(numExpectedResults, queryString, bindArguments));
  }

  @Test
  public void whenMultipleTypeBindParameterIsUsedWithInQueryAndMapIndexIsPresentReturnCorrectResults()
      throws CacheException {
    final int numberOfEntries = 10;
    final int numExpectedResults = numberOfEntries / 2;

    final String queryString =
        "select * from " + regName + " where getMapField['1'] in SET ($1,$2,$3)";

    vm0.invoke(new CacheSerializableRunnable("Create cache server") {
      public void run2() throws CacheException {
        configAndStartBridgeServer();
        createReplicateRegion();
        createIndex("myIndex", "ts.getMapField[*]", regName + " ts");
        createEntries(numberOfEntries, regionName);
      }
    });

    Object[] bindArguments = new Object[] {2, DayEnum.MONDAY, "Tuesday"};
    vm1.invoke(executeQueryWithIndexOnReplicateRegion(numExpectedResults, queryString,
        bindArguments, "myIndex", "ts.getMapField[*]", regName + " ts"));
  }

  @Test
  public void whenMultipleEnumBindParametersAreUsedWithInQueryAndMapIndexIsPresentInPartitionRegionReturnCorrectResults()
      throws CacheException {
    final int numberOfEntries = 10;
    final int numExpectedResults = numberOfEntries / 2;
    final String queryString =
        "select * from " + regName + " where getMapField['1'] in SET ($1,$2)";

    vm0.invoke(new CacheSerializableRunnable("Create cache server") {
      public void run2() throws CacheException {
        configAndStartBridgeServer();
        createPartitionRegion(false);
        createIndex("myIndex", "ts.getMapField[*]", regName + " ts");
        createEntries(numberOfEntries, regionName);
      }
    });

    Object[] bindArguments = new Object[] {DayEnum.MONDAY, DayEnum.TUESDAY};
    vm1.invoke(executeQueryOnPartitionRegion(numExpectedResults, queryString, bindArguments));
  }

  @Test
  public void whenMultipleEnumBindParametersAreUsedWithInQueryInPartitionRegionReturnCorrectResults()
      throws CacheException {
    final int numberOfEntries = 10;
    final int numExpectedResults = numberOfEntries / 2;
    final String queryString =
        "select * from " + regName + " where getMapField['1'] in SET ($1,$2)";

    vm0.invoke(new CacheSerializableRunnable("Create cache server") {
      public void run2() throws CacheException {
        configAndStartBridgeServer();
        createPartitionRegion(false);
        createEntries(numberOfEntries, regionName);
      }
    });

    Object[] bindArguments = new Object[] {DayEnum.MONDAY, DayEnum.TUESDAY};
    vm1.invoke(executeQueryOnPartitionRegion(numExpectedResults, queryString, bindArguments));
  }

  @Test
  public void whenASingleEnumBindParameterIsUsedWithInQueryAndMapIndexIsPresentInPartitionRegionReturnCorrectResults()
      throws CacheException {
    final int numberOfEntries = 10;
    final int numExpectedResults = numberOfEntries / 2;
    final String queryString = "select * from " + regName + " where getMapField['1'] in SET ($1)";

    vm0.invoke(new CacheSerializableRunnable("Create cache server") {
      public void run2() throws CacheException {
        configAndStartBridgeServer();
        createPartitionRegion(false);
        createIndex("myIndex", "ts.getMapField[*]", regName + " ts");
        createEntries(numberOfEntries, regionName);
      }
    });

    Object[] bindArguments = new Object[] {DayEnum.MONDAY};
    vm1.invoke(executeQueryOnPartitionRegion(numExpectedResults, queryString, bindArguments));
  }

  @Test
  public void whenASingleEnumBindParameterIsUsedWithInQueryInPartitionRegionReturnCorrectResults()
      throws CacheException {
    final int numberOfEntries = 10;
    final int numExpectedResults = numberOfEntries / 2;
    final String queryString = "select * from " + regName + " where getMapField['1'] in SET ($1)";

    vm0.invoke(new CacheSerializableRunnable("Create cache server") {
      public void run2() throws CacheException {
        configAndStartBridgeServer();
        createPartitionRegion(false);
        createEntries(numberOfEntries, regionName);
      }
    });

    Object[] bindArguments = new Object[] {DayEnum.MONDAY};
    vm1.invoke(executeQueryOnPartitionRegion(numExpectedResults, queryString, bindArguments));
  }

  @Test
  public void whenMultipleTypeBindParameterIsUsedWithInQueryAndMapIndexIsPresentInPartitionRegionReturnCorrectResults()
      throws CacheException {
    final int numberOfEntries = 10;
    final int numExpectedResults = numberOfEntries / 2;

    final String queryString =
        "select * from " + regName + " where getMapField['1'] in SET ($1,$2,$3)";

    vm0.invoke(new CacheSerializableRunnable("Create cache server") {
      public void run2() throws CacheException {
        configAndStartBridgeServer();
        createPartitionRegion(false);
        createIndex("myIndex", "ts.getMapField[*]", regName + " ts");
        createEntries(numberOfEntries, regionName);
      }
    });

    Object[] bindArguments = new Object[] {2, DayEnum.MONDAY, "Tuesday"};
    vm1.invoke(executeQueryOnPartitionRegion(numExpectedResults, queryString, bindArguments));
  }

  @Test
  public void whenEnumBindArgumentIsMatchedInSetWithIteratingFieldShouldReturnResults()
      throws CacheException {
    final int numberOfEntries = 10;
    final int numExpectedResults = numberOfEntries / 2;

    final String queryString = "select * from " + regName + " where $1 in SET (getMapField['1'])";

    vm0.invoke(new CacheSerializableRunnable("Create cache server") {
      public void run2() throws CacheException {
        configAndStartBridgeServer();
        createReplicateRegion();
        createIndex("myIndex", "ts.getMapField[*]", regName + " ts");
        createEntries(numberOfEntries, regionName);
      }
    });

    Object[] bindArguments = new Object[] {DayEnum.MONDAY};
    vm1.invoke(executeQueryWithIndexOnReplicateRegion(numExpectedResults, queryString,
        bindArguments, "myIndex", "ts.getMapField[*]", regName + " ts"));
  }

  @Test
  public void whenEnumBindArgumentIsMatchedInSetWithMultipleIteratingFieldShouldReturnResults()
      throws CacheException {
    final int numberOfEntries = 10;
    final int numExpectedResults = numberOfEntries / 2;

    final String queryString =
        "select * from " + regName + " where $1 in SET (getMapField['1'], getMapField['0'])";

    vm0.invoke(new CacheSerializableRunnable("Create cache server") {
      public void run2() throws CacheException {
        configAndStartBridgeServer();
        createReplicateRegion();
        createIndex("myIndex", "ts.getMapField[*]", regName + " ts");
        createEntries(numberOfEntries, regionName);
      }
    });

    Object[] bindArguments = new Object[] {DayEnum.TUESDAY};
    vm1.invoke(executeQueryWithIndexOnReplicateRegion(numExpectedResults, queryString,
        bindArguments, "myIndex", "ts.getMapField[*]", regName + " ts"));
  }

  @Test
  public void whenEnumBindArgumentIsMatchedInSetWithMultiTypedIteratingFieldShouldReturnResults()
      throws CacheException {
    final int numberOfEntries = 10;
    final int numExpectedResults = numberOfEntries;

    final String queryString = "select * from " + regName
        + " where getMapField['1'] in SET (getMapField['1'], getMapField['2'], 'asdfasdf', getMapField['0'])";

    vm0.invoke(new CacheSerializableRunnable("Create cache server") {
      public void run2() throws CacheException {
        configAndStartBridgeServer();
        createReplicateRegion();
        createIndex("myIndex", "ts.getMapField[*]", regName + " ts");
        createEntries(numberOfEntries, regionName);
      }
    });

    Object[] bindArguments = new Object[] {DayEnum.TUESDAY};
    vm1.invoke(executeQueryWithIndexOnReplicateRegion(numExpectedResults, queryString,
        bindArguments, "myIndex", "ts.getMapField[*]", regName + " ts"));
  }

  @Test
  public void whenANDConditionWithInSetFiltersOutAllFieldsReturnNoResults() throws CacheException {
    final int numberOfEntries = 10;
    final int numExpectedResults = 0;

    final String queryString = "select * from " + regName
        + " where getMapField['1'] in SET ($1, $2) AND getMapField['1'] in SET($3)";

    vm0.invoke(new CacheSerializableRunnable("Create cache server") {
      public void run2() throws CacheException {
        configAndStartBridgeServer();
        createReplicateRegion();
        createIndex("myIndex", "ts.getMapField[*]", regName + " ts");
        createEntries(numberOfEntries, regionName);
      }
    });

    Object[] bindArguments = new Object[] {DayEnum.MONDAY, DayEnum.TUESDAY, DayEnum.WEDNESDAY};
    vm1.invoke(executeQueryWithIndexOnReplicateRegion(numExpectedResults, queryString,
        bindArguments, "myIndex", "ts.getMapField[*]", regName + " ts"));
  }

  @Test
  public void whenANDConditionWithInSetMatchesReturnResults() throws CacheException {
    final int numberOfEntries = 10;
    final int numExpectedResults = 5;

    final String queryString = "select * from " + regName
        + " where getMapField['1'] in SET ($1, $2) AND getMapField['2'] in SET($3)";

    vm0.invoke(new CacheSerializableRunnable("Create cache server") {
      public void run2() throws CacheException {
        configAndStartBridgeServer();
        createReplicateRegion();
        createIndex("myIndex", "ts.getMapField[*]", regName + " ts");
        HashMap entries = new HashMap();
        IntStream.range(0, 10).forEach(i -> {
          MapTestObject object = new MapTestObject(i);
          object.getMapField().put("2", DayEnum.WEDNESDAY);
          entries.put("key" + i, object);
        });
        createEntries(regionName, entries.entrySet().iterator());
      }
    });

    Object[] bindArguments = new Object[] {DayEnum.MONDAY, DayEnum.TUESDAY, DayEnum.WEDNESDAY};
    vm1.invoke(executeQueryWithIndexOnReplicateRegion(numExpectedResults, queryString,
        bindArguments, "myIndex", "ts.getMapField[*]", regName + " ts"));
  }

  @Test
  public void whenInSetCollectionContainsNonUniqueValuesMatchingSetShouldNotBeDuplicated()
      throws CacheException {
    final int numberOfEntries = 10;
    final int numExpectedResults = numberOfEntries / 2;

    final String queryString =
        "select * from " + regName + " where getMapField['1'] in SET($1, $1, $1)";

    vm0.invoke(new CacheSerializableRunnable("Create cache server") {
      public void run2() throws CacheException {
        configAndStartBridgeServer();
        createReplicateRegion();
        createIndex("myIndex", "ts.getMapField[*]", regName + " ts");
        createEntries(numberOfEntries, regionName);
      }
    });

    Object[] bindArguments = new Object[] {DayEnum.MONDAY, DayEnum.TUESDAY, DayEnum.WEDNESDAY};
    vm1.invoke(executeQueryWithIndexOnReplicateRegion(numExpectedResults, queryString,
        bindArguments, "myIndex", "ts.getMapField[*]", regName + " ts"));
  }

  @Test
  public void whenORConditionWithInSetMatchesReturnResults() throws CacheException {
    final int numberOfEntries = 10;
    final int numExpectedResults = numberOfEntries;

    final String queryString = "select * from " + regName
        + " where getMapField['1'] in SET ($1) OR getMapField['0'] in SET($2)";

    vm0.invoke(new CacheSerializableRunnable("Create cache server") {
      public void run2() throws CacheException {
        configAndStartBridgeServer();
        createReplicateRegion();
        createIndex("myIndex", "ts.getMapField[*]", regName + " ts");
        createEntries(numberOfEntries, regionName);
      }
    });

    Object[] bindArguments = new Object[] {DayEnum.MONDAY, DayEnum.TUESDAY};
    vm1.invoke(executeQueryWithIndexOnReplicateRegion(numExpectedResults, queryString,
        bindArguments, "myIndex", "ts.getMapField[*]", regName + " ts"));
  }

  @Test
  public void whenUsingAccessorMultipleEnumBindParametersAreUsedWithInQueryAndMapIndexIsPresentInPartitionRegionReturnCorrectResults()
      throws CacheException {
    final int numberOfEntries = 10;
    final int numExpectedResults = numberOfEntries / 2;
    final String queryString =
        "select * from " + regName + " where getMapField['1'] in SET ($1,$2)";

    vm0.invoke(new CacheSerializableRunnable("Create cache server") {
      public void run2() throws CacheException {
        configAndStartBridgeServer();
        createPartitionRegion(false);
        createIndex("myIndex", "ts.getMapField[*]", regName + " ts");
        createEntries(numberOfEntries, regionName);
      }
    });

    Object[] bindArguments = new Object[] {DayEnum.MONDAY, DayEnum.TUESDAY};
    vm1.invoke(executeQueryWithAccessor(numExpectedResults, queryString, bindArguments));
  }

  @Test
  public void whenUsingAccessorMultipleEnumBindParametersAreUsedWithInQueryInPartitionRegionReturnCorrectResults()
      throws CacheException {
    final int numberOfEntries = 10;
    final int numExpectedResults = numberOfEntries / 2;
    final String queryString =
        "select * from " + regName + " where getMapField['1'] in SET ($1,$2)";

    vm0.invoke(new CacheSerializableRunnable("Create cache server") {
      public void run2() throws CacheException {
        configAndStartBridgeServer();
        createPartitionRegion(false);
        createEntries(numberOfEntries, regionName);
      }
    });

    Object[] bindArguments = new Object[] {DayEnum.MONDAY, DayEnum.TUESDAY};
    vm1.invoke(executeQueryWithAccessor(numExpectedResults, queryString, bindArguments));
  }

  @Test
  public void whenUsingAccessorASingleEnumBindParameterIsUsedWithInQueryAndMapIndexIsPresentInPartitionRegionReturnCorrectResults()
      throws CacheException {
    final int numberOfEntries = 10;
    final int numExpectedResults = numberOfEntries / 2;
    final String queryString = "select * from " + regName + " where getMapField['1'] in SET ($1)";

    vm0.invoke(new CacheSerializableRunnable("Create cache server") {
      public void run2() throws CacheException {
        configAndStartBridgeServer();
        createPartitionRegion(false);
        createIndex("myIndex", "ts.getMapField[*]", regName + " ts");
        createEntries(numberOfEntries, regionName);
      }
    });

    Object[] bindArguments = new Object[] {DayEnum.MONDAY};
    vm1.invoke(executeQueryWithAccessor(numExpectedResults, queryString, bindArguments));
  }

  @Test
  public void whenUsingAccessorASingleEnumBindParameterIsUsedWithInQueryInPartitionRegionReturnCorrectResults()
      throws CacheException {
    final int numberOfEntries = 10;
    final int numExpectedResults = numberOfEntries / 2;
    final String queryString = "select * from " + regName + " where getMapField['1'] in SET ($1)";

    vm0.invoke(new CacheSerializableRunnable("Create cache server") {
      public void run2() throws CacheException {
        configAndStartBridgeServer();
        createPartitionRegion(false);
        createEntries(numberOfEntries, regionName);
      }
    });

    Object[] bindArguments = new Object[] {DayEnum.MONDAY};
    vm1.invoke(executeQueryWithAccessor(numExpectedResults, queryString, bindArguments));
  }

  @Test
  public void whenUsingAccessorMultipleTypeBindParameterIsUsedWithInQueryAndMapIndexIsPresentInPartitionRegionReturnCorrectResults()
      throws CacheException {
    final int numberOfEntries = 10;
    final int numExpectedResults = numberOfEntries / 2;

    final String queryString =
        "select * from " + regName + " where getMapField['1'] in SET ($1,$2,$3)";

    vm0.invoke(new CacheSerializableRunnable("Create cache server") {
      public void run2() throws CacheException {
        configAndStartBridgeServer();
        createPartitionRegion(false);
        createIndex("myIndex", "ts.getMapField[*]", regName + " ts");
        createEntries(numberOfEntries, regionName);
      }
    });

    Object[] bindArguments = new Object[] {2, DayEnum.MONDAY, "Tuesday"};
    vm1.invoke(executeQueryWithAccessor(numExpectedResults, queryString, bindArguments));
  }


  CacheSerializableRunnable executeQueryOnReplicateRegion(final int numberOfEntries,
      final String queryString, Object[] bindArguments) {
    return new CacheSerializableRunnable("Execute Query in Replicated Region") {
      public void run2() throws CacheException {
        configAndStartBridgeServer();
        createReplicateRegion();
        verifyQuery(numberOfEntries, queryString, bindArguments);
      }
    };
  }

  CacheSerializableRunnable executeQueryWithIndexOnReplicateRegion(final int numberOfEntries,
      final String queryString, Object[] bindArguments, String indexName, String indexExpression,
      String regionPath) {
    return new CacheSerializableRunnable("Execute Query with Index in Replicated Region") {
      public void run2() throws CacheException {
        configAndStartBridgeServer();
        createReplicateRegion();
        createIndex(indexName, indexExpression, regionPath);
        verifyQuery(numberOfEntries, queryString, bindArguments);
      }
    };
  }


  CacheSerializableRunnable executeQueryOnPartitionRegion(final int numberOfEntries,
      final String queryString, Object[] bindArguments) {
    return new CacheSerializableRunnable("Execute Query in Partition Regions") {
      public void run2() throws CacheException {
        configAndStartBridgeServer();
        createPartitionRegion(false);
        verifyQuery(numberOfEntries, queryString, bindArguments);
      }
    };
  }

  CacheSerializableRunnable executeQueryWithAccessor(final int numberOfEntries,
      final String queryString, Object[] bindArguments) {
    return new CacheSerializableRunnable("Execute Query with Accessor") {
      public void run2() throws CacheException {
        configAndStartBridgeServer();
        createPartitionRegion(true);
        verifyQuery(numberOfEntries, queryString, bindArguments);
      }
    };
  }


  void verifyQuery(final int numExpectedEntries, final String queryString, Object[] bindArguments) {
    QueryService qs = getCache().getQueryService();
    Query query = null;
    SelectResults sr = null;
    try {
      query = qs.newQuery(queryString);

      sr = (SelectResults) query.execute(bindArguments);
      Iterator iterator = sr.iterator();

    } catch (Exception ex) {
      ex.printStackTrace();

      Assert.fail("Failed to execute query, " + ex.getMessage());
    }
    Assert.assertEquals(numExpectedEntries, sr.size());
  }

  void createIndex(String indexName, String indexExpression, String regionPath) {
    QueryService localQueryService = getCache().getQueryService();
    try {
      localQueryService.createIndex(indexName, indexExpression, regionPath);
    } catch (Exception ex) {
      Assert.fail("Failed to create index." + ex.getMessage());
    }
  }

  void createEntries(final int numberOfEntries, String regionName) {
    Region region = getRootRegion().getSubregion(regionName);
    for (int i = 0; i < numberOfEntries; i++) {
      region.put("key" + i, new MapTestObject(i));
    }
  }

  void createEntries(String regionName, Iterator<Map.Entry> objects) {
    Region region = getRootRegion().getSubregion(regionName);
    objects.forEachRemaining((mk) -> {
      region.put(mk.getKey(), mk.getValue());
    });
  }

  enum DayEnum {
    MONDAY, TUESDAY, WEDNESDAY
  }// Map objects should be either [1] = DayEnum.Monday or [0] = DayEnum.Tuesday

  static class MapTestObject implements Serializable {

    private HashMap mapField = new HashMap<Object, Object>();

    public MapTestObject(int i) {
      int n = i % 2;
      DayEnum enumVal = DayEnum.MONDAY;
      if (n == 0) {
        enumVal = DayEnum.TUESDAY;
      }

      mapField.put("" + n, enumVal);
    }

    public Map<String, DayEnum> getMapField() {
      return mapField;
    }
  }
}
