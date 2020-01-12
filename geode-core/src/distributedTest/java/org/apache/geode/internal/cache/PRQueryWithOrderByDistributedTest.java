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
import static org.apache.geode.cache.RegionShortcut.PARTITION;
import static org.apache.geode.test.dunit.NetworkUtils.getServerHostName;
import static org.apache.geode.test.dunit.VM.getVM;
import static org.assertj.core.api.Assertions.assertThat;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.io.Serializable;
import java.sql.Time;
import java.sql.Timestamp;
import java.util.Date;

import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.experimental.categories.Category;

import org.apache.geode.DataSerializable;
import org.apache.geode.cache.Region;
import org.apache.geode.cache.client.ClientCacheFactory;
import org.apache.geode.cache.client.ClientRegionShortcut;
import org.apache.geode.cache.query.QueryService;
import org.apache.geode.cache.query.SelectResults;
import org.apache.geode.cache.server.CacheServer;
import org.apache.geode.pdx.JSONFormatter;
import org.apache.geode.test.dunit.VM;
import org.apache.geode.test.dunit.rules.CacheRule;
import org.apache.geode.test.dunit.rules.ClientCacheRule;
import org.apache.geode.test.dunit.rules.DistributedRule;
import org.apache.geode.test.junit.categories.OQLQueryTest;
import org.apache.geode.test.junit.rules.serializable.SerializableTestName;

/**
 * Extracted from {@link PRQueryDistributedTest}.
 */
@Category(OQLQueryTest.class)
@SuppressWarnings("serial")
public class PRQueryWithOrderByDistributedTest implements Serializable {

  private String regionName;
  private String[] queries;

  private VM server1;
  private VM server2;
  private VM client;

  @Rule
  public DistributedRule distributedRule = new DistributedRule();

  @Rule
  public CacheRule cacheRule = new CacheRule();

  @Rule
  public ClientCacheRule clientCacheRule = new ClientCacheRule();

  @Rule
  public SerializableTestName testName = new SerializableTestName();

  @Before
  public void setUp() {
    server1 = getVM(0);
    server2 = getVM(1);
    client = getVM(2);

    regionName = getClass().getSimpleName() + "_" + testName.getMethodName();

    queries = new String[] {
        "select distinct * from " + SEPARATOR + regionName + " order by \"date\"",
        "select distinct \"date\" from " + SEPARATOR + regionName + " order by \"date\"",
        "select distinct * from " + SEPARATOR + regionName + " order by \"time\"",
        "select distinct \"time\" from " + SEPARATOR + regionName + " order by \"time\"",
        "select distinct * from " + SEPARATOR + regionName + " order by \"timestamp\"",
        "select distinct \"timestamp\" from " + SEPARATOR + regionName + " order by \"timestamp\"",
        "select distinct \"date\" from " + SEPARATOR + regionName + " order by \"date\".\"score\"",
        "select distinct * from " + SEPARATOR + regionName + " order by nested.\"date\"",
        "select distinct * from " + SEPARATOR + regionName + " order by nested.\"date\".nonKeyword",
        "select distinct * from " + SEPARATOR + regionName + " order by nested.\"date\".\"date\"",
        "select distinct * from " + SEPARATOR + regionName
            + " order by nested.\"date\".\"date\".score"};

    String jsonCustomer = "{" + "\"firstName\": \"John\"," + "\"lastName\": \"Smith\","
        + " \"age\": 25," + " \"date\":" + " \"" + new Date() + "\"," + " \"time\":" + " \""
        + new Time(1000) + "\"," + " \"timestamp\":" + " \"" + new Timestamp(1000) + "\"" + "}";

    String jsonCustomer1 = "{" + "\"firstName\": \"John1\"," + "\"lastName\": \"Smith1\","
        + " \"age\": 25," + " \"date\":" + " \"" + new Date() + "\"," + " \"time\":" + " \""
        + new Time(1000) + "\"," + " \"timestamp\":" + " \"" + new Timestamp(1000) + "\"" + "}";

    String jsonCustomer2 = "{" + "\"firstName\": \"John2\"," + "\"lastName\": \"Smith2\","
        + " \"age\": 25," + " \"date\":" + " \"" + new Date() + "\"," + " \"time\":" + " \""
        + new Time(1000) + "\"," + " \"timestamp\":" + " \"" + new Timestamp(1000) + "\"" + "}";

    String jsonCustomer3 = "{" + "\"firstName\": \"John3\"," + "\"lastName\": \"Smith3\","
        + " \"age\": 25," + " \"date\":" + " \"" + new TestObject(1) + "\"," + " \"time\":" + " \""
        + new Time(1000) + "\"," + " \"timestamp\":" + " \"" + new Timestamp(1000) + "\"" + "}";

    String jsonCustomer4 = "{" + "\"firstName\": \"John4\"," + "\"lastName\": \"Smith4\","
        + " \"age\": 25," + " \"date\":" + " \"" + new TestObject(1) + "\"," + " \"time\":" + " \""
        + new Time(1000) + "\"," + " \"timestamp\":" + " \"" + new Timestamp(1000) + "\","
        + " \"nested\":" + " \"" + new NestedKeywordObject(1) + "\"" + "}";

    String jsonCustomer5 = "{" + "\"firstName\": \"John5\"," + "\"lastName\": \"Smith5\","
        + " \"age\": 25," + " \"date\":" + " \"" + new TestObject(1) + "\"," + " \"time\":" + " \""
        + new Time(1000) + "\"," + " \"timestamp\":" + " \"" + new Timestamp(1000) + "\","
        + " \"nested\":" + " \""
        + new NestedKeywordObject(new NestedKeywordObject(new TestObject(1))) + "\"" + "}";

    int portForServer1 = server1.invoke("Create Server1", () -> {
      cacheRule.createCache();
      Region region = cacheRule.getCache().createRegionFactory(PARTITION).create(regionName);

      region.put("jsondoc", JSONFormatter.fromJSON(jsonCustomer));
      region.put("jsondoc1", JSONFormatter.fromJSON(jsonCustomer1));
      region.put("jsondoc2", JSONFormatter.fromJSON(jsonCustomer2));
      region.put("jsondoc3", JSONFormatter.fromJSON(jsonCustomer3));
      region.put("jsondoc4", JSONFormatter.fromJSON(jsonCustomer4));
      region.put("jsondoc5", JSONFormatter.fromJSON(jsonCustomer5));

      CacheServer server = cacheRule.getCache().addCacheServer();
      server.setPort(0);
      server.start();
      return server.getPort();
    });

    int portForServer2 = server2.invoke("Create Server2", () -> {
      cacheRule.createCache();
      cacheRule.getCache().createRegionFactory(PARTITION).create(regionName);
      CacheServer server = cacheRule.getCache().addCacheServer();
      server.setPort(0);
      server.start();
      return server.getPort();
    });

    client.invoke("Create client", () -> {
      ClientCacheFactory ccf = new ClientCacheFactory();
      ccf.addPoolServer(getServerHostName(), portForServer1);
      ccf.addPoolServer(getServerHostName(), portForServer2);

      clientCacheRule.createClientCache(ccf);
      clientCacheRule.getClientCache().createClientRegionFactory(ClientRegionShortcut.CACHING_PROXY)
          .create(regionName);
    });
  }

  @Test
  public void testOrderByOnPRWithReservedKeywords() throws Exception {
    client.invoke("Execute queries on client", () -> {
      QueryService queryService = clientCacheRule.getClientCache().getQueryService();

      for (String query : queries) {
        SelectResults results = (SelectResults) queryService.newQuery(query).execute();
        assertThat(results.size())
            .as("Size of result set should be greater than 0 for query: " + query).isGreaterThan(0);
      }
    });
  }

  private static class TestObject implements DataSerializable, Comparable {

    private Double score;

    public TestObject() {
      // nothing
    }

    public TestObject(double score) {
      this.score = score;
    }

    @Override
    public int compareTo(Object o) {
      if (o instanceof TestObject) {
        return score.compareTo(((TestObject) o).score);
      }
      return 1;
    }

    @Override
    public void toData(DataOutput out) throws IOException {
      out.writeDouble(score);
    }

    @Override
    public void fromData(DataInput in) throws IOException, ClassNotFoundException {
      score = in.readDouble();
    }
  }

  @SuppressWarnings("unused")
  private static class NestedKeywordObject implements Serializable {

    private final Object date;

    NestedKeywordObject(Object object) {
      date = object;
    }
  }
}
