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

import static org.apache.geode.test.dunit.VM.getVM;
import static org.assertj.core.api.Assertions.assertThat;

import java.io.IOException;
import java.io.Serializable;
import java.util.ArrayList;
import java.util.Properties;
import java.util.stream.IntStream;

import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;

import org.apache.geode.cache.Cache;
import org.apache.geode.cache.CacheFactory;
import org.apache.geode.cache.Region;
import org.apache.geode.cache.RegionShortcut;
import org.apache.geode.cache.client.ClientCache;
import org.apache.geode.cache.client.ClientCacheFactory;
import org.apache.geode.cache.client.ClientRegionShortcut;
import org.apache.geode.cache.execute.FunctionAdapter;
import org.apache.geode.cache.execute.FunctionContext;
import org.apache.geode.cache.execute.FunctionException;
import org.apache.geode.cache.execute.FunctionService;
import org.apache.geode.cache.query.FunctionDomainException;
import org.apache.geode.cache.query.NameResolutionException;
import org.apache.geode.cache.query.Query;
import org.apache.geode.cache.query.QueryInvocationTargetException;
import org.apache.geode.cache.query.QueryService;
import org.apache.geode.cache.query.SelectResults;
import org.apache.geode.cache.query.TypeMismatchException;
import org.apache.geode.cache.server.CacheServer;
import org.apache.geode.distributed.internal.DistributionConfig;
import org.apache.geode.test.dunit.SerializableRunnableIF;
import org.apache.geode.test.dunit.VM;
import org.apache.geode.test.dunit.rules.CacheRule;
import org.apache.geode.test.dunit.rules.ClientCacheRule;
import org.apache.geode.test.dunit.rules.DistributedRule;

public class MultiServerPartitionedRegionQueryDUnitTest implements Serializable {

  @Rule
  public DistributedRule distributedRule = new DistributedRule();
  @Rule
  public CacheRule cacheRule = new CacheRule();
  @Rule
  public ClientCacheRule clientCacheRule = new ClientCacheRule();

  private VM server1, server2, client;
  private static String regionName = "region";

  @Before
  public void setup() {
    server1 = getVM(0);
    server2 = getVM(1);
    client = getVM(2);
  }

  @Test
  public void cumulativeResultsToDataShouldWriteToTheCorrectStreamNotCauseCorruption() {
    int numPuts = 10;
    int port = server1.invoke(this::createServerAndRegion);
    server2.invoke((SerializableRunnableIF) this::createServerAndRegion);

    String hostname = server1.getHost().getHostName();
    client.invoke(() -> {
      ClientCacheFactory ccf = new ClientCacheFactory();
      ccf.addPoolServer(hostname, port);
      clientCacheRule.createClientCache(ccf);
      ClientCache clientCache = clientCacheRule.getClientCache();
      Region region =
          clientCache.createClientRegionFactory(ClientRegionShortcut.PROXY).create(regionName);
      IntStream.range(0, numPuts).forEach(id -> region.put("key-" + id, new TestObject(id)));
      ArrayList results = (ArrayList) FunctionService.onRegion(region)
          .execute(new QueryWithoutTurningIntoListFunction(regionName,
              "select distinct r.id, r.name from /" + regionName + " r, /" + regionName
                  + " t where t.id = r.id"))
          .getResult();
      SelectResults rs = (SelectResults) results.get(0);
      assertThat(rs).size().isEqualTo(numPuts);
    });
  }

  @Test
  public void nwayMergeResultsToDataShouldWriteToTheCorrectStreamAndNotCauseCorruption() {
    int numPuts = 10;
    int port = server1.invoke(this::createServerAndRegion);
    server2.invoke((SerializableRunnableIF) this::createServerAndRegion);

    String hostname = server1.getHost().getHostName();
    client.invoke(() -> {
      ClientCacheFactory ccf = new ClientCacheFactory();
      ccf.addPoolServer(hostname, port);
      clientCacheRule.createClientCache(ccf);
      ClientCache clientCache = clientCacheRule.getClientCache();
      Region region =
          clientCache.createClientRegionFactory(ClientRegionShortcut.PROXY).create(regionName);
      IntStream.range(0, numPuts).forEach(id -> region.put("key-" + id, new TestObject(id)));
      ArrayList results = (ArrayList) FunctionService.onRegion(region)
          .execute(new QueryWithoutTurningIntoListFunction(regionName,
              "select distinct r.id, r.name from /" + regionName + " r, /" + regionName
                  + " t where t.id = r.id order by r.id"))
          .getResult();
      SelectResults rs = (SelectResults) results.get(0);
      assertThat(rs).size().isEqualTo(numPuts);
    });
  }

  private int createServerAndRegion() throws IOException {
    Properties props = new Properties();
    props.setProperty(DistributionConfig.VALIDATE_SERIALIZABLE_OBJECTS_NAME, "true");
    props.setProperty(DistributionConfig.SERIALIZABLE_OBJECT_FILTER_NAME, "*");
    cacheRule.createCache(new CacheFactory(props).setPdxReadSerialized(true));
    Cache cache = cacheRule.getCache();
    CacheServer cs = cache.addCacheServer();
    cs.setPort(0);
    cs.start();
    cache.createRegionFactory(RegionShortcut.PARTITION).create(regionName);
    return cs.getPort();
  }

  public static class TestObject implements Serializable {
    public int id;
    public String name;

    public TestObject(int id) {
      this.id = id;
      this.name = "Name:" + id;
    }
  }

  public static class QueryWithoutTurningIntoListFunction extends FunctionAdapter {

    private String regionName;
    private String queryString;

    @Override
    public boolean hasResult() {
      return true;
    }

    @Override
    public boolean isHA() {
      return false;
    }

    public QueryWithoutTurningIntoListFunction(String regionName, String queryString) {
      super();
      this.regionName = regionName;
      this.queryString = queryString;
    }

    @Override
    public void execute(FunctionContext context) {
      QueryService queryService = CacheFactory.getAnyInstance().getQueryService();
      Query query = queryService.newQuery(queryString);
      SelectResults results = null;
      try {
        results = (SelectResults) query.execute(context);
      } catch (FunctionDomainException | TypeMismatchException | NameResolutionException
          | QueryInvocationTargetException e) {
        throw new FunctionException("Exception while executing function", e);
      }
      context.getResultSender().lastResult(results);

    }

    @Override
    public String getId() {
      return this.getClass().getName();
    }
  }

}
