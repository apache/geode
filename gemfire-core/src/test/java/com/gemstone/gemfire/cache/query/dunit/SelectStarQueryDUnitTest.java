/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.gemstone.gemfire.cache.query.dunit;

import java.io.Serializable;

import com.gemstone.gemfire.cache.Region;
import com.gemstone.gemfire.cache.RegionShortcut;
import com.gemstone.gemfire.cache.client.ClientCache;
import com.gemstone.gemfire.cache.client.ClientCacheFactory;
import com.gemstone.gemfire.cache.client.ClientRegionShortcut;
import com.gemstone.gemfire.cache.query.CacheUtils;
import com.gemstone.gemfire.cache.query.QueryService;
import com.gemstone.gemfire.cache.query.SelectResults;
import com.gemstone.gemfire.cache.query.data.PortfolioPdx;
import com.gemstone.gemfire.cache.query.data.PositionPdx;
import com.gemstone.gemfire.cache.query.internal.CompiledValue;
import com.gemstone.gemfire.cache.query.internal.QueryObserver;
import com.gemstone.gemfire.cache.query.internal.QueryObserverAdapter;
import com.gemstone.gemfire.cache.query.internal.QueryObserverHolder;
import com.gemstone.gemfire.cache.query.internal.StructImpl;
import com.gemstone.gemfire.cache.server.CacheServer;
import com.gemstone.gemfire.cache30.CacheTestCase;
import com.gemstone.gemfire.internal.AvailablePortHelper;
import com.gemstone.gemfire.internal.cache.GemFireCacheImpl;
import com.gemstone.gemfire.internal.cache.VMCachedDeserializable;
import com.gemstone.gemfire.pdx.PdxInstance;
import com.gemstone.gemfire.test.dunit.Assert;
import com.gemstone.gemfire.test.dunit.Host;
import com.gemstone.gemfire.test.dunit.LogWriterUtils;
import com.gemstone.gemfire.test.dunit.NetworkUtils;
import com.gemstone.gemfire.test.dunit.SerializableCallable;
import com.gemstone.gemfire.test.dunit.VM;

/**
 * Test for #44807 to eliminate unnecessary serialization/deserialization in
 * select * queries
 * 
 * @author Tejas Nomulwar
 * 
 */
public class SelectStarQueryDUnitTest extends CacheTestCase {
  public SelectStarQueryDUnitTest(String name) {
    super(name);
  }

  private final String regName = "exampleRegion";
  private final String regName2 = "exampleRegion2";

  private final String[] queries = { "SELECT * FROM /" + regName,// 0
      "SELECT * FROM /" + regName + " limit 5",// 1
      "SELECT p from  /" + regName + " p",// 2
      "SELECT count(*) FROM /" + regName,// 3
      "SELECT ALL * from /" + regName,// 4
      "SELECT * from /" + regName + ".values",// 5
      "SELECT distinct * FROM /" + regName,// 6
      "SELECT distinct * FROM /" + regName + " p order by p.ID",// 7
      "SELECT * from /" + regName + " r, positions.values pos"// 8
  };
  private final int[] resultSize = { 20, 5, 20, 1, 20, 20, 20, 20, 40 };

  private final String[] multipleRegionQueries = {
      " SELECT * FROM /" + regName + ", /" + regName2,
      "SELECT * FROM /" + regName + ", /" + regName2 + " limit 5",
      "SELECT distinct * FROM /" + regName + ", /" + regName2,
      "SELECT distinct * FROM /" + regName + " p1, /" + regName2
          + " p2 order by p1.ID",
      "SELECT count(*) FROM /" + regName + ", /" + regName2,
      "SELECT p, q from  /" + regName + " p, /" + regName2 + " q",
      "SELECT ALL * from /" + regName + " p, /" + regName2 + " q",
      "SELECT * from /" + regName + ".values" + ", /" + regName2 + ".values",
      "SELECT * from /" + regName + " p, p.positions.values pos" + ", /"
          + regName2 + " q, q.positions.values pos", };
  private final int[] resultSize2 = { 400, 5, 400, 400, 1, 400, 400, 400, 1600 };

  public void testSelectStarQueryForPartitionedRegion() throws Exception {
    final Host host = Host.getHost(0);
    final VM server1 = host.getVM(0);
    final VM server2 = host.getVM(1);
    final VM server3 = host.getVM(2);
    final VM client = host.getVM(3);
    PortfolioPdx[] portfolios = new PortfolioPdx[10];
    for (int i = 0; i < portfolios.length; i++) {
      portfolios[i] = new PortfolioPdx(i);
    }

    // create servers and regions
    final int port1 = startPartitionedCacheServer(server1, portfolios);
    final int port2 = startPartitionedCacheServer(server2, portfolios);
    final int port3 = startPartitionedCacheServer(server3, portfolios);

    final QueryObserver oldObserver = (QueryObserver) server1
        .invoke(new SerializableCallable("Set observer") {
          @Override
          public Object call() throws Exception {
            QueryObserver observer = QueryObserverHolder
                .setInstance(new QueryResultTrackingObserver());
            return observer;
          }
        });

    // create client
    client.invoke(new SerializableCallable("Create client") {
      @Override
      public Object call() throws Exception {
        ClientCacheFactory cf = new ClientCacheFactory();
        cf.addPoolServer(NetworkUtils.getServerHostName(server1.getHost()), port1);
        cf.addPoolServer(NetworkUtils.getServerHostName(server2.getHost()), port2);
        cf.addPoolServer(NetworkUtils.getServerHostName(server3.getHost()), port3);
        ClientCache cache = getClientCache(cf);
        cache.createClientRegionFactory(ClientRegionShortcut.CACHING_PROXY)
            .create(regName);
        return null;
      }
    });

    // put serialized PortfolioPdx objects
    client.invoke(new SerializableCallable("Put objects") {
      @Override
      public Object call() throws Exception {
        Region r1 = getRootRegion(regName);
        for (int i = 10; i < 20; i++) {
          r1.put("key-" + i, new PortfolioPdx(i));
        }
        return null;
      }
    });

    // query remotely from client
    client.invoke(new SerializableCallable("Query") {
      @Override
      public Object call() throws Exception {
        LogWriterUtils.getLogWriter().info("Querying remotely from client");
        QueryService localQS = null;
        QueryService remoteQS = null;
        try {
          localQS = ((ClientCache) getCache()).getLocalQueryService();
          remoteQS = ((ClientCache) getCache()).getQueryService();
        } catch (Exception e) {
          Assert.fail("Exception getting query service ", e);
        }
        SelectResults[][] sr = new SelectResults[1][2];
        SelectResults res = null;
        for (int i = 0; i < queries.length; i++) {
          try {
            res = (SelectResults) localQS.newQuery(queries[i]).execute();
            sr[0][0] = res;
            res = (SelectResults) remoteQS.newQuery(queries[i]).execute();
            sr[0][1] = res;
            CacheUtils.compareResultsOfWithAndWithoutIndex(sr);
          } catch (Exception e) {
            Assert.fail("Error executing query: " + queries[i], e);
          }
          assertEquals(resultSize[i], res.size());
          if (i == 3) {
            int cnt = ((Integer) res.iterator().next());
            assertEquals(20, cnt);
          } else {
            for (Object rs : res) {
              if (rs instanceof StructImpl) {
                for (Object obj : ((StructImpl) rs).getFieldValues()) {
                  if (obj instanceof PortfolioPdx || obj instanceof PositionPdx) {
                  } else {
                    fail("Result objects for remote client query: "
                        + queries[i]
                        + " should be instance of PortfolioPdx and not "
                        + obj.getClass());
                  }
                }
              } else if (rs instanceof PortfolioPdx) {
              } else {
                fail("Result objects for remote client query: " + queries[i]
                    + " should be instance of PortfolioPdx and not "
                    + rs.getClass());
              }
            }
          }
        }
        return null;
      }
    });

    // verify if objects iterated by query are serialized
    server1.invoke(new SerializableCallable("Get observer") {
      @Override
      public Object call() throws Exception {
        QueryObserver observer = QueryObserverHolder.getInstance();
        assertTrue(QueryObserverHolder.hasObserver());
        assertTrue(observer instanceof QueryResultTrackingObserver);
        QueryResultTrackingObserver resultObserver = (QueryResultTrackingObserver) observer;
        assertTrue(resultObserver.isObjectSerialized());
        return null;
      }
    });

    // verify if objects returned by local server query are not serialized
    server1.invoke(new SerializableCallable("Query") {
      @Override
      public Object call() throws Exception {
        QueryObserver observer = QueryObserverHolder
            .setInstance(new QueryResultTrackingObserver());
        QueryService qs = null;
        try {
          qs = getCache().getQueryService();
        } catch (Exception e) {
          Assert.fail("Exception getting query service ", e);
        }
        SelectResults res = null;
        for (int i = 0; i < queries.length; i++) {
          try {
            res = (SelectResults) qs.newQuery(queries[i]).execute();
          } catch (Exception e) {
            Assert.fail("Error executing query: " + queries[i], e);
          }
          assertEquals(resultSize[i], res.size());
          if (i == 3) {
            int cnt = ((Integer) res.iterator().next());
            assertEquals(20, cnt);
          } else {
            for (Object rs : res) {
              if (rs instanceof StructImpl) {
                for (Object obj : ((StructImpl) rs).getFieldValues()) {
                  if (obj instanceof PortfolioPdx || obj instanceof PositionPdx) {
                  } else {
                    fail("Result objects for remote client query: "
                        + queries[i]
                        + " should be instance of PortfolioPdx and not "
                        + obj.getClass());
                  }
                }
              } else if (rs instanceof PortfolioPdx) {
              } else {
                fail("Result objects for remote client query: " + queries[i]
                    + " should be instance of PortfolioPdx and not "
                    + rs.getClass());
              }
            }
          }
        }
        observer = QueryObserverHolder.getInstance();
        assertTrue(QueryObserverHolder.hasObserver());
        assertTrue(observer instanceof QueryResultTrackingObserver);
        QueryResultTrackingObserver resultObserver = (QueryResultTrackingObserver) observer;
        assertFalse(resultObserver.isObjectSerialized());
        // reset observer
        QueryObserverHolder.setInstance(oldObserver);
        return null;
      }
    });

    closeCache(client);
    closeCache(server1);
    closeCache(server2);
    closeCache(server3);
  }

  public void testSelectStarQueryForReplicatedRegion() throws Exception {
    final Host host = Host.getHost(0);
    final VM server1 = host.getVM(1);
    final VM client = host.getVM(3);
    // create servers and regions
    final int port1 = startReplicatedCacheServer(server1);

    final QueryObserver oldObserver = (QueryObserver) server1
        .invoke(new SerializableCallable("Set observer") {
          @Override
          public Object call() throws Exception {
            QueryObserver observer = QueryObserverHolder
                .setInstance(new QueryResultTrackingObserver());
            return observer;
          }
        });

    // create client
    client.invoke(new SerializableCallable("Create client") {
      @Override
      public Object call() throws Exception {
        ClientCacheFactory cf = new ClientCacheFactory();
        cf.addPoolServer(NetworkUtils.getServerHostName(server1.getHost()), port1);
        ClientCache cache = getClientCache(cf);
        cache.createClientRegionFactory(ClientRegionShortcut.CACHING_PROXY)
            .create(regName);
        cache.createClientRegionFactory(ClientRegionShortcut.CACHING_PROXY)
            .create(regName2);
        return null;
      }
    });

    // put serialized PortfolioPdx objects
    client.invoke(new SerializableCallable("Put objects") {
      @Override
      public Object call() throws Exception {
        Region r1 = getRootRegion(regName);
        Region r2 = getRootRegion(regName2);
        for (int i = 10; i < 20; i++) {
          r1.put("key-" + i, new PortfolioPdx(i));
          r2.put("key-" + i, new PortfolioPdx(i));
        }
        return null;
      }
    });

    // query remotely from client
    client.invoke(new SerializableCallable("Query") {
      @Override
      public Object call() throws Exception {
        LogWriterUtils.getLogWriter().info("Querying remotely from client");
        QueryService localQS = null;
        QueryService remoteQS = null;
        try {
          localQS = ((ClientCache) getCache()).getLocalQueryService();
          remoteQS = ((ClientCache) getCache()).getQueryService();
        } catch (Exception e) {
          Assert.fail("Exception getting query service ", e);
        }
        SelectResults res = null;
        SelectResults[][] sr = new SelectResults[1][2];

        for (int i = 0; i < multipleRegionQueries.length; i++) {
          try {
            res = (SelectResults) localQS.newQuery(multipleRegionQueries[i])
                .execute();
            sr[0][0] = res;
            res = (SelectResults) remoteQS.newQuery(multipleRegionQueries[i])
                .execute();
            sr[0][1] = res;
            CacheUtils.compareResultsOfWithAndWithoutIndex(sr);
          } catch (Exception e) {
            Assert.fail("Error executing query: " + multipleRegionQueries[i], e);
          }
          assertEquals(resultSize2[i], res.size());
          if (i == 4) {
            int cnt = ((Integer) res.iterator().next());
            assertEquals(400, cnt);

          } else {
            for (Object rs : res) {
              if (rs instanceof StructImpl) {
                for (Object obj : ((StructImpl) rs).getFieldValues()) {
                  if (obj instanceof PortfolioPdx || obj instanceof PositionPdx) {
                  } else {
                    fail("Result objects for remote client query: "
                        + multipleRegionQueries[i]
                        + " should be instance of PortfolioPdx and not "
                        + obj.getClass());
                  }
                }
              } else if (rs instanceof PortfolioPdx) {
              } else {
                fail("Result objects for remote client query: "
                    + multipleRegionQueries[i]
                    + " should be instance of PortfolioPdx and not "
                    + rs.getClass());
              }
            }
          }

        }
        return null;
      }
    });

    // verify if objects iterated by query are serialized
    server1.invoke(new SerializableCallable("Get observer") {
      @Override
      public Object call() throws Exception {
        QueryObserver observer = QueryObserverHolder.getInstance();
        assertTrue(QueryObserverHolder.hasObserver());
        assertTrue(observer instanceof QueryResultTrackingObserver);
        QueryResultTrackingObserver resultObserver = (QueryResultTrackingObserver) observer;
        assertTrue(resultObserver.isObjectSerialized());
        return null;
      }
    });

    // verify if objects returned by local server query are not serialized
    server1.invoke(new SerializableCallable("Query") {
      @Override
      public Object call() throws Exception {
        QueryObserver observer = QueryObserverHolder
            .setInstance(new QueryResultTrackingObserver());
        QueryService qs = null;
        try {
          qs = getCache().getQueryService();
        } catch (Exception e) {
          Assert.fail("Exception getting query service ", e);
        }
        SelectResults res = null;
        for (int i = 0; i < multipleRegionQueries.length; i++) {
          try {
            res = (SelectResults) qs.newQuery(multipleRegionQueries[i])
                .execute();
          } catch (Exception e) {
            Assert.fail("Error executing query: " + multipleRegionQueries[i], e);
          }
          assertEquals(resultSize2[i], res.size());
          if (i == 4) {
            int cnt = ((Integer) res.iterator().next());
            assertEquals(400, cnt);

          } else {
            for (Object rs : res) {
              if (rs instanceof StructImpl) {
                for (Object obj : ((StructImpl) rs).getFieldValues()) {
                  if (obj instanceof PortfolioPdx || obj instanceof PositionPdx) {
                  } else {
                    fail("Result objects for remote client query: "
                        + multipleRegionQueries[i]
                        + " should be instance of PortfolioPdx and not "
                        + obj.getClass());
                  }
                }
              } else if (rs instanceof PortfolioPdx) {
              } else {
                fail("Result objects for remote client query: "
                    + multipleRegionQueries[i]
                    + " should be instance of PortfolioPdx and not "
                    + rs.getClass());
              }
            }

          }
        }
        observer = QueryObserverHolder.getInstance();
        assertTrue(QueryObserverHolder.hasObserver());
        assertTrue(observer instanceof QueryResultTrackingObserver);
        QueryResultTrackingObserver resultObserver = (QueryResultTrackingObserver) observer;
        assertFalse(resultObserver.isObjectSerialized());
        // reset observer
        QueryObserverHolder.setInstance(oldObserver);
        return null;
      }
    });

    closeCache(client);
    closeCache(server1);
  }

  public void testByteArrayReplicatedRegion() throws Exception {
    final Host host = Host.getHost(0);
    final VM server1 = host.getVM(0);
    final VM client = host.getVM(3);
    final byte[] ba = new byte[] { 1, 2, 3, 4, 5 };

    // create servers and regions
    final int port = (Integer) server1.invoke(new SerializableCallable(
        "Create Server1") {
      @Override
      public Object call() throws Exception {
        Region r1 = getCache().createRegionFactory(RegionShortcut.REPLICATE)
            .create(regName);
        // put domain objects
        for (int i = 0; i < 10; i++) {
          r1.put("key-" + i, ba);
        }
        CacheServer server = getCache().addCacheServer();
        int port = AvailablePortHelper.getRandomAvailablePortForDUnitSite();
        server.setPort(port);
        server.start();
        return port;
      }
    });

    final QueryObserver oldObserver = (QueryObserver) server1
        .invoke(new SerializableCallable("Set observer") {
          @Override
          public Object call() throws Exception {
            QueryObserver observer = QueryObserverHolder
                .setInstance(new QueryResultTrackingObserver());
            return observer;
          }
        });

    // create client
    client.invoke(new SerializableCallable("Create client") {
      @Override
      public Object call() throws Exception {
        ClientCacheFactory cf = new ClientCacheFactory();
        cf.addPoolServer(NetworkUtils.getServerHostName(server1.getHost()), port);
        ClientCache cache = getClientCache(cf);
        cache.createClientRegionFactory(ClientRegionShortcut.CACHING_PROXY)
            .create(regName);
        return null;
      }
    });

    // put serialized PortfolioPdx objects
    client.invoke(new SerializableCallable("Put objects") {
      @Override
      public Object call() throws Exception {
        Region r1 = getRootRegion(regName);
        for (int i = 10; i < 20; i++) {
          r1.put("key-" + i, ba);
        }
        return null;
      }
    });

    // query remotely from client
    client.invoke(new SerializableCallable("Query") {
      @Override
      public Object call() throws Exception {
        LogWriterUtils.getLogWriter().info("Querying remotely from client");
        QueryService localQS = null;
        QueryService remoteQS = null;
        try {
          localQS = ((ClientCache) getCache()).getLocalQueryService();
          remoteQS = ((ClientCache) getCache()).getQueryService();
        } catch (Exception e) {
          Assert.fail("Exception getting query service ", e);
        }
        SelectResults res = null;
        SelectResults[][] sr = new SelectResults[1][2];

        for (int i = 0; i < 6; i++) {
          try {
            res = (SelectResults) localQS.newQuery(queries[i]).execute();
            sr[0][0] = res;
            res = (SelectResults) remoteQS.newQuery(queries[i]).execute();
            sr[0][1] = res;
            CacheUtils.compareResultsOfWithAndWithoutIndex(sr);
          } catch (Exception e) {
            Assert.fail("Error executing query: " + queries[i], e);
          }
          assertEquals(resultSize[i], res.size());
          if (i == 3) {
            int cnt = ((Integer) res.iterator().next());
            assertEquals(20, cnt);

          } else {
            for (Object o : res) {
              if (o instanceof byte[]) {
                int j = 0;
                for (byte b : ((byte[]) o)) {
                  if (b != ba[j++]) {
                    fail("Bytes in byte array are different when queried from client");
                  }
                }
              } else {
                fail("Result objects for remote client query: " + queries[i]
                    + " should be instance of PortfolioPdx and not "
                    + o.getClass());
              }
            }
          }
        }
        return null;
      }
    });

    // verify if objects iterated by query are serialized
    server1.invoke(new SerializableCallable("Get observer") {
      @Override
      public Object call() throws Exception {
        QueryObserver observer = QueryObserverHolder.getInstance();
        assertTrue(QueryObserverHolder.hasObserver());
        assertTrue(observer instanceof QueryResultTrackingObserver);
        QueryResultTrackingObserver resultObserver = (QueryResultTrackingObserver) observer;
        assertFalse(resultObserver.isObjectSerialized());
        return null;
      }
    });

    // verify if objects returned by local server query are not serialized
    server1.invoke(new SerializableCallable("Query") {
      @Override
      public Object call() throws Exception {
        QueryObserver observer = QueryObserverHolder
            .setInstance(new QueryResultTrackingObserver());
        QueryService qs = null;
        try {
          qs = getCache().getQueryService();
        } catch (Exception e) {
          Assert.fail("Exception getting query service ", e);
        }
        SelectResults res = null;
        for (int i = 0; i < 6; i++) {
          try {
            res = (SelectResults) qs.newQuery(queries[i]).execute();
          } catch (Exception e) {
            Assert.fail("Error executing query: " + queries[i], e);
          }
          assertEquals(resultSize[i], res.size());
          if (i == 3) {
            int cnt = ((Integer) res.iterator().next());
            assertEquals(20, cnt);

          } else {
            for (Object o : res) {
              if (o instanceof byte[]) {
                int j = 0;
                for (byte b : ((byte[]) o)) {
                  if (b != ba[j++]) {
                    fail("Bytes in byte array are different when queried locally on server");
                  }
                }
              } else {
                fail("Result objects for server local query: " + queries[i]
                    + " should be instance of byte array and not "
                    + o.getClass());
              }
            }
          }
        }
        observer = QueryObserverHolder.getInstance();
        assertTrue(QueryObserverHolder.hasObserver());
        assertTrue(observer instanceof QueryResultTrackingObserver);
        QueryResultTrackingObserver resultObserver = (QueryResultTrackingObserver) observer;
        assertFalse(resultObserver.isObjectSerialized());
        // reset observer
        QueryObserverHolder.setInstance(oldObserver);
        return null;
      }
    });

    closeCache(client);
    closeCache(server1);
  }

  public void testByteArrayPartitionedRegion() throws Exception {
    final Host host = Host.getHost(0);
    final VM server1 = host.getVM(0);
    final VM client = host.getVM(3);
    final VM server2 = host.getVM(1);
    final VM server3 = host.getVM(2);
    final byte[][] objs = new byte[10][5];

    for (int i = 0; i < objs.length; i++) {
      objs[i] = new byte[] { 1, 2, 3, 4, 5 };
    }

    // create servers and regions
    final int port1 = startPartitionedCacheServer(server1, objs);
    final int port2 = startPartitionedCacheServer(server2, objs);
    final int port3 = startPartitionedCacheServer(server3, objs);

    final QueryObserver oldObserver = (QueryObserver) server1
        .invoke(new SerializableCallable("Set observer") {
          @Override
          public Object call() throws Exception {
            QueryObserver observer = QueryObserverHolder
                .setInstance(new QueryResultTrackingObserver());
            return observer;
          }
        });

    // create client
    client.invoke(new SerializableCallable("Create client") {
      @Override
      public Object call() throws Exception {
        ClientCacheFactory cf = new ClientCacheFactory();
        cf.addPoolServer(NetworkUtils.getServerHostName(server1.getHost()), port1);
        cf.addPoolServer(NetworkUtils.getServerHostName(server2.getHost()), port2);
        cf.addPoolServer(NetworkUtils.getServerHostName(server3.getHost()), port3);
        ClientCache cache = getClientCache(cf);
        cache.createClientRegionFactory(ClientRegionShortcut.CACHING_PROXY)
            .create(regName);
        return null;
      }
    });

    // put serialized PortfolioPdx objects
    client.invoke(new SerializableCallable("Put objects") {
      @Override
      public Object call() throws Exception {
        Region r1 = getRootRegion(regName);
        for (int i = 10; i < 20; i++) {
          r1.put("key-" + i, new byte[] { 1, 2, 3, 4, 5 });
        }
        return null;
      }
    });

    // query remotely from client
    client.invoke(new SerializableCallable("Query") {
      @Override
      public Object call() throws Exception {
        LogWriterUtils.getLogWriter().info("Querying remotely from client");
        QueryService localQS = null;
        QueryService remoteQS = null;
        try {
          localQS = ((ClientCache) getCache()).getLocalQueryService();
          remoteQS = ((ClientCache) getCache()).getQueryService();
        } catch (Exception e) {
          Assert.fail("Exception getting query service ", e);
        }
        SelectResults res = null;
        SelectResults[][] sr = new SelectResults[1][2];

        for (int i = 0; i < 6; i++) {
          try {
            res = (SelectResults) localQS.newQuery(queries[i]).execute();
            sr[0][0] = res;
            res = (SelectResults) remoteQS.newQuery(queries[i]).execute();
            sr[0][1] = res;
            CacheUtils.compareResultsOfWithAndWithoutIndex(sr);
          } catch (Exception e) {
            Assert.fail("Error executing query: " + queries[i], e);
          }
          assertEquals(resultSize[i], res.size());
          if (i == 3) {
            int cnt = ((Integer) res.iterator().next());
            assertEquals(20, cnt);

          } else {
            for (Object o : res) {
              if (o instanceof byte[]) {
                int j = 0;
                for (byte b : ((byte[]) o)) {
                  if (b != objs[0][j++]) {
                    fail("Bytes in byte array are different when queried from client");
                  }
                }
              } else {
                fail("Result objects for remote client query: " + queries[i]
                    + " should be instance of PortfolioPdx and not "
                    + o.getClass());
              }
            }
          }
        }
        return null;
      }
    });

    // verify if objects iterated by query are serialized
    server1.invoke(new SerializableCallable("Get observer") {
      @Override
      public Object call() throws Exception {
        QueryObserver observer = QueryObserverHolder.getInstance();
        assertTrue(QueryObserverHolder.hasObserver());
        assertTrue(observer instanceof QueryResultTrackingObserver);
        QueryResultTrackingObserver resultObserver = (QueryResultTrackingObserver) observer;
        assertFalse(resultObserver.isObjectSerialized());
        return null;
      }
    });

    // verify if objects returned by local server query are not serialized
    server1.invoke(new SerializableCallable("Query") {
      @Override
      public Object call() throws Exception {
        QueryObserver observer = QueryObserverHolder
            .setInstance(new QueryResultTrackingObserver());
        QueryService qs = null;
        try {
          qs = getCache().getQueryService();
        } catch (Exception e) {
          Assert.fail("Exception getting query service ", e);
        }
        SelectResults res = null;
        for (int i = 0; i < 6; i++) {
          try {
            res = (SelectResults) qs.newQuery(queries[i]).execute();
          } catch (Exception e) {
            Assert.fail("Error executing query: " + queries[i], e);
          }
          assertEquals(resultSize[i], res.size());
          if (i == 3) {
            int cnt = ((Integer) res.iterator().next());
            assertEquals(20, cnt);

          } else {
            for (Object o : res) {
              if (o instanceof byte[]) {
                int j = 0;
                for (byte b : ((byte[]) o)) {
                  if (b != objs[0][j++]) {
                    fail("Bytes in byte array are different when queried locally on server");
                  }
                }
              } else {
                fail("Result objects for server local query: " + queries[i]
                    + " should be instance of byte array and not "
                    + o.getClass());
              }
            }
          }

        }
        observer = QueryObserverHolder.getInstance();
        assertTrue(QueryObserverHolder.hasObserver());
        assertTrue(observer instanceof QueryResultTrackingObserver);
        QueryResultTrackingObserver resultObserver = (QueryResultTrackingObserver) observer;
        assertFalse(resultObserver.isObjectSerialized());
        // reset observer
        QueryObserverHolder.setInstance(oldObserver);
        return null;
      }
    });

    closeCache(client);
    closeCache(server1);

  }

  public void testSelectStarQueryForIndexes() throws Exception {
    final Host host = Host.getHost(0);
    final VM server1 = host.getVM(0);
    final VM client = host.getVM(3);
    // create servers and regions
    final int port1 = startReplicatedCacheServer(server1);
    // create client
    client.invoke(new SerializableCallable("Create client") {
      @Override
      public Object call() throws Exception {
        ClientCacheFactory cf = new ClientCacheFactory();
        cf.addPoolServer(NetworkUtils.getServerHostName(server1.getHost()), port1);
        ClientCache cache = getClientCache(cf);
        cache.createClientRegionFactory(ClientRegionShortcut.CACHING_PROXY)
            .create(regName);
        cache.createClientRegionFactory(ClientRegionShortcut.CACHING_PROXY)
            .create(regName2);
        return null;
      }
    });

    // put serialized PortfolioPdx objects
    client.invoke(new SerializableCallable("Put objects") {
      @Override
      public Object call() throws Exception {
        Region r1 = getRootRegion(regName);
        Region r2 = getRootRegion(regName2);
        for (int i = 10; i < 20; i++) {
          r1.put("key-" + i, new PortfolioPdx(i));
          r2.put("key-" + i, new PortfolioPdx(i));
        }
        return null;
      }
    });

    // create index on exampleRegion1
    server1.invoke(new SerializableCallable("Query") {
      @Override
      public Object call() throws Exception {
        QueryService qs = null;
        try {
          qs = getCache().getQueryService();
          qs.createIndex("status", "status", "/" + regName);
        } catch (Exception e) {
          Assert.fail("Exception getting query service ", e);
        }

        return null;
      }
    });

    // query remotely from client
    client.invoke(new SerializableCallable("Query") {
      @Override
      public Object call() throws Exception {
        LogWriterUtils.getLogWriter().info("Querying remotely from client");
        QueryService localQS = null;
        QueryService remoteQS = null;
        try {
          localQS = ((ClientCache) getCache()).getLocalQueryService();
          remoteQS = ((ClientCache) getCache()).getQueryService();
        } catch (Exception e) {
          Assert.fail("Exception getting query service ", e);
        }
        SelectResults res = null;
        SelectResults[][] sr = new SelectResults[1][2];

        for (int i = 0; i < multipleRegionQueries.length; i++) {
          try {
            res = (SelectResults) localQS.newQuery(multipleRegionQueries[i])
                .execute();
            sr[0][0] = res;
            res = (SelectResults) remoteQS.newQuery(multipleRegionQueries[i])
                .execute();
            sr[0][1] = res;
            CacheUtils.compareResultsOfWithAndWithoutIndex(sr);
          } catch (Exception e) {
            Assert.fail("Error executing query: " + multipleRegionQueries[i], e);
          }
          assertEquals(resultSize2[i], res.size());
          if (i == 4) {
            int cnt = ((Integer) res.iterator().next());
            assertEquals(400, cnt);

          } else {
            for (Object rs : res) {
              if (rs instanceof StructImpl) {
                for (Object obj : ((StructImpl) rs).getFieldValues()) {
                  if (obj instanceof PortfolioPdx || obj instanceof PositionPdx) {
                  } else {
                    fail("Result objects for remote client query: "
                        + multipleRegionQueries[i]
                        + " should be instance of PortfolioPdx and not "
                        + obj.getClass());
                  }
                }
              } else if (rs instanceof PortfolioPdx) {
              } else {
                fail("Result objects for remote client query: "
                    + multipleRegionQueries[i]
                    + " should be instance of PortfolioPdx and not "
                    + rs.getClass());
              }
            }
          }

        }
        return null;
      }
    });

    // create index on exampleRegion2
    server1.invoke(new SerializableCallable("Query") {
      @Override
      public Object call() throws Exception {
        QueryService qs = null;
        try {
          qs = getCache().getQueryService();
          qs.createIndex("status", "status", "/" + regName2);
        } catch (Exception e) {
          Assert.fail("Exception getting query service ", e);
        }

        return null;
      }
    });

    // query remotely from client
    client.invoke(new SerializableCallable("Query") {
      @Override
      public Object call() throws Exception {
        LogWriterUtils.getLogWriter().info("Querying remotely from client");
        QueryService localQS = null;
        QueryService remoteQS = null;
        try {
          localQS = ((ClientCache) getCache()).getLocalQueryService();
          remoteQS = ((ClientCache) getCache()).getQueryService();
        } catch (Exception e) {
          Assert.fail("Exception getting query service ", e);
        }
        SelectResults res = null;
        SelectResults[][] sr = new SelectResults[1][2];

        for (int i = 0; i < multipleRegionQueries.length; i++) {
          try {
            res = (SelectResults) localQS.newQuery(multipleRegionQueries[i])
                .execute();
            sr[0][0] = res;
            res = (SelectResults) remoteQS.newQuery(multipleRegionQueries[i])
                .execute();
            sr[0][1] = res;
            CacheUtils.compareResultsOfWithAndWithoutIndex(sr);
          } catch (Exception e) {
            Assert.fail("Error executing query: " + multipleRegionQueries[i], e);
          }
          assertEquals(resultSize2[i], res.size());
          if (i == 4) {
            int cnt = ((Integer) res.iterator().next());
            assertEquals(400, cnt);

          } else {
            for (Object rs : res) {
              if (rs instanceof StructImpl) {
                for (Object obj : ((StructImpl) rs).getFieldValues()) {
                  if (obj instanceof PortfolioPdx || obj instanceof PositionPdx) {
                  } else {
                    fail("Result objects for remote client query: "
                        + multipleRegionQueries[i]
                        + " should be instance of PortfolioPdx and not "
                        + obj.getClass());
                  }
                }
              } else if (rs instanceof PortfolioPdx) {
              } else {
                fail("Result objects for remote client query: "
                    + multipleRegionQueries[i]
                    + " should be instance of PortfolioPdx and not "
                    + rs.getClass());
              }
            }
          }

        }
        return null;
      }
    });

    closeCache(client);
    closeCache(server1);
  }

  public void testSelectStarQueryForPdxObjects() throws Exception {
    final Host host = Host.getHost(0);
    final VM server1 = host.getVM(0);
    final VM client = host.getVM(3);
    // create servers and regions
    final int port1 = startReplicatedCacheServer(server1);

    final QueryObserver oldObserver = (QueryObserver) server1
        .invoke(new SerializableCallable("Set observer") {
          @Override
          public Object call() throws Exception {
            QueryObserver observer = QueryObserverHolder
                .setInstance(new QueryResultTrackingObserver());
            return observer;
          }
        });

    // create client
    client.invoke(new SerializableCallable("Create client") {
      @Override
      public Object call() throws Exception {
        ClientCacheFactory cf = new ClientCacheFactory();
        cf.addPoolServer(NetworkUtils.getServerHostName(server1.getHost()), port1);
        ClientCache cache = getClientCache(cf);
        cache.createClientRegionFactory(ClientRegionShortcut.CACHING_PROXY)
            .create(regName);
        return null;
      }
    });

    // Update with serialized PortfolioPdx objects
    client.invoke(new SerializableCallable("Put objects") {
      @Override
      public Object call() throws Exception {
        Region r1 = getRootRegion(regName);
        for (int i = 0; i < 20; i++) {
          r1.put("key-" + i, new PortfolioPdx(i));
        }
        return null;
      }
    });

    // query remotely from client
    client.invoke(new SerializableCallable("Query") {
      @Override
      public Object call() throws Exception {
        LogWriterUtils.getLogWriter().info("Querying remotely from client");
        QueryService localQS = null;
        QueryService remoteQS = null;
        try {
          localQS = ((ClientCache) getCache()).getLocalQueryService();
          remoteQS = ((ClientCache) getCache()).getQueryService();
        } catch (Exception e) {
          Assert.fail("Exception getting query service ", e);
        }
        SelectResults res = null;
        SelectResults[][] sr = new SelectResults[1][2];

        for (int i = 0; i < queries.length; i++) {
          try {
            res = (SelectResults) localQS.newQuery(queries[i]).execute();
            sr[0][0] = res;
            res = (SelectResults) remoteQS.newQuery(queries[i]).execute();
            sr[0][1] = res;
            CacheUtils.compareResultsOfWithAndWithoutIndex(sr);
          } catch (Exception e) {
            Assert.fail("Error executing query: " + queries[i], e);
          }
          assertEquals(resultSize[i], res.size());
          if (i == 3) {
            int cnt = ((Integer) res.iterator().next());
            assertEquals(20, cnt);

          } else {
            for (Object rs : res) {
              if (rs instanceof StructImpl) {
                for (Object obj : ((StructImpl) rs).getFieldValues()) {
                  if (obj instanceof PortfolioPdx || obj instanceof PositionPdx) {
                  } else {
                    fail("Result objects for remote client query: "
                        + queries[i]
                        + " should be instance of PortfolioPdx and not "
                        + obj.getClass());
                  }
                }
              } else if (rs instanceof PortfolioPdx) {
              } else {
                fail("Result objects for remote client query: " + queries[i]
                    + " should be instance of PortfolioPdx and not "
                    + rs.getClass());
              }
            }
          }
        }
        return null;
      }
    });

    // verify if objects iterated by query are serialized
    server1.invoke(new SerializableCallable("Get observer") {
      @Override
      public Object call() throws Exception {
        QueryObserver observer = QueryObserverHolder.getInstance();
        assertTrue(QueryObserverHolder.hasObserver());
        assertTrue(observer instanceof QueryResultTrackingObserver);
        QueryResultTrackingObserver resultObserver = (QueryResultTrackingObserver) observer;
        assertTrue(resultObserver.isObjectSerialized());
        return null;
      }
    });

    // verify if objects returned by local server query are not serialized
    server1.invoke(new SerializableCallable("Query") {
      @Override
      public Object call() throws Exception {
        QueryObserver observer = QueryObserverHolder
            .setInstance(new QueryResultTrackingObserver());
        QueryService qs = null;
        try {
          qs = getCache().getQueryService();
        } catch (Exception e) {
          Assert.fail("Exception getting query service ", e);
        }
        SelectResults res = null;
        for (int i = 0; i < queries.length; i++) {
          try {
            res = (SelectResults) qs.newQuery(queries[i]).execute();
          } catch (Exception e) {
            Assert.fail("Error executing query: " + queries[i], e);
          }
          assertEquals(resultSize[i], res.size());
          if (i == 3) {
            int cnt = ((Integer) res.iterator().next());
            assertEquals(20, cnt);

          } else {
            for (Object rs : res) {
              if (rs instanceof StructImpl) {
                for (Object obj : ((StructImpl) rs).getFieldValues()) {
                  if (obj instanceof PortfolioPdx || obj instanceof PositionPdx) {
                  } else {
                    fail("Result objects for remote client query: "
                        + queries[i]
                        + " should be instance of PortfolioPdx and not "
                        + obj.getClass());
                  }
                }
              } else if (rs instanceof PortfolioPdx) {
              } else {
                fail("Result objects for remote client query: " + queries[i]
                    + " should be instance of PortfolioPdx and not "
                    + rs.getClass());
              }
            }
          }
        }
        observer = QueryObserverHolder.getInstance();
        assertTrue(QueryObserverHolder.hasObserver());
        assertTrue(observer instanceof QueryResultTrackingObserver);
        QueryResultTrackingObserver resultObserver = (QueryResultTrackingObserver) observer;
        assertFalse(resultObserver.isObjectSerialized());
        QueryObserverHolder.setInstance(oldObserver);
        return null;
      }
    });

    // verify if Pdx instances are returned by local server query
    // if read-serialized is set true
    server1.invoke(new SerializableCallable("Query") {
      @Override
      public Object call() throws Exception {
        GemFireCacheImpl cache = (GemFireCacheImpl) getCache();
        cache.setReadSerialized(true);
        QueryService qs = null;
        try {
          qs = getCache().getQueryService();
        } catch (Exception e) {
          Assert.fail("Exception getting query service ", e);
        }
        SelectResults res = null;
        for (int i = 0; i < queries.length; i++) {
          try {
            res = (SelectResults) qs.newQuery(queries[i]).execute();
          } catch (Exception e) {
            Assert.fail("Error executing query: " + queries[i], e);
          }
          assertEquals(resultSize[i], res.size());
          if (i == 3) {
            int cnt = ((Integer) res.iterator().next());
            assertEquals(20, cnt);

          } else {
            for(Object rs : res){
              if (rs instanceof StructImpl) {
                for (Object obj : ((StructImpl) rs).getFieldValues()) {
                  if (obj instanceof PdxInstance) {
                  } else {
                    fail("Result objects for remote client query: "
                        + queries[i]
                        + " should be instance of PdxInstance and not "
                        + obj.getClass());
                  }
                }
              } else if (rs instanceof PdxInstance) {
              } else {
                fail("Result objects for remote client query: " + queries[i]
                    + " should be instance of PdxInstance and not "
                    + rs.getClass());
              }
            }
          }
        }
        return null;
      }
    });

    closeCache(client);
    closeCache(server1);
  }

  public void testSelectStarQueryForPdxAndNonPdxObjects() throws Exception {
    final Host host = Host.getHost(0);
    final VM server1 = host.getVM(0);
    final VM client = host.getVM(3);
    // create servers and regions
    // put domain objects
    final int port1 = startReplicatedCacheServer(server1);

    final QueryObserver oldObserver = (QueryObserver) server1
        .invoke(new SerializableCallable("Set observer") {
          @Override
          public Object call() throws Exception {
            QueryObserver observer = QueryObserverHolder
                .setInstance(new QueryResultTrackingObserver());
            return observer;
          }
        });

    // create client
    client.invoke(new SerializableCallable("Create client") {
      @Override
      public Object call() throws Exception {
        ClientCacheFactory cf = new ClientCacheFactory();
        cf.addPoolServer(NetworkUtils.getServerHostName(server1.getHost()), port1);
        ClientCache cache = getClientCache(cf);
        cache.createClientRegionFactory(ClientRegionShortcut.CACHING_PROXY)
            .create(regName);
        return null;
      }
    });

    // Add some serialized PortfolioPdx objects
    client.invoke(new SerializableCallable("Put objects") {
      @Override
      public Object call() throws Exception {
        Region r1 = getRootRegion(regName);
        for (int i = 10; i < 20; i++) {
          r1.put("key-" + i, new PortfolioPdx(i));
        }
        return null;
      }
    });

    // query remotely from client
    client.invoke(new SerializableCallable("Query") {
      @Override
      public Object call() throws Exception {
        LogWriterUtils.getLogWriter().info("Querying remotely from client");
        QueryService localQS = null;
        QueryService remoteQS = null;
        try {
          localQS = ((ClientCache) getCache()).getLocalQueryService();
          remoteQS = ((ClientCache) getCache()).getQueryService();
        } catch (Exception e) {
          Assert.fail("Exception getting query service ", e);
        }
        SelectResults res = null;
        SelectResults[][] sr = new SelectResults[1][2];

        for (int i = 0; i < queries.length; i++) {
          try {
            res = (SelectResults) localQS.newQuery(queries[i]).execute();
            sr[0][0] = res;
            res = (SelectResults) remoteQS.newQuery(queries[i]).execute();
            sr[0][1] = res;
            CacheUtils.compareResultsOfWithAndWithoutIndex(sr);
          } catch (Exception e) {
            Assert.fail("Error executing query: " + queries[i], e);
          }
          assertEquals(resultSize[i], res.size());
          if (i == 3) {
            int cnt = ((Integer) res.iterator().next());
            assertEquals(20, cnt);

          } else {
            for (Object rs : res) {
              if (rs instanceof StructImpl) {
                for (Object obj : ((StructImpl) rs).getFieldValues()) {
                  if (obj instanceof PortfolioPdx || obj instanceof PositionPdx) {
                  } else if (obj instanceof PortfolioPdx
                      || obj instanceof PositionPdx) {
                  } else {
                    fail("Result objects for remote client query: "
                        + queries[i]
                        + " should be instance of PortfolioPdx or PortfolioPdx and not "
                        + obj.getClass());
                  }
                }
              } else if (rs instanceof PortfolioPdx || rs instanceof PortfolioPdx) {
              } else {
                fail("Result objects for remote client query: " + queries[i]
                    + " should be instance of PortfolioPdx and not "
                    + rs.getClass());
              }
            }
          }
        }
        return null;
      }
    });

    // verify if objects iterated by query are serialized
    server1.invoke(new SerializableCallable("Get observer") {
      @Override
      public Object call() throws Exception {
        QueryObserver observer = QueryObserverHolder.getInstance();
        assertTrue(QueryObserverHolder.hasObserver());
        assertTrue(observer instanceof QueryResultTrackingObserver);
        QueryResultTrackingObserver resultObserver = (QueryResultTrackingObserver) observer;
        assertTrue(resultObserver.isObjectSerialized());
        return null;
      }
    });

    // verify if objects returned by local server query are not serialized
    server1.invoke(new SerializableCallable("Query") {
      @Override
      public Object call() throws Exception {
        QueryObserver observer = QueryObserverHolder
            .setInstance(new QueryResultTrackingObserver());
        QueryService qs = null;
        try {
          qs = getCache().getQueryService();
        } catch (Exception e) {
          Assert.fail("Exception getting query service ", e);
        }
        SelectResults res = null;
        for (int i = 0; i < queries.length; i++) {
          try {
            res = (SelectResults) qs.newQuery(queries[i]).execute();
          } catch (Exception e) {
            Assert.fail("Error executing query: " + queries[i], e);
          }
          assertEquals(resultSize[i], res.size());
          if (i == 3) {
            int cnt = ((Integer) res.iterator().next());
            assertEquals(20, cnt);

          } else {
            for (Object rs : res) {
              if (rs instanceof StructImpl) {
                for (Object obj : ((StructImpl) rs).getFieldValues()) {
                  if (obj instanceof PortfolioPdx || obj instanceof PositionPdx) {
                  } else if (obj instanceof PortfolioPdx
                      || obj instanceof PositionPdx) {
                  } else {
                    fail("Result objects for remote client query: "
                        + queries[i]
                        + " should be instance of PortfolioPdx or PortfolioPdx and not "
                        + obj.getClass());
                  }
                }
              } else if (rs instanceof PortfolioPdx || rs instanceof PortfolioPdx) {
              } else {
                fail("Result objects for remote client query: " + queries[i]
                    + " should be instance of PortfolioPdx and not "
                    + rs.getClass());
              }
            }
          }
        }
        observer = QueryObserverHolder.getInstance();
        assertTrue(QueryObserverHolder.hasObserver());
        assertTrue(observer instanceof QueryResultTrackingObserver);
        QueryResultTrackingObserver resultObserver = (QueryResultTrackingObserver) observer;
        assertFalse(resultObserver.isObjectSerialized());
        QueryObserverHolder.setInstance(oldObserver);
        return null;
      }
    });

    server1.invoke(new SerializableCallable("Query") {
      @Override
      public Object call() throws Exception {
        GemFireCacheImpl cache = (GemFireCacheImpl) getCache();
        cache.setReadSerialized(true);
        QueryService qs = null;
        try {
          qs = getCache().getQueryService();
        } catch (Exception e) {
          Assert.fail("Exception getting query service ", e);
        }
        SelectResults res = null;
        for (int i = 0; i < queries.length; i++) {
          try {
            res = (SelectResults) qs.newQuery(queries[i]).execute();
          } catch (Exception e) {
            Assert.fail("Error executing query: " + queries[i], e);
          }
          assertEquals(resultSize[i], res.size());
          if (i == 3) {
            int cnt = ((Integer) res.iterator().next());
            assertEquals(20, cnt);

          } else {
            for(Object rs : res){
              if (rs instanceof StructImpl) {
                for (Object obj : ((StructImpl) rs).getFieldValues()) {
                  if (obj instanceof PortfolioPdx || obj instanceof PositionPdx) {
                  } else if (obj instanceof PdxInstance) {
                  } else {
                    fail("Result objects for remote client query: "
                        + queries[i]
                        + " should be instance of PdxInstance and not "
                        + obj.getClass());
                  }
                }
              } else if (rs instanceof PdxInstance || rs instanceof PortfolioPdx) {
              } else {
                fail("Result objects for remote client query: " + queries[i]
                    + " should be instance of PdxInstance and not "
                    + rs.getClass());
              }
            }
          }
        }
        return null;
      }
    });

    closeCache(client);
    closeCache(server1);
  }

  public void testSelectStarQueryForPdxObjectsReadSerializedTrue()
      throws Exception {
    final Host host = Host.getHost(0);
    final VM server1 = host.getVM(0);
    final VM client = host.getVM(3);
    // create servers and regions
    final int port = (Integer) server1.invoke(new SerializableCallable(
        "Create Server1") {
      @Override
      public Object call() throws Exception {
        ((GemFireCacheImpl) getCache()).setReadSerialized(true);
        Region r1 = getCache().createRegionFactory(RegionShortcut.REPLICATE)
            .create(regName);
        CacheServer server = getCache().addCacheServer();
        int port = AvailablePortHelper.getRandomAvailablePortForDUnitSite();
        server.setPort(port);
        server.start();
        return port;
      }
    });

    // create client
    client.invoke(new SerializableCallable("Create client") {
      @Override
      public Object call() throws Exception {
        ClientCacheFactory cf = new ClientCacheFactory();
        cf.addPoolServer(NetworkUtils.getServerHostName(server1.getHost()), port);
        ClientCache cache = getClientCache(cf);
        cache.createClientRegionFactory(ClientRegionShortcut.CACHING_PROXY)
            .create(regName);
        Region r1 = getRootRegion(regName);
        for (int i = 0; i < 20; i++) {
          r1.put("key-" + i, new PortfolioPdx(i));
        }
        return null;
      }
    });

    // query remotely from client
    client.invoke(new SerializableCallable("Query") {
      @Override
      public Object call() throws Exception {
        LogWriterUtils.getLogWriter().info("Querying remotely from client");
        QueryService localQS = null;
        QueryService remoteQS = null;
        try {
          localQS = ((ClientCache) getCache()).getLocalQueryService();
          remoteQS = ((ClientCache) getCache()).getQueryService();
        } catch (Exception e) {
          Assert.fail("Exception getting query service ", e);
        }
        SelectResults res = null;
        SelectResults[][] sr = new SelectResults[1][2];

        for (int i = 0; i < queries.length; i++) {
          try {
            res = (SelectResults) localQS.newQuery(queries[i]).execute();
            sr[0][0] = res;
            res = (SelectResults) remoteQS.newQuery(queries[i]).execute();
            sr[0][1] = res;
            CacheUtils.compareResultsOfWithAndWithoutIndex(sr);
          } catch (Exception e) {
            Assert.fail("Error executing query: " + queries[i], e);
          }
          assertEquals(resultSize[i], res.size());
          if (i == 3) {
            int cnt = ((Integer) res.iterator().next());
            assertEquals(20, cnt);

          } else {
            for (Object rs : res) {
              if (rs instanceof StructImpl) {
                for (Object obj : ((StructImpl) rs).getFieldValues()) {
                  if (obj instanceof PortfolioPdx || obj instanceof PositionPdx) {
                  } else {
                    fail("Result objects for remote client query: "
                        + queries[i]
                        + " should be instance of PortfolioPdx and not "
                        + obj.getClass());
                  }
                }
              } else if (rs instanceof PortfolioPdx) {
              } else {
                fail("Result objects for remote client query: " + queries[i]
                    + " should be instance of PortfolioPdx and not "
                    + rs.getClass());
              }
            }
          }
        }
        return null;
      }
    });

    closeCache(client);
    closeCache(server1);
  }

  private int startPartitionedCacheServer(VM vm, final Object[] objs) {
    final int port = (Integer) vm.invoke(new SerializableCallable(
        "Create Server1") {
      @Override
      public Object call() throws Exception {
        Region r1 = getCache().createRegionFactory(RegionShortcut.PARTITION)
            .create(regName);
        // put domain objects
        for (int i = 0; i < objs.length; i++) {
          r1.put("key-" + i, objs[i]);
        }

        CacheServer server = getCache().addCacheServer();
        int port = AvailablePortHelper.getRandomAvailablePortForDUnitSite();
        server.setPort(port);
        server.start();
        return port;
      }
    });
    return port;
  }

  private int startReplicatedCacheServer(VM vm) {
    final int port = (Integer) vm.invoke(new SerializableCallable(
        "Create Server1") {
      @Override
      public Object call() throws Exception {
        Region r1 = getCache().createRegionFactory(RegionShortcut.REPLICATE)
            .create(regName);
        Region r2 = getCache().createRegionFactory(RegionShortcut.REPLICATE)
            .create(regName2);
        // put domain objects
        for (int i = 0; i < 10; i++) {
          r1.put("key-" + i, new PortfolioPdx(i));
          r2.put("key-" + i, new PortfolioPdx(i));
        }

        CacheServer server = getCache().addCacheServer();
        int port = AvailablePortHelper.getRandomAvailablePortForDUnitSite();
        server.setPort(port);
        server.start();
        return port;
      }
    });
    return port;
  }

  private void closeCache(VM vm) {
    vm.invoke(new SerializableCallable() {
      public Object call() throws Exception {
        closeCache();
        return null;
      }
    });
  }

  public class QueryResultTrackingObserver extends QueryObserverAdapter 
      implements Serializable{
    private boolean isObjectSerialized = false;
    @Override
    public void beforeIterationEvaluation(CompiledValue executer,
        Object currentObject) {
      if (currentObject instanceof VMCachedDeserializable) {
        LogWriterUtils.getLogWriter().fine("currentObject is serialized object");
        isObjectSerialized = true;
      } else {
        LogWriterUtils.getLogWriter().fine("currentObject is deserialized object");
      }
    }

    public boolean isObjectSerialized() {
      return isObjectSerialized;
    }

    public void setObjectSerialized(boolean isObjectSerialized) {
      this.isObjectSerialized = isObjectSerialized;
    }

  }

}
