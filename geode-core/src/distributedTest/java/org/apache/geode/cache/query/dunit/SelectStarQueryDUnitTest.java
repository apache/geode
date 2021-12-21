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

import static org.apache.geode.cache.Region.SEPARATOR;
import static org.apache.geode.test.dunit.Assert.assertEquals;
import static org.apache.geode.test.dunit.Assert.assertFalse;
import static org.apache.geode.test.dunit.Assert.assertTrue;
import static org.apache.geode.test.dunit.Assert.fail;
import static org.apache.geode.test.dunit.Invoke.invokeInEveryVM;
import static org.apache.geode.test.dunit.LogWriterUtils.getLogWriter;
import static org.apache.geode.test.dunit.NetworkUtils.getServerHostName;

import java.io.Serializable;

import org.junit.Test;
import org.junit.experimental.categories.Category;

import org.apache.geode.cache.Region;
import org.apache.geode.cache.RegionShortcut;
import org.apache.geode.cache.client.ClientCache;
import org.apache.geode.cache.client.ClientCacheFactory;
import org.apache.geode.cache.client.ClientRegionShortcut;
import org.apache.geode.cache.query.CacheUtils;
import org.apache.geode.cache.query.QueryService;
import org.apache.geode.cache.query.SelectResults;
import org.apache.geode.cache.query.data.PortfolioPdx;
import org.apache.geode.cache.query.data.PositionPdx;
import org.apache.geode.cache.query.internal.CompiledValue;
import org.apache.geode.cache.query.internal.QueryObserver;
import org.apache.geode.cache.query.internal.QueryObserverAdapter;
import org.apache.geode.cache.query.internal.QueryObserverHolder;
import org.apache.geode.cache.query.internal.StructImpl;
import org.apache.geode.cache.server.CacheServer;
import org.apache.geode.internal.AvailablePortHelper;
import org.apache.geode.internal.cache.GemFireCacheImpl;
import org.apache.geode.internal.cache.VMCachedDeserializable;
import org.apache.geode.pdx.PdxInstance;
import org.apache.geode.test.dunit.Host;
import org.apache.geode.test.dunit.SerializableCallable;
import org.apache.geode.test.dunit.VM;
import org.apache.geode.test.dunit.cache.internal.JUnit4CacheTestCase;
import org.apache.geode.test.junit.categories.OQLQueryTest;

/**
 * Test for #44807 to eliminate unnecessary serialization/deserialization in select * queries
 */
@Category({OQLQueryTest.class})
public class SelectStarQueryDUnitTest extends JUnit4CacheTestCase {

  /** Used for saving & restoring oldObserver without serialization */
  private static volatile QueryObserver oldObserver;

  private final String regName = "exampleRegion";
  private final String regName2 = "exampleRegion2";

  private final String[] queries = {"SELECT * FROM " + SEPARATOR + regName, // 0
      "SELECT * FROM " + SEPARATOR + regName + " limit 5", // 1
      "SELECT p from  " + SEPARATOR + regName + " p", // 2
      "SELECT count(*) FROM " + SEPARATOR + regName, // 3
      "SELECT ALL * from " + SEPARATOR + regName, // 4
      "SELECT * from " + SEPARATOR + regName + ".values", // 5
      "SELECT distinct * FROM " + SEPARATOR + regName, // 6
      "SELECT distinct * FROM " + SEPARATOR + regName + " p order by p.ID", // 7
      "SELECT * from " + SEPARATOR + regName + " r, positions.values pos"// 8
  };
  private final int[] resultSize = {20, 5, 20, 1, 20, 20, 20, 20, 40};

  private final String[] multipleRegionQueries =
      {" SELECT * FROM " + SEPARATOR + regName + ", " + SEPARATOR + regName2,
          "SELECT * FROM " + SEPARATOR + regName + ", " + SEPARATOR + regName2 + " limit 5",
          "SELECT distinct * FROM " + SEPARATOR + regName + ", " + SEPARATOR + regName2,
          "SELECT distinct * FROM " + SEPARATOR + regName + " p1, " + SEPARATOR + regName2
              + " p2 order by p1.ID",
          "SELECT count(*) FROM " + SEPARATOR + regName + ", " + SEPARATOR + regName2,
          "SELECT p, q from  " + SEPARATOR + regName + " p, " + SEPARATOR + regName2 + " q",
          "SELECT ALL * from " + SEPARATOR + regName + " p, " + SEPARATOR + regName2 + " q",
          "SELECT * from " + SEPARATOR + regName + ".values" + ", " + SEPARATOR + regName2
              + ".values",
          "SELECT * from " + SEPARATOR + regName + " p, p.positions.values pos" + ", " + SEPARATOR
              + regName2
              + " q, q.positions.values pos",};
  private final int[] resultSize2 = {400, 5, 400, 400, 1, 400, 400, 400, 1600};

  @Override
  public final void preTearDownCacheTestCase() throws Exception {
    invokeInEveryVM(() -> oldObserver = null);
  }

  @Test
  public void functionWithStructTypeInInnerQueryShouldNotThrowException() throws Exception {
    final Host host = Host.getHost(0);
    final VM server1 = host.getVM(0);
    final VM client = host.getVM(3);
    PortfolioPdx[] portfolios = new PortfolioPdx[10];
    for (int i = 0; i < portfolios.length; i++) {
      portfolios[i] = new PortfolioPdx(i);
    }

    // create servers and regions
    final int port1 = startPartitionedCacheServer(server1, portfolios);

    // create client
    client.invoke(new SerializableCallable("Create client") {
      @Override
      public Object call() throws Exception {
        ClientCacheFactory cf = new ClientCacheFactory();
        cf.addPoolServer(getServerHostName(server1.getHost()), port1);
        ClientCache cache = getClientCache(cf);
        cache.createClientRegionFactory(ClientRegionShortcut.PROXY).create(regName);
        return null;
      }
    });

    // put serialized PortfolioPdx objects
    client.invoke(new SerializableCallable("Put objects") {
      @Override
      public Object call() throws Exception {
        Region r1 = getRootRegion(regName);
        for (int i = 10; i < 100; i++) {
          r1.put("key-" + i, new PortfolioPdx(i));
        }
        return null;
      }
    });

    // query remotely from client
    client.invoke(new SerializableCallable("Query") {
      @Override
      public Object call() throws Exception {
        getLogWriter().info("Querying remotely from client");
        QueryService remoteQS = null;
        try {
          remoteQS = getCache().getQueryService();
          SelectResults sr = (SelectResults) remoteQS
              .newQuery("select distinct oP.ID, oP.status, oP.getType from " + SEPARATOR + regName
                  + " oP where element(select distinct p.ID, p.status, p.getType from " + SEPARATOR
                  + regName
                  + " p where p.ID = oP.ID).status = 'inactive'")
              .execute();
          assertEquals(50, sr.size());
        } catch (Exception e) {
          fail("Exception getting query service ", e);
        }

        return null;
      }
    });

    closeCache(client);
    closeCache(server1);
  }

  @Test
  public void functionWithStructTypeInInnerQueryShouldNotThrowExceptionWhenRunOnMultipleNodes()
      throws Exception {
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

    // create client
    client.invoke(new SerializableCallable("Create client") {
      @Override
      public Object call() throws Exception {
        ClientCacheFactory cf = new ClientCacheFactory();
        cf.addPoolServer(getServerHostName(server1.getHost()), port1);
        cf.addPoolServer(getServerHostName(server2.getHost()), port2);
        cf.addPoolServer(getServerHostName(server3.getHost()), port3);
        ClientCache cache = getClientCache(cf);
        cache.createClientRegionFactory(ClientRegionShortcut.PROXY).create(regName);
        return null;
      }
    });

    // put serialized PortfolioPdx objects
    client.invoke(new SerializableCallable("Put objects") {
      @Override
      public Object call() throws Exception {
        Region r1 = getRootRegion(regName);
        for (int i = 10; i < 100; i++) {
          r1.put("key-" + i, new PortfolioPdx(i));
        }
        return null;
      }
    });

    // query remotely from client
    client.invoke(new SerializableCallable("Query") {
      @Override
      public Object call() throws Exception {
        getLogWriter().info("Querying remotely from client");
        QueryService remoteQS = null;
        try {
          remoteQS = getCache().getQueryService();
          SelectResults sr = (SelectResults) remoteQS
              .newQuery("select distinct oP.ID, oP.status, oP.getType from " + SEPARATOR + regName
                  + " oP where element(select distinct p.ID, p.status, p.getType from " + SEPARATOR
                  + regName
                  + " p where p.ID = oP.ID).status = 'inactive'")
              .execute();
          assertEquals(50, sr.size());
        } catch (Exception e) {
          fail("Exception getting query service ", e);
        }
        return null;
      }
    });


    closeCache(client);
    closeCache(server1);
    closeCache(server2);
    closeCache(server3);
  }

  @Test
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

    server1.invoke(new SerializableCallable("Set observer") {
      @Override
      public Object call() throws Exception {
        oldObserver = QueryObserverHolder.setInstance(new QueryResultTrackingObserver());
        return null;
      }
    });

    // create client
    client.invoke(new SerializableCallable("Create client") {
      @Override
      public Object call() throws Exception {
        ClientCacheFactory cf = new ClientCacheFactory();
        cf.addPoolServer(getServerHostName(server1.getHost()), port1);
        cf.addPoolServer(getServerHostName(server2.getHost()), port2);
        cf.addPoolServer(getServerHostName(server3.getHost()), port3);
        ClientCache cache = getClientCache(cf);
        cache.createClientRegionFactory(ClientRegionShortcut.CACHING_PROXY).create(regName);
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
        getLogWriter().info("Querying remotely from client");
        QueryService localQS = null;
        QueryService remoteQS = null;
        try {
          localQS = ((ClientCache) getCache()).getLocalQueryService();
          remoteQS = getCache().getQueryService();
        } catch (Exception e) {
          fail("Exception getting query service ", e);
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
            fail("Error executing query: " + queries[i], e);
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
                    fail("Result objects for remote client query: " + queries[i]
                        + " should be instance of PortfolioPdx and not " + obj.getClass());
                  }
                }
              } else if (rs instanceof PortfolioPdx) {
              } else {
                fail("Result objects for remote client query: " + queries[i]
                    + " should be instance of PortfolioPdx and not " + rs.getClass());
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
        QueryObserver observer = QueryObserverHolder.setInstance(new QueryResultTrackingObserver());
        QueryService qs = null;
        try {
          qs = getCache().getQueryService();
        } catch (Exception e) {
          fail("Exception getting query service ", e);
        }
        SelectResults res = null;
        for (int i = 0; i < queries.length; i++) {
          try {
            res = (SelectResults) qs.newQuery(queries[i]).execute();
          } catch (Exception e) {
            fail("Error executing query: " + queries[i], e);
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
                    fail("Result objects for remote client query: " + queries[i]
                        + " should be instance of PortfolioPdx and not " + obj.getClass());
                  }
                }
              } else if (rs instanceof PortfolioPdx) {
              } else {
                fail("Result objects for remote client query: " + queries[i]
                    + " should be instance of PortfolioPdx and not " + rs.getClass());
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

  @Test
  public void testSelectStarQueryForReplicatedRegion() throws Exception {
    final Host host = Host.getHost(0);
    final VM server1 = host.getVM(1);
    final VM client = host.getVM(3);
    // create servers and regions
    final int port1 = startReplicatedCacheServer(server1);

    server1.invoke(new SerializableCallable("Set observer") {
      @Override
      public Object call() throws Exception {
        oldObserver = QueryObserverHolder.setInstance(new QueryResultTrackingObserver());
        return null;
      }
    });

    // create client
    client.invoke(new SerializableCallable("Create client") {
      @Override
      public Object call() throws Exception {
        ClientCacheFactory cf = new ClientCacheFactory();
        cf.addPoolServer(getServerHostName(server1.getHost()), port1);
        ClientCache cache = getClientCache(cf);
        cache.createClientRegionFactory(ClientRegionShortcut.CACHING_PROXY).create(regName);
        cache.createClientRegionFactory(ClientRegionShortcut.CACHING_PROXY).create(regName2);
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
        getLogWriter().info("Querying remotely from client");
        QueryService localQS = null;
        QueryService remoteQS = null;
        try {
          localQS = ((ClientCache) getCache()).getLocalQueryService();
          remoteQS = getCache().getQueryService();
        } catch (Exception e) {
          fail("Exception getting query service ", e);
        }
        SelectResults res = null;
        SelectResults[][] sr = new SelectResults[1][2];

        for (int i = 0; i < multipleRegionQueries.length; i++) {
          try {
            res = (SelectResults) localQS.newQuery(multipleRegionQueries[i]).execute();
            sr[0][0] = res;
            res = (SelectResults) remoteQS.newQuery(multipleRegionQueries[i]).execute();
            sr[0][1] = res;
            CacheUtils.compareResultsOfWithAndWithoutIndex(sr);
          } catch (Exception e) {
            fail("Error executing query: " + multipleRegionQueries[i], e);
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
                    fail("Result objects for remote client query: " + multipleRegionQueries[i]
                        + " should be instance of PortfolioPdx and not " + obj.getClass());
                  }
                }
              } else if (rs instanceof PortfolioPdx) {
              } else {
                fail("Result objects for remote client query: " + multipleRegionQueries[i]
                    + " should be instance of PortfolioPdx and not " + rs.getClass());
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
        QueryObserver observer = QueryObserverHolder.setInstance(new QueryResultTrackingObserver());
        QueryService qs = null;
        try {
          qs = getCache().getQueryService();
        } catch (Exception e) {
          fail("Exception getting query service ", e);
        }
        SelectResults res = null;
        for (int i = 0; i < multipleRegionQueries.length; i++) {
          try {
            res = (SelectResults) qs.newQuery(multipleRegionQueries[i]).execute();
          } catch (Exception e) {
            fail("Error executing query: " + multipleRegionQueries[i], e);
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
                    fail("Result objects for remote client query: " + multipleRegionQueries[i]
                        + " should be instance of PortfolioPdx and not " + obj.getClass());
                  }
                }
              } else if (rs instanceof PortfolioPdx) {
              } else {
                fail("Result objects for remote client query: " + multipleRegionQueries[i]
                    + " should be instance of PortfolioPdx and not " + rs.getClass());
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

  @Test
  public void testByteArrayReplicatedRegion() throws Exception {
    final Host host = Host.getHost(0);
    final VM server1 = host.getVM(0);
    final VM client = host.getVM(3);
    final byte[] ba = new byte[] {1, 2, 3, 4, 5};

    // create servers and regions
    final int port = (Integer) server1.invoke(new SerializableCallable("Create Server1") {
      @Override
      public Object call() throws Exception {
        Region r1 = getCache().createRegionFactory(RegionShortcut.REPLICATE).create(regName);
        // put domain objects
        for (int i = 0; i < 10; i++) {
          r1.put("key-" + i, ba);
        }
        CacheServer server = getCache().addCacheServer();
        int port = AvailablePortHelper.getRandomAvailableTCPPort();
        server.setPort(port);
        server.start();
        return port;
      }
    });

    server1.invoke(new SerializableCallable("Set observer") {
      @Override
      public Object call() throws Exception {
        oldObserver = QueryObserverHolder.setInstance(new QueryResultTrackingObserver());
        return null;
      }
    });

    // create client
    client.invoke(new SerializableCallable("Create client") {
      @Override
      public Object call() throws Exception {
        ClientCacheFactory cf = new ClientCacheFactory();
        cf.addPoolServer(getServerHostName(server1.getHost()), port);
        ClientCache cache = getClientCache(cf);
        cache.createClientRegionFactory(ClientRegionShortcut.CACHING_PROXY).create(regName);
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
        getLogWriter().info("Querying remotely from client");
        QueryService localQS = null;
        QueryService remoteQS = null;
        try {
          localQS = ((ClientCache) getCache()).getLocalQueryService();
          remoteQS = getCache().getQueryService();
        } catch (Exception e) {
          fail("Exception getting query service ", e);
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
            fail("Error executing query: " + queries[i], e);
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
                    + " should be instance of PortfolioPdx and not " + o.getClass());
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
        QueryObserver observer = QueryObserverHolder.setInstance(new QueryResultTrackingObserver());
        QueryService qs = null;
        try {
          qs = getCache().getQueryService();
        } catch (Exception e) {
          fail("Exception getting query service ", e);
        }
        SelectResults res = null;
        for (int i = 0; i < 6; i++) {
          try {
            res = (SelectResults) qs.newQuery(queries[i]).execute();
          } catch (Exception e) {
            fail("Error executing query: " + queries[i], e);
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
                    + " should be instance of byte array and not " + o.getClass());
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

  @Test
  public void testByteArrayPartitionedRegion() throws Exception {
    final Host host = Host.getHost(0);
    final VM server1 = host.getVM(0);
    final VM client = host.getVM(3);
    final VM server2 = host.getVM(1);
    final VM server3 = host.getVM(2);
    final byte[][] objs = new byte[10][5];

    for (int i = 0; i < objs.length; i++) {
      objs[i] = new byte[] {1, 2, 3, 4, 5};
    }

    // create servers and regions
    final int port1 = startPartitionedCacheServer(server1, objs);
    final int port2 = startPartitionedCacheServer(server2, objs);
    final int port3 = startPartitionedCacheServer(server3, objs);

    server1.invoke(new SerializableCallable("Set observer") {
      @Override
      public Object call() throws Exception {
        oldObserver = QueryObserverHolder.setInstance(new QueryResultTrackingObserver());
        return null;
      }
    });

    // create client
    client.invoke(new SerializableCallable("Create client") {
      @Override
      public Object call() throws Exception {
        ClientCacheFactory cf = new ClientCacheFactory();
        cf.addPoolServer(getServerHostName(server1.getHost()), port1);
        cf.addPoolServer(getServerHostName(server2.getHost()), port2);
        cf.addPoolServer(getServerHostName(server3.getHost()), port3);
        ClientCache cache = getClientCache(cf);
        cache.createClientRegionFactory(ClientRegionShortcut.CACHING_PROXY).create(regName);
        return null;
      }
    });

    // put serialized PortfolioPdx objects
    client.invoke(new SerializableCallable("Put objects") {
      @Override
      public Object call() throws Exception {
        Region r1 = getRootRegion(regName);
        for (int i = 10; i < 20; i++) {
          r1.put("key-" + i, new byte[] {1, 2, 3, 4, 5});
        }
        return null;
      }
    });

    // query remotely from client
    client.invoke(new SerializableCallable("Query") {
      @Override
      public Object call() throws Exception {
        getLogWriter().info("Querying remotely from client");
        QueryService localQS = null;
        QueryService remoteQS = null;
        try {
          localQS = ((ClientCache) getCache()).getLocalQueryService();
          remoteQS = getCache().getQueryService();
        } catch (Exception e) {
          fail("Exception getting query service ", e);
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
            fail("Error executing query: " + queries[i], e);
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
                    + " should be instance of PortfolioPdx and not " + o.getClass());
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
        QueryObserver observer = QueryObserverHolder.setInstance(new QueryResultTrackingObserver());
        QueryService qs = null;
        try {
          qs = getCache().getQueryService();
        } catch (Exception e) {
          fail("Exception getting query service ", e);
        }
        SelectResults res = null;
        for (int i = 0; i < 6; i++) {
          try {
            res = (SelectResults) qs.newQuery(queries[i]).execute();
          } catch (Exception e) {
            fail("Error executing query: " + queries[i], e);
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
                    + " should be instance of byte array and not " + o.getClass());
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

  @Test
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
        cf.addPoolServer(getServerHostName(server1.getHost()), port1);
        ClientCache cache = getClientCache(cf);
        cache.createClientRegionFactory(ClientRegionShortcut.CACHING_PROXY).create(regName);
        cache.createClientRegionFactory(ClientRegionShortcut.CACHING_PROXY).create(regName2);
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
          qs.createIndex("status", "status", SEPARATOR + regName);
        } catch (Exception e) {
          fail("Exception getting query service ", e);
        }

        return null;
      }
    });

    // query remotely from client
    client.invoke(new SerializableCallable("Query") {
      @Override
      public Object call() throws Exception {
        getLogWriter().info("Querying remotely from client");
        QueryService localQS = null;
        QueryService remoteQS = null;
        try {
          localQS = ((ClientCache) getCache()).getLocalQueryService();
          remoteQS = getCache().getQueryService();
        } catch (Exception e) {
          fail("Exception getting query service ", e);
        }
        SelectResults res = null;
        SelectResults[][] sr = new SelectResults[1][2];

        for (int i = 0; i < multipleRegionQueries.length; i++) {
          try {
            res = (SelectResults) localQS.newQuery(multipleRegionQueries[i]).execute();
            sr[0][0] = res;
            res = (SelectResults) remoteQS.newQuery(multipleRegionQueries[i]).execute();
            sr[0][1] = res;
            CacheUtils.compareResultsOfWithAndWithoutIndex(sr);
          } catch (Exception e) {
            fail("Error executing query: " + multipleRegionQueries[i], e);
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
                    fail("Result objects for remote client query: " + multipleRegionQueries[i]
                        + " should be instance of PortfolioPdx and not " + obj.getClass());
                  }
                }
              } else if (rs instanceof PortfolioPdx) {
              } else {
                fail("Result objects for remote client query: " + multipleRegionQueries[i]
                    + " should be instance of PortfolioPdx and not " + rs.getClass());
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
          qs.createIndex("status", "status", SEPARATOR + regName2);
        } catch (Exception e) {
          fail("Exception getting query service ", e);
        }

        return null;
      }
    });

    // query remotely from client
    client.invoke(new SerializableCallable("Query") {
      @Override
      public Object call() throws Exception {
        getLogWriter().info("Querying remotely from client");
        QueryService localQS = null;
        QueryService remoteQS = null;
        try {
          localQS = ((ClientCache) getCache()).getLocalQueryService();
          remoteQS = getCache().getQueryService();
        } catch (Exception e) {
          fail("Exception getting query service ", e);
        }
        SelectResults res = null;
        SelectResults[][] sr = new SelectResults[1][2];

        for (int i = 0; i < multipleRegionQueries.length; i++) {
          try {
            res = (SelectResults) localQS.newQuery(multipleRegionQueries[i]).execute();
            sr[0][0] = res;
            res = (SelectResults) remoteQS.newQuery(multipleRegionQueries[i]).execute();
            sr[0][1] = res;
            CacheUtils.compareResultsOfWithAndWithoutIndex(sr);
          } catch (Exception e) {
            fail("Error executing query: " + multipleRegionQueries[i], e);
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
                    fail("Result objects for remote client query: " + multipleRegionQueries[i]
                        + " should be instance of PortfolioPdx and not " + obj.getClass());
                  }
                }
              } else if (rs instanceof PortfolioPdx) {
              } else {
                fail("Result objects for remote client query: " + multipleRegionQueries[i]
                    + " should be instance of PortfolioPdx and not " + rs.getClass());
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

  @Test
  public void testSelectStarQueryForPdxObjects() throws Exception {
    final Host host = Host.getHost(0);
    final VM server1 = host.getVM(0);
    final VM client = host.getVM(3);
    // create servers and regions
    final int port1 = startReplicatedCacheServer(server1);

    server1.invoke(new SerializableCallable("Set observer") {
      @Override
      public Object call() throws Exception {
        oldObserver = QueryObserverHolder.setInstance(new QueryResultTrackingObserver());
        return null;
      }
    });

    // create client
    client.invoke(new SerializableCallable("Create client") {
      @Override
      public Object call() throws Exception {
        ClientCacheFactory cf = new ClientCacheFactory();
        cf.addPoolServer(getServerHostName(server1.getHost()), port1);
        ClientCache cache = getClientCache(cf);
        cache.createClientRegionFactory(ClientRegionShortcut.CACHING_PROXY).create(regName);
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
        getLogWriter().info("Querying remotely from client");
        QueryService localQS = null;
        QueryService remoteQS = null;
        try {
          localQS = ((ClientCache) getCache()).getLocalQueryService();
          remoteQS = getCache().getQueryService();
        } catch (Exception e) {
          fail("Exception getting query service ", e);
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
            fail("Error executing query: " + queries[i], e);
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
                    fail("Result objects for remote client query: " + queries[i]
                        + " should be instance of PortfolioPdx and not " + obj.getClass());
                  }
                }
              } else if (rs instanceof PortfolioPdx) {
              } else {
                fail("Result objects for remote client query: " + queries[i]
                    + " should be instance of PortfolioPdx and not " + rs.getClass());
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
        QueryObserver observer = QueryObserverHolder.setInstance(new QueryResultTrackingObserver());
        QueryService qs = null;
        try {
          qs = getCache().getQueryService();
        } catch (Exception e) {
          fail("Exception getting query service ", e);
        }
        SelectResults res = null;
        for (int i = 0; i < queries.length; i++) {
          try {
            res = (SelectResults) qs.newQuery(queries[i]).execute();
          } catch (Exception e) {
            fail("Error executing query: " + queries[i], e);
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
                    fail("Result objects for remote client query: " + queries[i]
                        + " should be instance of PortfolioPdx and not " + obj.getClass());
                  }
                }
              } else if (rs instanceof PortfolioPdx) {
              } else {
                fail("Result objects for remote client query: " + queries[i]
                    + " should be instance of PortfolioPdx and not " + rs.getClass());
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
        cache.setReadSerializedForTest(true);
        QueryService qs = null;
        try {
          qs = getCache().getQueryService();
        } catch (Exception e) {
          fail("Exception getting query service ", e);
        }
        SelectResults res = null;
        for (int i = 0; i < queries.length; i++) {
          try {
            res = (SelectResults) qs.newQuery(queries[i]).execute();
          } catch (Exception e) {
            fail("Error executing query: " + queries[i], e);
          }
          assertEquals(resultSize[i], res.size());
          if (i == 3) {
            int cnt = ((Integer) res.iterator().next());
            assertEquals(20, cnt);

          } else {
            for (Object rs : res) {
              if (rs instanceof StructImpl) {
                for (Object obj : ((StructImpl) rs).getFieldValues()) {
                  if (obj instanceof PdxInstance) {
                  } else {
                    fail("Result objects for remote client query: " + queries[i]
                        + " should be instance of PdxInstance and not " + obj.getClass());
                  }
                }
              } else if (rs instanceof PdxInstance) {
              } else {
                fail("Result objects for remote client query: " + queries[i]
                    + " should be instance of PdxInstance and not " + rs.getClass());
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

  @Test
  public void testSelectStarQueryForPdxAndNonPdxObjects() throws Exception {
    final Host host = Host.getHost(0);
    final VM server1 = host.getVM(0);
    final VM client = host.getVM(3);
    // create servers and regions
    // put domain objects
    final int port1 = startReplicatedCacheServer(server1);

    server1.invoke(new SerializableCallable("Set observer") {
      @Override
      public Object call() throws Exception {
        oldObserver = QueryObserverHolder.setInstance(new QueryResultTrackingObserver());
        return null;
      }
    });

    // create client
    client.invoke(new SerializableCallable("Create client") {
      @Override
      public Object call() throws Exception {
        ClientCacheFactory cf = new ClientCacheFactory();
        cf.addPoolServer(getServerHostName(server1.getHost()), port1);
        ClientCache cache = getClientCache(cf);
        cache.createClientRegionFactory(ClientRegionShortcut.CACHING_PROXY).create(regName);
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
        getLogWriter().info("Querying remotely from client");
        QueryService localQS = null;
        QueryService remoteQS = null;
        try {
          localQS = ((ClientCache) getCache()).getLocalQueryService();
          remoteQS = getCache().getQueryService();
        } catch (Exception e) {
          fail("Exception getting query service ", e);
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
            fail("Error executing query: " + queries[i], e);
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
                  } else if (obj instanceof PortfolioPdx || obj instanceof PositionPdx) {
                  } else {
                    fail("Result objects for remote client query: " + queries[i]
                        + " should be instance of PortfolioPdx or PortfolioPdx and not "
                        + obj.getClass());
                  }
                }
              } else if (rs instanceof PortfolioPdx) {
              } else {
                fail("Result objects for remote client query: " + queries[i]
                    + " should be instance of PortfolioPdx and not " + rs.getClass());
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
        QueryObserver observer = QueryObserverHolder.setInstance(new QueryResultTrackingObserver());
        QueryService qs = null;
        try {
          qs = getCache().getQueryService();
        } catch (Exception e) {
          fail("Exception getting query service ", e);
        }
        SelectResults res = null;
        for (int i = 0; i < queries.length; i++) {
          try {
            res = (SelectResults) qs.newQuery(queries[i]).execute();
          } catch (Exception e) {
            fail("Error executing query: " + queries[i], e);
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
                  } else if (obj instanceof PortfolioPdx || obj instanceof PositionPdx) {
                  } else {
                    fail("Result objects for remote client query: " + queries[i]
                        + " should be instance of PortfolioPdx or PortfolioPdx and not "
                        + obj.getClass());
                  }
                }
              } else if (rs instanceof PortfolioPdx) {
              } else {
                fail("Result objects for remote client query: " + queries[i]
                    + " should be instance of PortfolioPdx and not " + rs.getClass());
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
        cache.setReadSerializedForTest(true);
        QueryService qs = null;
        try {
          qs = getCache().getQueryService();
        } catch (Exception e) {
          fail("Exception getting query service ", e);
        }
        SelectResults res = null;
        for (int i = 0; i < queries.length; i++) {
          try {
            res = (SelectResults) qs.newQuery(queries[i]).execute();
          } catch (Exception e) {
            fail("Error executing query: " + queries[i], e);
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
                  } else if (obj instanceof PdxInstance) {
                  } else {
                    fail("Result objects for remote client query: " + queries[i]
                        + " should be instance of PdxInstance and not " + obj.getClass());
                  }
                }
              } else if (rs instanceof PdxInstance || rs instanceof PortfolioPdx) {
              } else {
                fail("Result objects for remote client query: " + queries[i]
                    + " should be instance of PdxInstance and not " + rs.getClass());
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

  @Test
  public void testSelectStarQueryForPdxObjectsReadSerializedTrue() throws Exception {
    final Host host = Host.getHost(0);
    final VM server1 = host.getVM(0);
    final VM client = host.getVM(3);
    // create servers and regions
    final int port = (Integer) server1.invoke(new SerializableCallable("Create Server1") {
      @Override
      public Object call() throws Exception {
        getCache().setReadSerializedForTest(true);
        Region r1 = getCache().createRegionFactory(RegionShortcut.REPLICATE).create(regName);
        CacheServer server = getCache().addCacheServer();
        int port = AvailablePortHelper.getRandomAvailableTCPPort();
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
        cf.addPoolServer(getServerHostName(server1.getHost()), port);
        ClientCache cache = getClientCache(cf);
        cache.createClientRegionFactory(ClientRegionShortcut.CACHING_PROXY).create(regName);
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
        getLogWriter().info("Querying remotely from client");
        QueryService localQS = null;
        QueryService remoteQS = null;
        try {
          localQS = ((ClientCache) getCache()).getLocalQueryService();
          remoteQS = getCache().getQueryService();
        } catch (Exception e) {
          fail("Exception getting query service ", e);
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
            fail("Error executing query: " + queries[i], e);
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
                    fail("Result objects for remote client query: " + queries[i]
                        + " should be instance of PortfolioPdx and not " + obj.getClass());
                  }
                }
              } else if (rs instanceof PortfolioPdx) {
              } else {
                fail("Result objects for remote client query: " + queries[i]
                    + " should be instance of PortfolioPdx and not " + rs.getClass());
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
    final int port = (Integer) vm.invoke(new SerializableCallable("Create Server1") {
      @Override
      public Object call() throws Exception {
        Region r1 = getCache().createRegionFactory(RegionShortcut.PARTITION).create(regName);
        // put domain objects
        for (int i = 0; i < objs.length; i++) {
          r1.put("key-" + i, objs[i]);
        }

        CacheServer server = getCache().addCacheServer();
        int port = AvailablePortHelper.getRandomAvailableTCPPort();
        server.setPort(port);
        server.start();
        return port;
      }
    });
    return port;
  }

  private int startReplicatedCacheServer(VM vm) {
    final int port = (Integer) vm.invoke(new SerializableCallable("Create Server1") {
      @Override
      public Object call() throws Exception {
        Region r1 = getCache().createRegionFactory(RegionShortcut.REPLICATE).create(regName);
        Region r2 = getCache().createRegionFactory(RegionShortcut.REPLICATE).create(regName2);
        // put domain objects
        for (int i = 0; i < 10; i++) {
          r1.put("key-" + i, new PortfolioPdx(i));
          r2.put("key-" + i, new PortfolioPdx(i));
        }

        CacheServer server = getCache().addCacheServer();
        int port = AvailablePortHelper.getRandomAvailableTCPPort();
        server.setPort(port);
        server.start();
        return port;
      }
    });
    return port;
  }

  private void closeCache(VM vm) {
    vm.invoke(new SerializableCallable() {
      @Override
      public Object call() throws Exception {
        closeCache();
        return null;
      }
    });
  }

  public class QueryResultTrackingObserver extends QueryObserverAdapter implements Serializable {

    private boolean isObjectSerialized = false;

    @Override
    public void beforeIterationEvaluation(CompiledValue executer, Object currentObject) {
      if (currentObject instanceof VMCachedDeserializable) {
        getLogWriter().fine("currentObject is serialized object");
        isObjectSerialized = true;
      } else {
        getLogWriter().fine("currentObject is deserialized object");
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
