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

import static org.apache.geode.test.dunit.Assert.assertEquals;
import static org.apache.geode.test.dunit.Assert.assertTrue;
import static org.apache.geode.test.dunit.Assert.fail;

import org.junit.Test;
import org.junit.experimental.categories.Category;

import org.apache.geode.cache.CacheException;
import org.apache.geode.cache.Region;
import org.apache.geode.cache.RegionShortcut;
import org.apache.geode.cache.client.ClientCache;
import org.apache.geode.cache.client.ClientCacheFactory;
import org.apache.geode.cache.client.ClientRegionShortcut;
import org.apache.geode.cache.query.QueryService;
import org.apache.geode.cache.query.SelectResults;
import org.apache.geode.cache.query.Struct;
import org.apache.geode.cache.query.data.PortfolioPdx;
import org.apache.geode.cache.query.data.PositionPdx;
import org.apache.geode.cache.query.functional.StructSetOrResultsSet;
import org.apache.geode.cache.server.CacheServer;
import org.apache.geode.cache30.CacheSerializableRunnable;
import org.apache.geode.internal.AvailablePortHelper;
import org.apache.geode.internal.cache.GemFireCacheImpl;
import org.apache.geode.pdx.PdxInstance;
import org.apache.geode.pdx.PdxInstanceFactory;
import org.apache.geode.pdx.internal.PdxInstanceEnum;
import org.apache.geode.pdx.internal.PdxInstanceFactoryImpl;
import org.apache.geode.pdx.internal.PdxString;
import org.apache.geode.test.dunit.Assert;
import org.apache.geode.test.dunit.Host;
import org.apache.geode.test.dunit.LogWriterUtils;
import org.apache.geode.test.dunit.NetworkUtils;
import org.apache.geode.test.dunit.SerializableCallable;
import org.apache.geode.test.dunit.SerializableRunnable;
import org.apache.geode.test.dunit.VM;
import org.apache.geode.test.junit.categories.OQLQueryTest;

@Category({OQLQueryTest.class})
public class PdxLocalQueryDUnitTest extends PDXQueryTestBase {

  @Test
  public void testLocalPdxQueriesVerifyNoDeserialization() throws Exception {
    final Host host = Host.getHost(0);
    final VM server1 = host.getVM(0);
    final VM server2 = host.getVM(1);

    final int numberOfEntries = 10;
    final String name = "/" + regionName;

    final String[] queries = {"select * from " + name + " where status = 'inactive'",
        "select p from " + name + " p where p.status = 'inactive'",
        "select * from " + name + " p, p.positions.values v where v.secId = 'IBM'",
        "select p.status from " + name + " p where p.status = 'inactive' or p.ID > 0",
        "select * from " + name + " p where p.status = 'inactive' and p.ID >= 0",
        "select p.status from " + name + " p where p.status in set ('inactive', 'active')",
        "select * from " + name + " p where p.ID > 0 and p.ID < 10",};

    // Start server1
    server1.invoke(new SerializableCallable("Create Server1") {
      @Override
      public Object call() throws Exception {
        Region r1 = getCache().createRegionFactory(RegionShortcut.REPLICATE).create(regionName);

        for (int i = 0; i < numberOfEntries; i++) {
          PortfolioPdx p = new PortfolioPdx(i);
          r1.put("key-" + i, p);
        }
        return null;
      }
    });

    // Start server2
    server2.invoke(new SerializableCallable("Create Server2") {
      @Override
      public Object call() throws Exception {
        Region r1 = getCache().createRegionFactory(RegionShortcut.REPLICATE).create(regionName);

        QueryService qs = null;
        SelectResults sr = null;
        // Execute query locally
        try {
          qs = getCache().getQueryService();
        } catch (Exception e) {
          Assert.fail("Failed to get QueryService.", e);
        }

        for (int i = 0; i < queries.length; i++) {
          try {
            sr = (SelectResults) qs.newQuery(queries[i]).execute();
            assertTrue("Size of resultset should be greater than 0 for query: " + queries[i],
                sr.size() > 0);
          } catch (Exception e) {
            Assert.fail("Failed executing query " + queries[i], e);
          }
        }
        assertEquals("Unexpected number of objects deserialized ", 0, PortfolioPdx.numInstance);
        return null;
      }
    });
    this.closeClient(server1);
    this.closeClient(server2);

  }

  @Test
  public void testLocalPdxQueriesReadSerialized() throws Exception {
    final Host host = Host.getHost(0);
    final VM server1 = host.getVM(0);
    final VM server2 = host.getVM(1);

    final int numberOfEntries = 10;
    final String name = "/" + regionName;

    final String[] queries = {"select * from " + name + " where position1 = $1",
        "select * from " + name + " where aDay = $1",
        "select * from " + name + " where status = 'inactive'",
        "select distinct * from " + name + " where status = 'inactive'",
        "select p from " + name + " p where p.status = 'inactive'",
        "select * from " + name + " p, p.positions.values v where v.secId = 'IBM'",
        "select * from " + name + " p where p.status = 'inactive' or p.ID > 0",
        "select * from " + name + " p where p.status = 'inactive' and p.ID >= 0",
        "select * from " + name + " p where p.status in set ('inactive', 'active')",
        "select * from " + name + " p where p.ID > 0 and p.ID < 10",};

    // Start server1
    server1.invoke(new SerializableCallable("Create Server1") {
      @Override
      public Object call() throws Exception {
        Region r1 = getCache().createRegionFactory(RegionShortcut.REPLICATE).create(regionName);

        for (int i = 0; i < numberOfEntries; i++) {
          PortfolioPdx p = new PortfolioPdx(i);
          r1.put("key-" + i, p);
        }
        return null;
      }
    });

    // Start server2
    server2.invoke(new SerializableCallable("Create Server2") {
      @Override
      public Object call() throws Exception {
        ((GemFireCacheImpl) getCache()).setReadSerializedForTest(true);
        Region r1 = getCache().createRegionFactory(RegionShortcut.REPLICATE).create(regionName);

        QueryService qs = null;
        SelectResults sr = null;
        // Execute query locally
        try {
          qs = getCache().getQueryService();
        } catch (Exception e) {
          Assert.fail("Failed to get QueryService.", e);
        }

        PositionPdx pos = new PositionPdx("IBM", 100);
        PdxInstanceFactory out = PdxInstanceFactoryImpl
            .newCreator("org.apache.geode.cache.query.data.PositionPdx", false, getCache());
        out.writeLong("avg20DaysVol", 0);
        out.writeString("bondRating", "");
        out.writeDouble("convRatio", 0);
        out.writeString("country", "");
        out.writeDouble("delta", 0);
        out.writeLong("industry", 0);
        out.writeLong("issuer", 0);
        out.writeDouble("mktValue", pos.getMktValue());
        out.writeDouble("qty", 0);
        out.writeString("secId", pos.secId);
        out.writeString("secIdIndexed", pos.secIdIndexed);
        out.writeString("secLinks", "");
        out.writeDouble("sharesOutstanding", pos.getSharesOutstanding());
        out.writeString("underlyer", "");
        out.writeLong("volatility", 0);
        out.writeInt("pid", pos.getPid());
        out.writeInt("portfolioId", 0);
        out.markIdentityField("secId");
        PdxInstance pi = out.create();

        PortfolioPdx.Day pDay = new PortfolioPdx(1).aDay;
        PdxInstanceEnum pdxEnum = new PdxInstanceEnum(pDay);

        for (int i = 0; i < queries.length; i++) {
          try {
            if (i == 0) {
              sr = (SelectResults) qs.newQuery(queries[i]).execute(new Object[] {pi});
            } else if (i == 1) {
              sr = (SelectResults) qs.newQuery(queries[i]).execute(new Object[] {pdxEnum});
            } else {
              sr = (SelectResults) qs.newQuery(queries[i]).execute();
            }
            assertTrue("Size of resultset should be greater than 0 for query: " + queries[i],
                sr.size() > 0);
            for (Object result : sr) {
              if (result instanceof Struct) {
                Object[] r = ((Struct) result).getFieldValues();
                for (int j = 0; j < r.length; j++) {
                  if (!(r[j] instanceof PdxInstance)) {
                    fail("Result object should be a PdxInstance  and not an instance of "
                        + r[j].getClass() + " for query: " + queries[i]);
                  }
                }
              } else if (!(result instanceof PdxInstance)) {
                fail("Result object should be a PdxInstance  and not an instance of "
                    + result.getClass() + " for query: " + queries[i]);
              }
            }
          } catch (Exception e) {
            Assert.fail("Failed executing query " + queries[i], e);
          }
        }
        return null;
      }
    });
    this.closeClient(server1);
    this.closeClient(server2);

  }

  @Test
  public void testLocalPdxQueries() throws Exception {
    final Host host = Host.getHost(0);
    final VM server1 = host.getVM(1);
    final VM client = host.getVM(2);

    final int numberOfEntries = 10;
    final String name = "/" + regionName;
    final String name2 = "/" + regionName2;
    final String[] queries = {"select * from " + name + " where position1 = $1",
        "select * from " + name + " where aDay = $1",
        "select distinct * from " + name + " p where p.status = 'inactive'", // numberOfEntries
        "select distinct p.status from " + name + " p where p.status = 'inactive'", // 1
        "select p from " + name + " p where p.status = 'inactive'", // numberOfEntries
        "select * from " + name + " p, p.positions.values v where v.secId = 'IBM'", // 4
        "select v from " + name + " p, p.positions.values v where v.secId = 'IBM'", // 4
        "select p.status from " + name + " p where p.status = 'inactive'", // numberOfEntries
        "select distinct * from " + name + " p where p.status = 'inactive' order by p.ID", // numberOfEntries
        "select * from " + name + " p where p.status = 'inactive' or p.ID > 0", // 19
        "select * from " + name + " p where p.status = 'inactive' and p.ID >= 0", // numberOfEntries
        "select * from " + name + " p where p.status in set ('inactive', 'active')", // numberOfEntries*2
        "select * from " + name + " p where p.ID > 0 and p.ID < 10", // 9
        "select v from " + name + " p, p.positions.values v where p.status = 'inactive'", // numberOfEntries*2
        "select v.secId from " + name + " p, p.positions.values v where p.status = 'inactive'", // numberOfEntries*2
        "select distinct p from " + name
            + " p, p.positions.values v where p.status = 'inactive' and v.pid >= 0", // numberOfEntries
        "select distinct p from " + name
            + " p, p.positions.values v where p.status = 'inactive' or v.pid > 0", // numberOfEntries*2
        "select distinct * from " + name + " p, p.positions.values v where p.status = 'inactive'", // numberOfEntries*2
        "select * from " + name + ".values v where v.status = 'inactive'", // numberOfEntries
        "select v from " + name + " v where v in (select p from " + name + " p where p.ID > 0)", // 19
        "select v from " + name + " v where v.status in (select distinct p.status from " + name
            + " p where p.status = 'inactive')", // numberOfEntries
        "select * from " + name + " r1, " + name2 + " r2 where r1.status = r2.status", // 200
        "select * from " + name + " r1, " + name2
            + " r2 where r1.status = r2.status and r1.status = 'active'", // 100
        "select r2.status from " + name + " r1, " + name2
            + " r2 where r1.status = r2.status and r1.status = 'active'", // 100
        "select distinct r2.status from " + name + " r1, " + name2
            + " r2 where r1.status = r2.status and r1.status = 'active'", // 1
        "select * from " + name + " v where v.status = ELEMENT (select distinct p.status from "
            + name + " p where p.status = 'inactive')", // numberOfEntries
    };

    final int[] results = {2, 3, numberOfEntries, 1, numberOfEntries, 4, 4, numberOfEntries,
        numberOfEntries, 19, numberOfEntries, numberOfEntries * 2, 9, numberOfEntries * 2,
        numberOfEntries * 2, numberOfEntries, numberOfEntries * 2, numberOfEntries * 2,
        numberOfEntries, 19, numberOfEntries, 200, 100, 100, 1, numberOfEntries};

    // Start server1
    final int port1 = (Integer) server1.invoke(new SerializableCallable("Create Server1") {
      @Override
      public Object call() throws Exception {
        Region r1 = getCache().createRegionFactory(RegionShortcut.REPLICATE).create(regionName);
        Region r2 = getCache().createRegionFactory(RegionShortcut.REPLICATE).create(regionName2);

        for (int i = 0; i < numberOfEntries; i++) {
          PortfolioPdx p = new PortfolioPdx(i);
          r1.put("key-" + i, p);
          r2.put("key-" + i, p);
        }

        CacheServer server = getCache().addCacheServer();
        int port = AvailablePortHelper.getRandomAvailablePortForDUnitSite();
        server.setPort(port);
        server.start();
        return port;
      }
    });

    // client loads pdx objects on server
    client.invoke(new SerializableCallable("Create client") {
      @Override
      public Object call() throws Exception {
        ClientCacheFactory cf = new ClientCacheFactory();
        cf.addPoolServer(NetworkUtils.getServerHostName(server1.getHost()), port1);
        ClientCache cache = getClientCache(cf);

        Region region =
            cache.createClientRegionFactory(ClientRegionShortcut.CACHING_PROXY).create(regionName);
        Region region2 =
            cache.createClientRegionFactory(ClientRegionShortcut.CACHING_PROXY).create(regionName2);

        for (int i = numberOfEntries; i < numberOfEntries * 2; i++) {
          PortfolioPdx p = new PortfolioPdx(i);
          region.put("key-" + i, p);
          region2.put("key-" + i, p);
        }
        return null;
      }
    });

    // query locally on server1 to verify pdx objects are not deserialized
    server1.invoke(new SerializableCallable("query locally on server1") {
      @Override
      public Object call() throws Exception {
        GemFireCacheImpl cache = (GemFireCacheImpl) getCache();

        QueryService qs = null;
        SelectResults sr = null;
        // Execute query locally
        try {
          qs = getCache().getQueryService();
        } catch (Exception e) {
          Assert.fail("Failed to get QueryService.", e);
        }

        PositionPdx pos = new PositionPdx("IBM", 100);
        PortfolioPdx.Day pDay = new PortfolioPdx(1).aDay;

        for (int i = 0; i < queries.length; i++) {
          try {
            if (i == 0) {
              sr = (SelectResults) qs.newQuery(queries[i]).execute(new Object[] {pos});
            } else if (i == 1) {
              sr = (SelectResults) qs.newQuery(queries[i]).execute(new Object[] {pDay});
            } else {
              sr = (SelectResults) qs.newQuery(queries[i]).execute();
            }

            assertTrue("Size of resultset should be greater than 0 for query: " + queries[i],
                sr.size() > 0);
            assertEquals("Expected and actual results do not match for query: " + queries[i],
                results[i], sr.size());
          } catch (Exception e) {
            Assert.fail("Failed executing query " + queries[i], e);
          }
        }

        int extra = 0;
        if (cache.getLogger().fineEnabled()) {
          extra = 20;
        }
        assertEquals(numberOfEntries * 6 + 1 + extra, PortfolioPdx.numInstance);

        // set readserealized and query
        ((GemFireCacheImpl) getCache()).setReadSerializedForTest(true);

        PdxInstanceFactory out = PdxInstanceFactoryImpl
            .newCreator("org.apache.geode.cache.query.data.PositionPdx", false, getCache());
        out.writeLong("avg20DaysVol", 0);
        out.writeString("bondRating", "");
        out.writeDouble("convRatio", 0);
        out.writeString("country", "");
        out.writeDouble("delta", 0);
        out.writeLong("industry", 0);
        out.writeLong("issuer", 0);
        out.writeDouble("mktValue", pos.getMktValue());
        out.writeDouble("qty", 0);
        out.writeString("secId", pos.secId);
        out.writeString("secIdIndexed", pos.secIdIndexed);
        out.writeString("secLinks", "");
        out.writeDouble("sharesOutstanding", pos.getSharesOutstanding());
        out.writeString("underlyer", "");
        out.writeLong("volatility", 0);
        out.writeInt("pid", pos.getPid());
        out.writeInt("portfolioId", 0);
        // Identity Field.
        out.markIdentityField("secId");
        PdxInstance pi = out.create();

        PdxInstanceEnum pdxEnum = new PdxInstanceEnum(pDay);

        for (int i = 0; i < queries.length; i++) {
          try {
            if (i == 0) {
              sr = (SelectResults) qs.newQuery(queries[i]).execute(new Object[] {pi});
            } else if (i == 1) {
              sr = (SelectResults) qs.newQuery(queries[i]).execute(new Object[] {pdxEnum});
            } else {
              sr = (SelectResults) qs.newQuery(queries[i]).execute();
            }
            assertTrue("Size of resultset should be greater than 0 for query: " + queries[i],
                sr.size() > 0);
            // For distinct queries with a mix of pdx and non pdx objects
            // the hashcodes should be equal for comparison which are not
            // in case of PortfolioPdx
            if (queries[i].indexOf("distinct") == -1) {
              if (i == 0 || i == 1) {
                assertEquals("Expected and actual results do not match for query: " + queries[i], 1,
                    sr.size());
              } else {
                assertEquals("Expected and actual results do not match for query: " + queries[i],
                    results[i], sr.size());
              }
            }
          } catch (Exception e) {
            Assert.fail("Failed executing query " + queries[i], e);
          }
        }

        // reset readserealized and query
        ((GemFireCacheImpl) getCache()).setReadSerializedForTest(false);
        return null;
      }
    });

    // query from client
    client.invoke(new SerializableCallable("Create client") {
      @Override
      public Object call() throws Exception {
        ClientCacheFactory cf = new ClientCacheFactory();
        cf.addPoolServer(NetworkUtils.getServerHostName(server1.getHost()), port1);
        ClientCache cache = getClientCache(cf);

        QueryService qs = null;
        SelectResults sr = null;
        // Execute query remotely
        try {
          qs = cache.getQueryService();
        } catch (Exception e) {
          Assert.fail("Failed to get QueryService.", e);
        }

        PositionPdx pos = new PositionPdx("IBM", 100);
        PortfolioPdx.Day pDay = new PortfolioPdx(1).aDay;

        for (int i = 0; i < queries.length; i++) {
          try {
            if (i == 0) {
              sr = (SelectResults) qs.newQuery(queries[i]).execute(new Object[] {pos});
            } else if (i == 1) {
              sr = (SelectResults) qs.newQuery(queries[i]).execute(new Object[] {pDay});
            } else {
              sr = (SelectResults) qs.newQuery(queries[i]).execute();
            }
            assertTrue("Size of resultset should be greater than 0 for query: " + queries[i],
                sr.size() > 0);
            assertEquals("Expected and actual results do not match for query: " + queries[i],
                results[i], sr.size());
            for (Object result : sr) {
              if (result instanceof Struct) {
                Object[] r = ((Struct) result).getFieldValues();
                for (int j = 0; j < r.length; j++) {
                  if (r[j] instanceof PdxInstance || r[j] instanceof PdxString) {
                    fail("Result object should be a domain object and not an instance of "
                        + r[j].getClass() + " for query: " + queries[i]);
                  }
                }
              } else if (result instanceof PdxInstance || result instanceof PdxString) {
                fail("Result object should be a domain object and not an instance of "
                    + result.getClass() + " for query: " + queries[i]);
              }
            }
          } catch (Exception e) {
            Assert.fail("Failed executing query " + queries[i], e);
          }
        }

        return null;
      }
    });

    // query locally on server1
    server1.invoke(new SerializableCallable("query locally on server1") {
      @Override
      public Object call() throws Exception {
        GemFireCacheImpl cache = (GemFireCacheImpl) getCache();

        QueryService qs = null;
        SelectResults[][] sr = new SelectResults[queries.length][2];
        // Execute query locally
        try {
          qs = getCache().getQueryService();
        } catch (Exception e) {
          Assert.fail("Failed to get QueryService.", e);
        }
        int cnt = PositionPdx.cnt;
        PositionPdx pos = new PositionPdx("IBM", 100);
        PortfolioPdx.Day pDay = new PortfolioPdx(1).aDay;

        for (int i = 0; i < queries.length; i++) {
          try {
            if (i == 0) {
              sr[i][0] = (SelectResults) qs.newQuery(queries[i]).execute(new Object[] {pos});
            } else if (i == 1) {
              sr[i][0] = (SelectResults) qs.newQuery(queries[i]).execute(new Object[] {pDay});
            } else {
              sr[i][0] = (SelectResults) qs.newQuery(queries[i]).execute();
            }

            assertTrue("Size of resultset should be greater than 0 for query: " + queries[i],
                sr[i][0].size() > 0);
            assertEquals("Expected and actual results do not match for query: " + queries[i],
                results[i], sr[i][0].size());
            for (Object result : sr[i][0]) {
              if (result instanceof Struct) {
                Object[] r = ((Struct) result).getFieldValues();
                for (int j = 0; j < r.length; j++) {
                  if (r[j] instanceof PdxInstance || r[j] instanceof PdxString) {
                    fail("Result object should be a domain object and not an instance of "
                        + r[j].getClass() + " for query: " + queries[i]);
                  }
                }
              } else if (result instanceof PdxInstance || result instanceof PdxString) {
                fail("Result object should be a domain object and not an instance of "
                    + result.getClass() + " for query: " + queries[i]);
              }
            }
          } catch (Exception e) {
            Assert.fail("Failed executing query " + queries[i], e);
          }
        }

        // create index
        qs.createIndex("statusIndex", "status", name);
        qs.createIndex("IDIndex", "ID", name);
        qs.createIndex("pIdIndex", "pos.getPid()", name + " p, p.positions.values pos");
        qs.createIndex("secIdIndex", "pos.secId", name + " p, p.positions.values pos");

        for (int i = 0; i < queries.length; i++) {
          try {
            if (i == 0) {
              sr[i][1] = (SelectResults) qs.newQuery(queries[i]).execute(new Object[] {pos});
            } else if (i == 1) {
              sr[i][1] = (SelectResults) qs.newQuery(queries[i]).execute(new Object[] {pDay});
            } else {
              sr[i][1] = (SelectResults) qs.newQuery(queries[i]).execute();
            }

            assertTrue("Size of resultset should be greater than 0 for query: " + queries[i],
                sr[i][1].size() > 0);
            assertEquals("Expected and actual results do not match for query: " + queries[i],
                results[i], sr[i][1].size());
            for (Object result : sr[i][1]) {
              if (result instanceof Struct) {
                Object[] r = ((Struct) result).getFieldValues();
                for (int j = 0; j < r.length; j++) {
                  if (r[j] instanceof PdxInstance || r[j] instanceof PdxString) {
                    fail("Result object should be a domain object and not an instance of "
                        + r[j].getClass() + " for query: " + queries[i]);
                  }
                }
              } else if (result instanceof PdxInstance || result instanceof PdxString) {
                fail("Result object should be a domain object and not an instance of "
                    + result.getClass() + " for query: " + queries[i]);
              }
            }
          } catch (Exception e) {
            Assert.fail("Failed executing query " + queries[i], e);
          }
        }

        StructSetOrResultsSet ssOrrs = new StructSetOrResultsSet();
        ssOrrs.CompareQueryResultsWithoutAndWithIndexes(sr, queries.length, queries);
        return null;
      }
    });

    this.closeClient(client);
    this.closeClient(server1);

  }

  @Test
  public void testLocalPdxQueriesOnPR() throws Exception {
    final Host host = Host.getHost(0);
    final VM server1 = host.getVM(0);
    final VM server2 = host.getVM(1);
    final VM client = host.getVM(2);

    final int numberOfEntries = 10;
    final String name = "/" + regionName;
    final String[] queries = {"select * from " + name + " where position1 = $1",
        "select * from " + name + " where aDay = $1",
        "select distinct * from " + name + " p where p.status = 'inactive'", // numberOfEntries
        "select distinct p.status from " + name + " p where p.status = 'inactive'", // 1
        "select p from " + name + " p where p.status = 'inactive'", // numberOfEntries
        "select * from " + name + " p, p.positions.values v where v.secId = 'IBM'", // 4
        "select v from " + name + " p, p.positions.values v where v.secId = 'IBM'", // 4
        "select p.status from " + name + " p where p.status = 'inactive'", // numberOfEntries
        "select distinct * from " + name + " p where p.status = 'inactive' order by p.ID", // numberOfEntries
        "select * from " + name + " p where p.status = 'inactive' or p.ID > 0", // 19
        "select * from " + name + " p where p.status = 'inactive' and p.ID >= 0", // numberOfEntries
        "select * from " + name + " p where p.status in set ('inactive', 'active')", // numberOfEntries*2
        "select * from " + name + " p where p.ID > 0 and p.ID < 10", // 9
        "select v from " + name + " p, p.positions.values v where p.status = 'inactive'", // numberOfEntries*2
        "select v.secId from " + name + " p, p.positions.values v where p.status = 'inactive'", // numberOfEntries*2
        "select distinct p from " + name
            + " p, p.positions.values v where p.status = 'inactive' and v.pid >= 0", // numberOfEntries
        "select distinct p from " + name
            + " p, p.positions.values v where p.status = 'inactive' or v.pid > 0", // numberOfEntries*2
        "select distinct * from " + name + " p, p.positions.values v where p.status = 'inactive'", // numberOfEntries*2
        "select * from " + name + ".values v where v.status = 'inactive'", // numberOfEntries
        "select v from " + name + " v where v in (select p from " + name + " p where p.ID > 0)", // 19
        "select v from " + name + " v where v.status in (select distinct p.status from " + name
            + " p where p.status = 'inactive')", // numberOfEntries
        "select * from " + name + " v where v.status = ELEMENT (select distinct p.status from "
            + name + " p where p.status = 'inactive')", // numberOfEntries
    };

    final int[] results = {2, 3, numberOfEntries, 1, numberOfEntries, 4, 4, numberOfEntries,
        numberOfEntries, 19, numberOfEntries, numberOfEntries * 2, 9, numberOfEntries * 2,
        numberOfEntries * 2, numberOfEntries, numberOfEntries * 2, numberOfEntries * 2,
        numberOfEntries, 19, numberOfEntries, numberOfEntries};

    // Start server1
    final int port1 = (Integer) server1.invoke(new SerializableCallable("Create Server1") {
      @Override
      public Object call() throws Exception {
        Region r1 = getCache().createRegionFactory(RegionShortcut.PARTITION).create(regionName);
        for (int i = 0; i < numberOfEntries; i++) {
          r1.put("key-" + i, new PortfolioPdx(i));
        }
        CacheServer server = getCache().addCacheServer();
        int port = AvailablePortHelper.getRandomAvailablePortForDUnitSite();
        server.setPort(port);
        server.start();
        return port;
      }
    });

    // Start server2
    final int port2 = (Integer) server2.invoke(new SerializableCallable("Create Server2") {
      @Override
      public Object call() throws Exception {
        Region r1 = getCache().createRegionFactory(RegionShortcut.PARTITION).create(regionName);
        CacheServer server = getCache().addCacheServer();
        int port = AvailablePortHelper.getRandomAvailablePortForDUnitSite();
        server.setPort(port);
        server.start();
        return port;
      }
    });

    // client loads pdx objects on server
    client.invoke(new SerializableCallable("Create client") {
      @Override
      public Object call() throws Exception {
        ClientCacheFactory cf = new ClientCacheFactory();
        cf.addPoolServer(NetworkUtils.getServerHostName(server1.getHost()), port1);
        ClientCache cache = getClientCache(cf);
        Region region =
            cache.createClientRegionFactory(ClientRegionShortcut.CACHING_PROXY).create(regionName);

        for (int i = numberOfEntries; i < numberOfEntries * 2; i++) {
          region.put("key-" + i, new PortfolioPdx(i));
        }

        QueryService qs = null;
        SelectResults sr = null;
        // Execute query remotely
        try {
          qs = cache.getQueryService();
        } catch (Exception e) {
          Assert.fail("Failed to get QueryService.", e);
        }
        PositionPdx pos = new PositionPdx("IBM", 100);
        PortfolioPdx.Day pDay = new PortfolioPdx(1).aDay;

        for (int i = 0; i < queries.length; i++) {
          try {
            if (i == 0) {
              sr = (SelectResults) qs.newQuery(queries[i]).execute(new Object[] {pos});
            } else if (i == 1) {
              sr = (SelectResults) qs.newQuery(queries[i]).execute(new Object[] {pDay});
            } else {
              sr = (SelectResults) qs.newQuery(queries[i]).execute();
            }
            assertTrue("Size of resultset should be greater than 0 for query: " + queries[i],
                sr.size() > 0);
            assertEquals("Expected and actual results do not match for query: " + queries[i],
                results[i], sr.size());

            for (Object result : sr) {
              if (result instanceof Struct) {
                Object[] r = ((Struct) result).getFieldValues();
                for (int j = 0; j < r.length; j++) {
                  if (r[j] instanceof PdxInstance || r[j] instanceof PdxString) {
                    fail("Result object should be a domain object and not an instance of "
                        + r[j].getClass() + " for query: " + queries[i]);
                  }
                }
              } else if (result instanceof PdxInstance || result instanceof PdxString) {
                fail("Result object should be a domain object and not an instance of "
                    + result.getClass() + " for query: " + queries[i]);
              }
            }
          } catch (Exception e) {
            Assert.fail("Failed executing query " + queries[i], e);
          }
        }

        return null;
      }
    });

    // query locally on server1
    server1.invoke(new SerializableCallable("query locally on server1") {
      @Override
      public Object call() throws Exception {
        GemFireCacheImpl cache = (GemFireCacheImpl) getCache();

        QueryService qs = null;
        SelectResults sr = null;
        // Execute query locally
        try {
          qs = getCache().getQueryService();
        } catch (Exception e) {
          Assert.fail("Failed to get QueryService.", e);
        }

        PositionPdx pos = new PositionPdx("IBM", 100);
        PortfolioPdx.Day pDay = new PortfolioPdx(1).aDay;

        for (int i = 0; i < queries.length; i++) {
          try {
            if (i == 0) {
              sr = (SelectResults) qs.newQuery(queries[i]).execute(new Object[] {pos});
            } else if (i == 1) {
              sr = (SelectResults) qs.newQuery(queries[i]).execute(new Object[] {pDay});
            } else {
              sr = (SelectResults) qs.newQuery(queries[i]).execute();
            }
            assertTrue("Size of resultset should be greater than 0 for query: " + queries[i],
                sr.size() > 0);
            assertEquals("Expected and actual results do not match for query: " + queries[i],
                results[i], sr.size());

            for (Object result : sr) {
              if (result instanceof Struct) {
                Object[] r = ((Struct) result).getFieldValues();
                for (int j = 0; j < r.length; j++) {
                  if (r[j] instanceof PdxInstance || r[j] instanceof PdxString) {
                    fail("Result object should be a domain object and not an instance of "
                        + r[j].getClass() + " for query: " + queries[i]);
                  }
                }
              } else if (result instanceof PdxInstance || result instanceof PdxString) {
                fail("Result object should be a domain object and not an instance of "
                    + result.getClass() + " for query: " + queries[i]);
              }
            }
          } catch (Exception e) {
            Assert.fail("Failed executing query " + queries[i], e);
          }
        }

        return null;
      }
    });

    // query locally on server2
    server2.invoke(new SerializableCallable("query locally on server2") {
      @Override
      public Object call() throws Exception {
        GemFireCacheImpl cache = (GemFireCacheImpl) getCache();

        QueryService qs = null;
        SelectResults[][] sr = new SelectResults[queries.length][2];
        // Execute query locally
        try {
          qs = getCache().getQueryService();
        } catch (Exception e) {
          Assert.fail("Failed to get QueryService.", e);
        }

        PositionPdx pos = new PositionPdx("IBM", 100);
        PortfolioPdx.Day pDay = new PortfolioPdx(1).aDay;

        for (int i = 0; i < queries.length; i++) {
          try {
            if (i == 0) {
              sr[i][0] = (SelectResults) qs.newQuery(queries[i]).execute(new Object[] {pos});
            } else if (i == 1) {
              sr[i][0] = (SelectResults) qs.newQuery(queries[i]).execute(new Object[] {pDay});
            } else {
              sr[i][0] = (SelectResults) qs.newQuery(queries[i]).execute();
            }
            assertTrue("Size of resultset should be greater than 0 for query: " + queries[i],
                sr[i][0].size() > 0);
            assertEquals("Expected and actual results do not match for query: " + queries[i],
                results[i], sr[i][0].size());

            for (Object result : sr[i][0]) {
              if (result instanceof Struct) {
                Object[] r = ((Struct) result).getFieldValues();
                for (int j = 0; j < r.length; j++) {
                  if (r[j] instanceof PdxInstance || r[j] instanceof PdxString) {
                    fail("Result object should be a domain object and not an instance of "
                        + r[j].getClass() + " for query: " + queries[i]);
                  }
                }
              } else if (result instanceof PdxInstance || result instanceof PdxString) {
                fail("Result object should be a domain object and not an instance of "
                    + result.getClass() + " for query: " + queries[i]);
              }
            }
          } catch (Exception e) {
            Assert.fail("Failed executing query " + queries[i], e);
          }
        }

        // create index
        qs.createIndex("statusIndex", "p.status", name + " p");
        qs.createIndex("IDIndex", "ID", name);
        qs.createIndex("pIdIndex", "pos.getPid()", name + " p, p.positions.values pos");
        qs.createIndex("secIdIndex", "pos.secId", name + " p, p.positions.values pos");

        for (int i = 0; i < queries.length; i++) {
          try {
            if (i == 0) {
              sr[i][1] = (SelectResults) qs.newQuery(queries[i]).execute(new Object[] {pos});
            } else if (i == 1) {
              sr[i][1] = (SelectResults) qs.newQuery(queries[i]).execute(new Object[] {pDay});
            } else {
              sr[i][1] = (SelectResults) qs.newQuery(queries[i]).execute();
            }
            assertTrue("Size of resultset should be greater than 0 for query: " + queries[i],
                sr[i][1].size() > 0);
            assertEquals("Expected and actual results do not match for query: " + queries[i],
                results[i], sr[i][1].size());

            for (Object result : sr[i][1]) {
              if (result instanceof Struct) {
                Object[] r = ((Struct) result).getFieldValues();
                for (int j = 0; j < r.length; j++) {
                  if (r[j] instanceof PdxInstance || r[j] instanceof PdxString) {
                    fail("Result object should be a domain object and not an instance of "
                        + r[j].getClass() + " for query: " + queries[i]);
                  }
                }
              } else if (result instanceof PdxInstance || result instanceof PdxString) {
                fail("Result object should be a domain object and not an instance of "
                    + result.getClass() + " for query: " + queries[i]);
              }
            }
          } catch (Exception e) {
            Assert.fail("Failed executing query " + queries[i], e);
          }
        }

        StructSetOrResultsSet ssOrrs = new StructSetOrResultsSet();
        ssOrrs.CompareQueryResultsWithoutAndWithIndexes(sr, queries.length, queries);

        return null;
      }
    });

    this.closeClient(client);
    this.closeClient(server1);
    this.closeClient(server2);
  }

  /* Close Client */
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

  @Override
  protected final void preTearDownPDXQueryTestBase() throws Exception {
    disconnectAllFromDS(); // tests all expect to create a new ds
    // Reset the testObject numinstance for the next test.
    TestObject.numInstance = 0;
    PortfolioPdx.DEBUG = false;
    // In all VM.
    resetTestObjectInstanceCount();
  }

  @Override
  public final void postSetUp() throws Exception {
    resetTestObjectInstanceCount();
  }

  private void resetTestObjectInstanceCount() {
    final Host host = Host.getHost(0);
    for (int i = 0; i < 4; i++) {
      VM vm = host.getVM(i);
      vm.invoke(new CacheSerializableRunnable("Create cache server") {
        public void run2() throws CacheException {
          TestObject.numInstance = 0;
          PortfolioPdx.numInstance = 0;
          PositionPdx.numInstance = 0;
          PositionPdx.cnt = 0;
          TestObject2.numInstance = 0;
        }
      });
    }
  }

}
