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
package com.gemstone.gemfire.cache.query;

import com.gemstone.gemfire.cache.Cache;
import com.gemstone.gemfire.cache.CacheFactory;
import com.gemstone.gemfire.cache.Region;
import com.gemstone.gemfire.cache.query.internal.DefaultQuery;
import com.gemstone.gemfire.cache.query.internal.QueryObserver;
import com.gemstone.gemfire.cache.query.internal.index.CompactRangeIndex;
import com.gemstone.gemfire.cache.query.internal.index.IndexStore.IndexStoreEntry;
import com.gemstone.gemfire.cache.query.internal.index.PrimaryKeyIndex;
import com.gemstone.gemfire.cache.query.internal.index.RangeIndex;
import com.gemstone.gemfire.internal.cache.LocalRegion.NonTXEntry;
import com.gemstone.gemfire.internal.cache.RegionEntry;
import com.gemstone.gemfire.internal.cache.persistence.query.CloseableIterator;
import com.gemstone.gemfire.pdx.PdxInstance;
import com.gemstone.gemfire.pdx.PdxInstanceFactory;
import com.gemstone.gemfire.pdx.internal.PdxInstanceFactoryImpl;
import com.gemstone.gemfire.pdx.internal.PdxInstanceImpl;
import com.gemstone.gemfire.pdx.internal.PdxString;
import com.gemstone.gemfire.test.junit.categories.IntegrationTest;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.junit.experimental.categories.Category;

import java.util.*;

import static com.gemstone.gemfire.distributed.ConfigurationProperties.MCAST_PORT;
import static org.junit.Assert.*;

/**
 * 
 * 
 */
@Category(IntegrationTest.class)
public class PdxStringQueryJUnitTest {
  private Cache c;
  private Region r;
  private String regName = "exampleRegion";
  QueryService qs;
  QueryObserver observer;

  private static final int NO_INDEX = 0;
  private static final int INDEX_TYPE_COMPACTRANGE = 0;
  private static final int INDEX_TYPE_PRIMARYKEY = 2;
  private static final int INDEX_TYPE_RANGE = 1;

  @Before
  public void setUp() {
    this.c = new CacheFactory().set(MCAST_PORT, "0").create();
    r = c.createRegionFactory().create(regName);
    qs = c.getQueryService();
  }

  @After
  public void tearDown() {
    this.c.close();
  }

  @Test
  public void testQueriesPdxInstances() throws Exception {
    putPdxInstances();
    executeQueriesValidateResults(NO_INDEX);
    r.clear();
  }
  
  @Test
  public void testQueriesHeterogenousObjects() throws Exception {
    putHeterogeneousObjects();
    executeQueriesValidateResults(NO_INDEX);
    r.clear();
  }

  @Test
  public void testQueriesWithCompactRangeIndexPdxInstances() throws Exception {
    Index index = qs.createIndex("index1", "secId", "/exampleRegion");
    assertTrue(index instanceof CompactRangeIndex);
    putPdxInstances();
    CloseableIterator<IndexStoreEntry> indexIterator = null;
    try {
      indexIterator = ((CompactRangeIndex) index).getIndexStorage().iterator(
          null);
      while (indexIterator.hasNext()) {
        assertTrue(indexIterator.next().getDeserializedKey() instanceof PdxString);
      }
    } finally {
      if(indexIterator != null){
        indexIterator.close();
      }
    }
    executeQueriesValidateResults(INDEX_TYPE_COMPACTRANGE);
    r.clear();
  }
  
  @Test
  public void testQueriesWithCompactRangeIndexPdxInstancesREUpdateInProgress() throws Exception {
    Index index = qs.createIndex("index1", "secId", "/exampleRegion");
    assertTrue(index instanceof CompactRangeIndex);
    putPdxInstancesWithREUpdateInProgress();
    CloseableIterator<IndexStoreEntry> indexIterator = null;
    try {
      indexIterator = ((CompactRangeIndex) index).getIndexStorage().iterator(
          null);
      while (indexIterator.hasNext()) {
        assertTrue(indexIterator.next().getDeserializedKey() instanceof PdxString);
      }
    } finally {
      if(indexIterator != null){
        indexIterator.close();
      }
    }
    executeQueriesValidateResults(INDEX_TYPE_COMPACTRANGE);
    r.clear();
  }
  
  @Test
  public void testQueriesWithCompactRangeIndexHeterogenousObjects() throws Exception {
    putHeterogeneousObjects();
    executeQueriesValidateResults(INDEX_TYPE_COMPACTRANGE);
    r.clear();
  }

  @Test
  public void testQueriesWithRangeIndex() throws Exception {
    Index index = qs.createIndex("index2", "p.secId",
        "/exampleRegion p, p.positions.values");
    assertTrue(index instanceof RangeIndex);
    PdxInstanceFactory pf = PdxInstanceFactoryImpl.newCreator("Portfolio",
        false);
    pf.writeInt("ID", 111);
    pf.writeString("secId", "IBM");
    pf.writeString("status", "active");
    HashMap positions = new HashMap();
    positions.put("price", "50");
    positions.put("price", "60");
    pf.writeObject("positions", positions);
    PdxInstance pi = pf.create();

    r.put("IBM", pi);

    positions = new HashMap();
    positions.put("price", "100");
    positions.put("price", "120");
    r.put("YHOO", new TestObject(222, "YHOO", positions,"inactive"));

    pf = PdxInstanceFactoryImpl.newCreator("Portfolio", false);
    pf.writeInt("ID", 333);
    pf.writeString("secId", "GOOGL");
    pf.writeString("status", "active");
    positions = new HashMap();
    positions.put("price", "130");
    positions.put("price", "150");
    pf.writeObject("positions", positions);
    pi = pf.create();

    positions = new HashMap();
    positions.put("price", "200");
    positions.put("price", "220");
    r.put("VMW", new TestObject(111, "VMW", positions,"inactive"));

    r.put("GOOGL", pi);

    Map map = ((RangeIndex) index).getValueToEntriesMap();
    for (Object key : map.keySet()) {
      assertTrue(key instanceof PdxString);
    }
    
    executeQueriesValidateResults(INDEX_TYPE_RANGE);
    qs.removeIndex(index);
    r.clear();
  }

  @Test
  public void testQueriesWithRangeIndexWithREUpdateInProgress() throws Exception {
    Index index = qs.createIndex("index2", "p.secId",
        "/exampleRegion p, p.positions.values");
    assertTrue(index instanceof RangeIndex);
    PdxInstanceFactory pf = PdxInstanceFactoryImpl.newCreator("Portfolio",
        false);
    pf.writeInt("ID", 111);
    pf.writeString("secId", "IBM");
    pf.writeString("status", "active");
    HashMap positions = new HashMap();
    positions.put("price", "50");
    positions.put("price", "60");
    pf.writeObject("positions", positions);
    PdxInstance pi = pf.create();

    r.put("IBM", pi);

    positions = new HashMap();
    positions.put("price", "100");
    positions.put("price", "120");
    r.put("YHOO", new TestObject(222, "YHOO", positions,"inactive"));

    pf = PdxInstanceFactoryImpl.newCreator("Portfolio", false);
    pf.writeInt("ID", 333);
    pf.writeString("secId", "GOOGL");
    pf.writeString("status", "active");
    positions = new HashMap();
    positions.put("price", "130");
    positions.put("price", "150");
    pf.writeObject("positions", positions);
    pi = pf.create();

    positions = new HashMap();
    positions.put("price", "200");
    positions.put("price", "220");
    r.put("VMW", new TestObject(111, "VMW", positions,"inactive"));

    r.put("GOOGL", pi);
    makeREUpdateInProgress();
    
    Map map = ((RangeIndex) index).getValueToEntriesMap();
    for (Object key : map.keySet()) {
      assertTrue(key instanceof PdxString);
    }
    DefaultQuery.setPdxReadSerialized(true);
    executeQueriesValidateResults(INDEX_TYPE_RANGE);
    qs.removeIndex(index);
    r.clear();
  }

  @Test
  public void testQueriesWithPrimaryKeyIndex() throws Exception {
    Index index = qs.createKeyIndex("index3", "secId", "/exampleRegion");
    assertTrue(index instanceof PrimaryKeyIndex);
    putPdxInstances();
    executeQueriesValidateResults(INDEX_TYPE_PRIMARYKEY);
    r.clear();
    putHeterogeneousObjects();
    executeQueriesValidateResults(INDEX_TYPE_PRIMARYKEY);
    qs.removeIndex(index);
    r.clear();
  }

  @Test
  public void testStringMethods() throws Exception {
    putPdxInstances();
    String queries[] = {
        "select secId from /exampleRegion where secId.toLowerCase()  = 'ibm'",
        "select secId from /exampleRegion where secId.startsWith('I')" };
    for (int i = 0; i < queries.length; i++) {
      SelectResults res = (SelectResults) qs.newQuery(queries[i]).execute();
      assertEquals("Incorrect result size returned for query. " + queries[i],
          1, res.size());
      validateStringResult("IBM", res.iterator().next());
    }
    r.clear();
  }

  public void putPdxInstances() throws Exception {
    PdxInstanceFactory pf = PdxInstanceFactoryImpl.newCreator("Portfolio",
        false);
    pf.writeInt("ID", 111);
    pf.writeString("status", "active");
    pf.writeString("secId", "IBM");
    PdxInstance pi = pf.create();
    r.put("IBM", pi);

    pf = PdxInstanceFactoryImpl.newCreator("Portfolio", false);
    pf.writeInt("ID", 222);
    pf.writeString("status", "inactive");
    pf.writeString("secId", "YHOO");
    pi = pf.create();
    r.put("YHOO", pi);

    pf = PdxInstanceFactoryImpl.newCreator("Portfolio", false);
    pf.writeInt("ID", 333);
    pf.writeString("status", "active");
    pf.writeString("secId", "GOOGL");
    pi = pf.create();
    r.put("GOOGL", pi);

    pf = PdxInstanceFactoryImpl.newCreator("Portfolio", false);
    pf.writeInt("ID", 111);
    pf.writeString("status", "inactive");
    pf.writeString("secId", "VMW");
    pi = pf.create();
    r.put("VMW", pi);
  }
  
  public void putPdxInstancesWithREUpdateInProgress() throws Exception {
    PdxInstanceFactory pf = PdxInstanceFactoryImpl.newCreator("Portfolio",
        false);
    pf.writeInt("ID", 111);
    pf.writeString("status", "active");
    pf.writeString("secId", "IBM");
    PdxInstance pi = pf.create();
    r.put("IBM", pi);

    pf = PdxInstanceFactoryImpl.newCreator("Portfolio", false);
    pf.writeInt("ID", 222);
    pf.writeString("status", "inactive");
    pf.writeString("secId", "YHOO");
    pi = pf.create();
    r.put("YHOO", pi);

    pf = PdxInstanceFactoryImpl.newCreator("Portfolio", false);
    pf.writeInt("ID", 333);
    pf.writeString("status", "active");
    pf.writeString("secId", "GOOGL");
    pi = pf.create();
    r.put("GOOGL", pi);

    pf = PdxInstanceFactoryImpl.newCreator("Portfolio", false);
    pf.writeInt("ID", 111);
    pf.writeString("status", "inactive");
    pf.writeString("secId", "VMW");
    pi = pf.create();
    r.put("VMW", pi);
    
    makeREUpdateInProgress();
  }
  
  public void makeREUpdateInProgress(){
    Iterator entryItr = r.entrySet().iterator();
    while (entryItr.hasNext()) {
      Region.Entry nonTxEntry = (Region.Entry) entryItr.next();
      RegionEntry entry = ((NonTXEntry)nonTxEntry).getRegionEntry();
      entry.setUpdateInProgress(true);
      assertTrue(entry.isUpdateInProgress());
    }
  }

  public void putHeterogeneousObjects() throws Exception {
    PdxInstanceFactory pf = PdxInstanceFactoryImpl.newCreator("Portfolio",
        false);
    pf.writeInt("ID", 111);
    pf.writeString("secId", "IBM");
    pf.writeString("status", "active");
    PdxInstance pi = pf.create();
    r.put("IBM", pi);

    r.put("YHOO", new TestObject(222, "YHOO","inactive"));
    r.put("GOOGL", new TestObject(333, "GOOGL","active"));

    pf = PdxInstanceFactoryImpl.newCreator("Portfolio", false);
    pf.writeInt("ID", 111);
    pf.writeString("secId", "VMW");
    pf.writeString("status", "inactive");
    pi = pf.create();
    r.put("VMW", pi);
  }

  private void executeQueriesValidateResults(int indexType) throws Exception {
    DefaultQuery.setPdxReadSerialized(true);

    String[] query = { "select count(*) from /exampleRegion",
        "select count(*) from /exampleRegion p, p.positions.values v",
        "select count(*) from /exampleRegion" };

    SelectResults res = (SelectResults) qs.newQuery(query[indexType]).execute();
    assertEquals(4, res.iterator().next());

    query = new String[] {
        "select secId from /exampleRegion where secId  = 'IBM'",
        "select p.secId from /exampleRegion p, p.positions.values v where p.secId = 'IBM'",
        "select secId from /exampleRegion where secId  = 'IBM'" };
    res = (SelectResults) qs.newQuery(query[indexType]).execute();
    assertEquals(1, res.size());
    validateStringResult("IBM", res.iterator().next());

    query = new String[] {
        "select p.secId from /exampleRegion p where p.secId  = ELEMENT(select e.secId from /exampleRegion e where e.secId  = 'IBM') ",
        "select p.secId from /exampleRegion p, p.positions.values v where p.secId = ELEMENT(select p1.secId from /exampleRegion p1, p.positions.values v1 where p1.secId = 'IBM')",
        "select p.secId from /exampleRegion p where p.secId  = ELEMENT(select e.secId from /exampleRegion e where e.secId  = 'IBM' )" };
    res = (SelectResults) qs.newQuery(query[indexType]).execute();
    assertEquals(1, res.size());
    validateStringResult("IBM", res.iterator().next());

    query = new String[] {
        "select secId from /exampleRegion where secId LIKE 'VMW'",
        "select p.secId from /exampleRegion p, p.positions.values v where p.secId LIKE 'VMW'",
        "select secId from /exampleRegion where secId LIKE 'VMW'" };
    res = (SelectResults) qs.newQuery(query[indexType]).execute();
    assertEquals(1, res.size());
    validateStringResult("VMW", res.iterator().next());

    query = new String[] {
        "select secId from /exampleRegion where secId LIKE 'VM%'",
        "select p.secId from /exampleRegion p, p.positions.values v where p.secId LIKE 'VM%'",
        "select secId from /exampleRegion where secId LIKE 'VM%'" };
    res = (SelectResults) qs.newQuery(query[indexType]).execute();
    assertEquals(1, res.size());
    validateStringResult("VMW", res.iterator().next());

    query = new String[] {
        "select secId from /exampleRegion where secId IN SET ('YHOO', 'VMW')",
        "select p.secId from /exampleRegion p, p.positions.values v where p.secId  IN SET ('YHOO', 'VMW')",
        "select secId from /exampleRegion where secId IN SET ('YHOO', 'VMW')" };
    res = (SelectResults) qs.newQuery(query[indexType]).execute();
    assertEquals(2, res.size());
    List secIdsList = new ArrayList();
    secIdsList.add("VMW");
    secIdsList.add("YHOO");

    Iterator iter = res.iterator();
    while (iter.hasNext()) {
      validateResult(secIdsList, iter.next());
    }
    
    query = new String[] {
        "select p.secId from /exampleRegion p where p.secId IN  (select e.secId from /exampleRegion e where e.secId ='YHOO' or e.secId = 'VMW')",
        "select p.secId from /exampleRegion p, p.positions.values v where p.secId  IN  (select e.secId from /exampleRegion e where e.secId ='YHOO' or e.secId = 'VMW')",
        "select p.secId from /exampleRegion p where p.secId IN  (select e.secId from /exampleRegion e where e.secId ='YHOO' or e.secId = 'VMW')" };
    res = (SelectResults) qs.newQuery(query[indexType]).execute();
    assertEquals(2, res.size());
    secIdsList = new ArrayList();
    secIdsList.add("VMW");
    secIdsList.add("YHOO");

    iter = res.iterator();
    while (iter.hasNext()) {
      validateResult(secIdsList, iter.next());
    }
  
    query = new String[] {
        "select secId, status from /exampleRegion where secId = 'IBM'",
        "select p.secId, p.status from /exampleRegion p, p.positions.values v where p.secId = 'IBM'",
        "select secId, status from /exampleRegion where secId = 'IBM'"};
    res = (SelectResults) qs.newQuery(query[indexType]).execute();
    assertEquals(1, res.size());
    secIdsList = new ArrayList();
    secIdsList.add("active");
    secIdsList.add("IBM");
    Struct rs = (Struct) res.iterator().next();
    Object o1 = rs.getFieldValues()[0];
    Object o2 = rs.getFieldValues()[1];

    validateResult(secIdsList, o1);
    validateResult(secIdsList, o2);


    query = new String[] {
        "select secId from /exampleRegion where secId < 'YHOO'",
        "select p.secId from /exampleRegion p, p.positions.values v where p.secId < 'YHOO'",
        "select secId from /exampleRegion where secId < 'YHOO'" };
    res = (SelectResults) qs.newQuery(query[indexType]).execute();
    assertEquals(3, res.size());
    iter = res.iterator();
    secIdsList.clear();
    secIdsList.add("VMW");
    secIdsList.add("GOOGL");
    secIdsList.add("IBM");
    while (iter.hasNext()) {
      validateResult(secIdsList, iter.next());
    }
    
    query = new String[] {
        "select secId from /exampleRegion where 'YHOO' > secId",
        "select p.secId from /exampleRegion p, p.positions.values v where 'YHOO' >  p.secId",
        "select secId from /exampleRegion where 'YHOO' > secId" };
    res = (SelectResults) qs.newQuery(query[indexType]).execute();
    assertEquals(3, res.size());
    iter = res.iterator();
    secIdsList.clear();
    secIdsList.add("VMW");
    secIdsList.add("GOOGL");
    secIdsList.add("IBM");
    while (iter.hasNext()) {
      validateResult(secIdsList, iter.next());
    }

    query = new String[] {
        "select secId from /exampleRegion where secId > 'IBM'",
        "select p.secId from /exampleRegion p, p.positions.values v where p.secId > 'IBM'",
        "select secId from /exampleRegion where secId > 'IBM'" };
    res = (SelectResults) qs.newQuery(query[indexType]).execute();
    assertEquals(2, res.size());
    iter = res.iterator();
    secIdsList.clear();
    secIdsList.add("VMW");
    secIdsList.add("YHOO");
    while (iter.hasNext()) {
      validateResult(secIdsList, iter.next());
    }

    query = new String[] {
        "select secId from /exampleRegion where secId > 'IBM' or ID=333",
        "select p.secId from /exampleRegion p, p.positions.values v where p.secId > 'IBM' or p.ID=333",
        "select secId from /exampleRegion where secId = 'VMW' or secId = 'YHOO' or secId = 'GOOGL'" };
    res = (SelectResults) qs.newQuery(query[indexType]).execute();
    assertEquals(3, res.size());
    iter = res.iterator();
    secIdsList.clear();
    secIdsList.add("VMW");
    secIdsList.add("YHOO");
    secIdsList.add("GOOGL");
    while (iter.hasNext()) {
      validateResult(secIdsList, iter.next());
    }

    query = new String[] {
        "select secId from /exampleRegion where secId > 'IBM' and secId < 'YHOO'",
        "select p.secId from /exampleRegion p, p.positions.values v where p.secId > 'IBM' and p.secId < 'YHOO'",
        "select secId from /exampleRegion where secId > 'IBM' and secId < 'YHOO'" };
    res = (SelectResults) qs.newQuery(query[indexType]).execute();
    assertEquals(1, res.size());
    iter = res.iterator();
    secIdsList.clear();
    secIdsList.add("VMW");
    while (iter.hasNext()) {
      validateResult(secIdsList, iter.next());
    }

    query = new String[] {
        "select secId from /exampleRegion where ID = 111",
        "select p.secId from /exampleRegion p, p.positions.values v where p.ID = 111",
        "select secId from /exampleRegion where secId = 'VMW' or secId = 'IBM'" };
    res = (SelectResults) qs.newQuery(query[indexType]).execute();
    assertEquals(2, res.size());
    iter = res.iterator();
    secIdsList.clear();
    secIdsList.add("VMW");
    secIdsList.add("IBM");
    while (iter.hasNext()) {
      validateResult(secIdsList, iter.next());
    }

    query = new String[] {
        "select distinct ID from /exampleRegion where ID = 111",
        "select distinct p.ID from /exampleRegion p, p.positions.values v where p.ID = 111",
        "select distinct secId from /exampleRegion where secId = 'VMW'" };
    res = (SelectResults) qs.newQuery(query[indexType]).execute();
    assertEquals(1, res.size());

    query = new String[] {
        "select ID from /exampleRegion where ID = 111 limit 1",
        "select p.ID from /exampleRegion p, p.positions.values v where p.ID = 111 limit 1",
        "select secId from /exampleRegion where secId = 'VMW' limit 1" };
    res = (SelectResults) qs.newQuery(query[indexType]).execute();
    assertEquals(1, res.size());

    query = new String[] {
        "select distinct secId from /exampleRegion order by secId",
        "select distinct p.secId from /exampleRegion p, p.positions.values order by p.secId",
        "select distinct secId from /exampleRegion order by secId" };
    res = (SelectResults) qs.newQuery(query[indexType]).execute();
    assertEquals(4, res.size());
    iter = res.iterator();
    String[] secIds = { "GOOGL", "IBM", "VMW", "YHOO" };
    int i = 0;
    while (iter.hasNext()) {
      validateStringResult(secIds[i++], iter.next());
    }

    query = new String[] {
        "select distinct * from /exampleRegion order by secId",
        "select distinct * from /exampleRegion p, p.positions.values v  order by p.secId",
        "select distinct * from /exampleRegion order by secId" };
    res = (SelectResults) qs.newQuery(query[indexType]).execute();
    assertEquals(4, res.size());
    iter = res.iterator();
    secIds = new String[] { "GOOGL", "IBM", "VMW", "YHOO" };
    i = 0;
    while (iter.hasNext()) {
      Object o = iter.next();
      if (o instanceof PdxInstanceImpl) {
        validateStringResult(secIds[i++],
            ((PdxInstanceImpl) o).getField("secId"));
      } else if (o instanceof TestObject) {
        validateStringResult(secIds[i++], ((TestObject) o).getSecId());
      }
    }

    query = new String[] {
        "select distinct secId from /exampleRegion order by secId limit 2",
        "select distinct p.secId from /exampleRegion p, p.positions.values v  order by p.secId limit 2",
        "select distinct secId from /exampleRegion order by secId limit 2" };
    res = (SelectResults) qs.newQuery(query[indexType]).execute();
    assertEquals(2, res.size());
    iter = res.iterator();
    secIds = new String[] { "GOOGL", "IBM" };
    i = 0;
    while (iter.hasNext()) {
      validateStringResult(secIds[i++], iter.next());
    }

    query = new String[] {
        "select secId from /exampleRegion where NOT (secId = 'VMW')",
        "select p.secId from /exampleRegion p, p.positions.values v where  NOT (p.secId = 'VMW')",
        "select secId from /exampleRegion where NOT (secId = 'VMW')" };
    res = (SelectResults) qs.newQuery(query[indexType]).execute();
    assertEquals(3, res.size());
    iter = res.iterator();
    secIdsList.clear();
    secIdsList.add("YHOO");
    secIdsList.add("IBM");
    secIdsList.add("GOOGL");
    while (iter.hasNext()) {
      validateResult(secIdsList, iter.next());
    }

    query = new String[] {
        "select secId from /exampleRegion p where NOT (p.ID IN SET(111, 222)) ",
        "select p.secId from /exampleRegion p, p.positions.values v where NOT (p.ID IN SET(111, 222)) ",
        "select secId from /exampleRegion where NOT (secId IN SET('VMW','IBM','YHOO'))" };
    res = (SelectResults) qs.newQuery(query[indexType]).execute();
    assertEquals(1, res.size());
    iter = res.iterator();
    secIdsList.clear();
    secIdsList.add("GOOGL");
    while (iter.hasNext()) {
      validateResult(secIdsList, iter.next());
    }
    
    query = new String[] {
        "select secId from /exampleRegion where secId  = $1",
        "select p.secId from /exampleRegion p, p.positions.values v where p.secId = $1",
        "select secId from /exampleRegion where secId  = $1" };
    res = (SelectResults) qs.newQuery(query[indexType]).execute(new Object[]{"IBM"});
    assertEquals(1, res.size());
    validateStringResult("IBM", res.iterator().next());
    
    query = new String[] {
        "select secId from /exampleRegion where secId > $1 and secId < $2",
        "select p.secId from /exampleRegion p, p.positions.values v where p.secId > $1 and p.secId < $2",
        "select secId from /exampleRegion where secId > $1 and secId < $2" };
    res = (SelectResults) qs.newQuery(query[indexType]).execute(new Object[]{"IBM", "YHOO"});
    assertEquals(1, res.size());
    iter = res.iterator();
    secIdsList.clear();
    secIdsList.add("VMW");
    while (iter.hasNext()) {
      validateResult(secIdsList, iter.next());
    }
    DefaultQuery.setPdxReadSerialized(false);

  }

  private void validateStringResult(Object str1, Object str2) {
    if (str1 instanceof String && str2 instanceof PdxString) {
      assertEquals(str1, str2.toString());
    } else if (str1 instanceof PdxString && str2 instanceof String) {
      assertEquals(str1.toString(), str2);
    } else if ((str1 instanceof PdxString && str2 instanceof PdxString)
        || (str1 instanceof String && str2 instanceof String)) {
      assertEquals(str1, str2);
    } else {
      fail("Not String or PdxString objects");
    }
  }

  private void validateResult(List list, Object str2) {
    if (str2 instanceof PdxString) {
      str2 = str2.toString();
    }
    assertTrue(list.contains(str2));
  }

  public static class TestObject {
    private int ID;
    private String secId;
    private String status;
    private Map positions;

    public TestObject(int id, String secId, String status) {
      this.ID = id;
      this.secId = secId;
      this.status = status;
    }

    public TestObject(int id, String secId, Map positions, String status) {
      this.ID = id;
      this.secId = secId;
      this.positions = positions;
      this.status = status;
    }

    public int getID() {
      return ID;
    }

    public void setID(int iD) {
      ID = iD;
    }

    public String getSecId() {
      return secId;
    }

    public void setSecId(String secId) {
      this.secId = secId;
    }

    public Map getPositions() {
      return positions;
    }

    public void setPositions(Map positions) {
      this.positions = positions;
    }

    public String getStatus() {
      return status;
    }

    public void setStatus(String status) {
      this.status = status;
    }
    
  }
}
