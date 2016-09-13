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
package com.gemstone.gemfire.internal.cache.persistence.query;

import static org.junit.Assert.*;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.TreeMap;

import org.junit.Test;
import org.junit.experimental.categories.Category;

import com.gemstone.gemfire.internal.cache.CachedDeserializable;
import com.gemstone.gemfire.internal.cache.persistence.query.IndexMap.IndexEntry;
import com.gemstone.gemfire.internal.cache.persistence.query.mock.IndexMapImpl;
import com.gemstone.gemfire.internal.cache.persistence.query.mock.NaturalComparator;
import com.gemstone.gemfire.internal.cache.persistence.query.mock.Pair;
import com.gemstone.gemfire.internal.cache.persistence.query.mock.PairComparator;
import com.gemstone.gemfire.test.junit.categories.UnitTest;

@Category(UnitTest.class)
public class TemporaryResultSetFactoryJUnitTest {

  @Test
  public void testSortedResultSet() {
    ResultSet set = new TemporaryResultSetFactory().getSortedResultSet(null, false);
    set.add(1);
    set.add(2);
    set.add(4);
    set.add(3);
    set.add(2);
    
    assertItrEquals(set.iterator(), 1,2, 3, 4);
  }

  @Test
  public void testSortedResultBag() {
    ResultBag set = new TemporaryResultSetFactory().getSortedResultBag(null, false);
    set.add(1);
    set.add(2);
    set.add(4);
    set.add(3);
    set.add(2);

    assertItrEquals(set.iterator(), 1,2, 2,3, 4);
  }

  @Test
  public void testResultList() {
    ResultList set = new TemporaryResultSetFactory().getResultList();
    set.add(1);
    set.add(2);
    set.add(4);
    set.add(3);
    set.add(2);

    assertItrEquals(set.iterator(), 1,2, 4,3, 2);
    assertItrEquals(set.iterator(2), 4,3, 2);
  }

  @Test
  public void testIndexMap() {
    IndexMap map = new IndexMapImpl();
    TreeMap expected = new TreeMap(new PairComparator(new NaturalComparator(), new NaturalComparator()));
    put("i1", "r1", "v1", map, expected);
    put("i2", "r2", "v4", map, expected);
    put("i4", "r4", "v4", map, expected);
    put("i2", "r5", "v5", map, expected);
    
    assertItrEquals(map.keyIterator(), "r1", "r2", "r5", "r4");
    assertItrEquals(map.keyIterator("i2", true, "i3", true), "r2", "r5");
    assertItrEquals(map.keyIterator("i2", true, "i2", true), "r2", "r5");
    assertItrEquals(map.getKey("i2"), "r2", "r5");
    
    //See if we can get an entry range
    assertEntryEquals(map.iterator("i2", true, "i4", true), expected.tailMap(new Pair("i1", "r2")));
  }

  private void put(String ikey, String rkey, String value, IndexMap map, Map expected) {
    map.put(ikey, rkey, value);
    expected.put(new Pair(ikey, rkey), value);
  }

  private void assertItrEquals(CloseableIterator<CachedDeserializable> iterator, Object ... values) {
    ArrayList actual = new ArrayList();
    
    while(iterator.hasNext()) {
      actual.add(iterator.next().getDeserializedForReading());
    }
    
    assertEquals(Arrays.asList(values), actual);
  }
  
  private void assertEntryEquals(CloseableIterator<IndexEntry> closeableIterator, Map expected) {
    LinkedHashMap actual = new LinkedHashMap();
    
    while(closeableIterator.hasNext()) {
      IndexEntry entry = closeableIterator.next();
      Object ikey = entry.getKey().getDeserializedForReading();
      Object rkey = entry.getRegionKey().getDeserializedForReading();
      Object value = entry.getValue().getDeserializedForReading();
      actual.put(new Pair(ikey, rkey), value);
    }
    
    assertEquals(expected, actual);
  }
}
