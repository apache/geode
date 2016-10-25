/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 * 
 *   http://www.apache.org/licenses/LICENSE-2.0
 * 
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package org.apache.geode.cache.lucene.internal.distributed;

import static org.junit.Assert.*;

import java.util.Collection;
import java.util.Iterator;

import org.jmock.Mockery;
import org.jmock.lib.concurrent.Synchroniser;
import org.jmock.lib.legacy.ClassImposteriser;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.junit.experimental.categories.Category;

import org.apache.geode.CopyHelper;
import org.apache.geode.cache.lucene.LuceneQueryFactory;
import org.apache.geode.cache.lucene.internal.LuceneServiceImpl;
import org.apache.geode.test.junit.categories.UnitTest;

@Category(UnitTest.class)
public class TopEntriesJUnitTest {

  private Mockery mockContext;

  private EntryScore<String> r1_1 = new EntryScore("3", .9f);
  private EntryScore<String> r1_2 = new EntryScore("1", .8f);
  private EntryScore<String> r2_1 = new EntryScore("2", 0.85f);
  private EntryScore<String> r2_2 = new EntryScore("4", 0.1f);

  @Test
  public void testPopulateTopEntries() {
    TopEntries<String> hits = new TopEntries<String>();
    hits.addHit(r1_1);
    hits.addHit(r2_1);
    hits.addHit(r1_2);
    hits.addHit(r2_2);
    
    assertEquals(4, hits.size());
    verifyResultOrder(hits.getHits(), r1_1, r2_1, r1_2, r2_2);
  }

  @Test
  public void putSameScoreEntries() {
    TopEntries<String> hits = new TopEntries<String>();
    EntryScore<String> r1 = new EntryScore<String>("1", .8f);
    EntryScore<String> r2 = new EntryScore<String>("2", .8f);
    hits.addHit(r1);
    hits.addHit(r2);
    
    assertEquals(2, hits.size());
    verifyResultOrder(hits.getHits(), r1, r2);
  }
  
  @Test
  public void testInitialization() {
    TopEntries<String> hits = new TopEntries<String>();
    assertEquals(LuceneQueryFactory.DEFAULT_LIMIT, hits.getLimit());

    hits = new TopEntries<String>(123);
    assertEquals(123, hits.getLimit());
  }

  @Test(expected = IllegalArgumentException.class)
  public void testInvalidLimit() {
    new TopEntries<String>(-1);
  }
  
  @Test
  public void enforceLimit() throws Exception {
    TopEntries<String> hits = new TopEntries<String>(3);
    hits.addHit(r1_1);
    hits.addHit(r2_1);
    hits.addHit(r1_2);
    hits.addHit(r2_2);

    assertEquals(3, hits.size());
    verifyResultOrder(hits.getHits(), r1_1, r2_1, r1_2);
  }

  @Test
  public void testSerialization() {
    LuceneServiceImpl.registerDataSerializables();
    TopEntries<String> hits = new TopEntries<String>(3);
    
    TopEntries<String> copy = CopyHelper.deepCopy(hits);
    assertEquals(3, copy.getLimit());
    assertEquals(0, copy.getHits().size());
    
    hits = new TopEntries<String>(3);
    hits.addHit(r1_1);
    hits.addHit(r2_1);
    hits.addHit(r1_2);
    
    copy = CopyHelper.deepCopy(hits);
    assertEquals(3, copy.size());
    verifyResultOrder(copy.getHits(), r1_1, r2_1, r1_2);
  }

  // TODO: extract to lucene test util class
  public static void verifyResultOrder(Collection<EntryScore<String>> list, EntryScore<String>... expectedEntries) {
    Iterator<EntryScore<String>> iter = list.iterator();
    for (EntryScore expectedEntry : expectedEntries) {
      if (!iter.hasNext()) {
        fail();
      }
      EntryScore toVerify = iter.next();
      assertEquals(expectedEntry.getKey(), toVerify.getKey());
      assertEquals(expectedEntry.getScore(), toVerify.getScore(), .0f);
    }
  }

  @Before
  public void setupMock() {
    mockContext = new Mockery() {
      {
        setImposteriser(ClassImposteriser.INSTANCE);
        setThreadingPolicy(new Synchroniser());
      }
    };
  }

  @After
  public void validateMock() {
    mockContext.assertIsSatisfied();
    mockContext = null;
  }
}
