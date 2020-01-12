/*
 * Licensed to the Apache Software Foundation (ASF) under one or more contributor license
 * agreements. See the NOTICE file distributed with this work for additional information regarding
 * copyright ownership. The ASF licenses this file to you under the Apache License, Version 2.0 (the
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
package org.apache.geode.cache.lucene.internal.distributed;

import static org.assertj.core.api.Assertions.assertThat;

import org.junit.Test;
import org.junit.experimental.categories.Category;

import org.apache.geode.CopyHelper;
import org.apache.geode.cache.lucene.LuceneQueryFactory;
import org.apache.geode.cache.lucene.internal.LuceneServiceImpl;
import org.apache.geode.cache.lucene.test.LuceneTestUtilities;
import org.apache.geode.test.junit.categories.LuceneTest;

@Category(LuceneTest.class)
public class TopEntriesJUnitTest {
  private EntryScore<String> r1_1 = new EntryScore<>("3", .9f);
  private EntryScore<String> r1_2 = new EntryScore<>("1", .8f);
  private EntryScore<String> r2_1 = new EntryScore<>("2", 0.85f);
  private EntryScore<String> r2_2 = new EntryScore<>("4", 0.1f);

  @Test
  @SuppressWarnings("unchecked")
  public void testPopulateTopEntries() {
    TopEntries<String> hits = new TopEntries<>();
    hits.addHit(r1_1);
    hits.addHit(r2_1);
    hits.addHit(r1_2);
    hits.addHit(r2_2);

    assertThat(hits.size()).isEqualTo(4);
    LuceneTestUtilities.verifyResultOrder(hits.getHits(), r1_1, r2_1, r1_2, r2_2);
  }

  @Test
  @SuppressWarnings("unchecked")
  public void putSameScoreEntries() {
    TopEntries<String> hits = new TopEntries<>();
    EntryScore<String> r1 = new EntryScore<>("1", .8f);
    EntryScore<String> r2 = new EntryScore<>("2", .8f);
    hits.addHit(r1);
    hits.addHit(r2);

    assertThat(hits.size()).isEqualTo(2);
    LuceneTestUtilities.verifyResultOrder(hits.getHits(), r1, r2);
  }

  @Test
  public void testInitialization() {
    TopEntries<String> hits = new TopEntries<>();
    assertThat(hits.getLimit()).isEqualTo(LuceneQueryFactory.DEFAULT_LIMIT);

    hits = new TopEntries<>(123);
    assertThat(hits.getLimit()).isEqualTo(123);
  }

  @Test(expected = IllegalArgumentException.class)
  public void testInvalidLimit() {
    new TopEntries<String>(-1);
  }

  @Test
  @SuppressWarnings("unchecked")
  public void enforceLimit() {
    TopEntries<String> hits = new TopEntries<>(3);
    hits.addHit(r1_1);
    hits.addHit(r2_1);
    hits.addHit(r1_2);
    hits.addHit(r2_2);

    assertThat(hits.size()).isEqualTo(3);
    LuceneTestUtilities.verifyResultOrder(hits.getHits(), r1_1, r2_1, r1_2);
  }

  @Test
  @SuppressWarnings("unchecked")
  public void testSerialization() {
    LuceneServiceImpl.registerDataSerializables();
    TopEntries<String> hits = new TopEntries<>(3);

    TopEntries<String> copy = CopyHelper.deepCopy(hits);
    assertThat(copy).isNotNull();
    assertThat(copy.getLimit()).isEqualTo(3);
    assertThat(copy.getHits().size()).isEqualTo(0);

    hits = new TopEntries<>(3);
    hits.addHit(r1_1);
    hits.addHit(r2_1);
    hits.addHit(r1_2);

    copy = CopyHelper.deepCopy(hits);
    assertThat(copy).isNotNull();
    assertThat(copy.size()).isEqualTo(3);
    LuceneTestUtilities.verifyResultOrder(copy.getHits(), r1_1, r2_1, r1_2);
  }
}
