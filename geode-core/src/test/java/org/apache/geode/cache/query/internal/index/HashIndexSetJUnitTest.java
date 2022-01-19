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
package org.apache.geode.cache.query.internal.index;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotEquals;
import static org.junit.Assert.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.IntStream;

import org.junit.Test;
import org.mockito.invocation.InvocationOnMock;
import org.mockito.stubbing.Answer;

import org.apache.geode.cache.query.TypeMismatchException;
import org.apache.geode.cache.query.data.Portfolio;

public class HashIndexSetJUnitTest {

  Map<Integer, Portfolio> portfoliosMap;
  Set<Portfolio> portfolioSet;
  HashIndexSet his;

  private void setupHashIndexSet(int numEntries) {
    his = createHashIndexSet();
    portfoliosMap = createPortfolioObjects(numEntries, 0);
    portfolioSet = new HashSet<>(portfoliosMap.values());
    addPortfoliosToHashIndexSet(portfoliosMap, his);
  }

  private void addPortfoliosToHashIndexSet(Map<Integer, Portfolio> portfoliosMap,
      HashIndexSet hashIndexSet) {
    portfoliosMap.forEach((k, v) -> {
      try {
        hashIndexSet.add(k, v);
      } catch (TypeMismatchException exception) {
        throw new Error(exception);
      }
    });
  }

  private HashIndexSet createHashIndexSet() {
    HashIndexSet his = new HashIndexSet();
    HashIndex.IMQEvaluator mockEvaluator = mock(HashIndex.IMQEvaluator.class);
    when(mockEvaluator.evaluateKey(any(Object.class))).thenAnswer(new EvaluateKeyAnswer());
    his.setEvaluator(mockEvaluator);
    return his;
  }

  /**
   * we are "indexed" on indexKey. Equality of portfolios is based on ID indexKeys are based on 0 ->
   * numEntries IDs are startID -> startID + numEntries
   *
   * @param numToCreate how many portfolios to create
   * @param startID the ID value to start incrementing from
   */
  private Map<Integer, Portfolio> createPortfolioObjects(int numToCreate, int startID) {
    Map<Integer, Portfolio> portfoliosMap = new HashMap<>();
    IntStream.range(0, numToCreate).forEach(e -> {
      Portfolio p = new Portfolio(e + startID);
      p.indexKey = e;
      portfoliosMap.put(p.indexKey, p);
    });
    return portfoliosMap;
  }

  @Test
  public void testHashIndexSetAdd() throws Exception {
    int numEntries = 100;
    setupHashIndexSet(numEntries);

    assertEquals(numEntries, his.size());
    his.iterator().forEachRemaining((e -> portfolioSet.remove(e)));
    assertTrue(portfolioSet.isEmpty());
  }

  @Test
  public void testHashIndexSetAddWithNullKey() throws Exception {
    int numEntries = 100;
    setupHashIndexSet(numEntries);

    assertEquals(numEntries, his.size());
    his.add(null, new Portfolio(numEntries + 1));
    assertEquals(numEntries + 1, his.size());
  }

  /**
   * we have to be sure that we dont cause a compaction or growth or else removed tokens will be
   * removed and a new backing array created
   */
  @Test
  public void testHashIndexSetAddUseRemoveTokenSlot() throws Exception {
    int numEntries = 20;
    setupHashIndexSet(numEntries);
    assertEquals(numEntries, his.size());
    his.removeAll(portfolioSet);
    assertEquals(numEntries, his.hashIndexSetProperties.removedTokens);
    assertEquals(0, his.size());
    addPortfoliosToHashIndexSet(portfoliosMap, his);

    assertEquals(0, his.hashIndexSetProperties.removedTokens);
    assertEquals(numEntries, his.size());
  }

  @Test
  public void testCompactDueToTooManyRemoveTokens() throws Exception {
    int numEntries = 10;
    setupHashIndexSet(numEntries);

    assertEquals(numEntries, his.size());
    his.removeAll(portfolioSet);
    assertEquals(numEntries, his.hashIndexSetProperties.removedTokens);

    assertEquals(0, his.size());

    // Very very bad but we fake out the number of removed tokens
    his.hashIndexSetProperties.removedTokens = his.hashIndexSetProperties.maxSize;
    addPortfoliosToHashIndexSet(portfoliosMap, his);

    // compaction should have occurred, removed tokens should now be gone
    assertEquals(0, his.hashIndexSetProperties.removedTokens);
    assertEquals(numEntries, his.size());
  }

  @Test
  public void testRehashRetainsAllValues() throws Exception {
    int numEntries = 80;
    setupHashIndexSet(numEntries);
    assertEquals(numEntries, his.size());
    his.rehash(1000);
    assertEquals(numEntries, his.size());
    his.iterator().forEachRemaining((e -> portfolioSet.remove(e)));
    assertTrue(portfolioSet.isEmpty());
  }

  @Test
  public void testShrinkByRehashRetainsAllValues() throws Exception {
    int numEntries = 20;
    setupHashIndexSet(numEntries);

    assertEquals(numEntries, his.size());
    his.rehash(64);
    assertEquals(numEntries, his.size());
    his.iterator().forEachRemaining((e -> portfolioSet.remove(e)));
    assertTrue(portfolioSet.isEmpty());
  }

  @Test
  public void testGetByKey() throws Exception {
    int numEntries = 20;
    setupHashIndexSet(numEntries);

    assertEquals(numEntries, his.size());
    his.get(1).forEachRemaining((e -> portfolioSet.remove(e)));
    assertEquals(numEntries - 1, portfolioSet.size());
  }

  @Test
  public void testGetByKeyMultipleCollisions() throws Exception {
    int numEntries = 20;
    int keyToLookup = 1;
    his = createHashIndexSet();
    Map<Integer, Portfolio> collectionOfPorts1 = createPortfolioObjects(numEntries, 0);
    Map<Integer, Portfolio> collectionOfPorts2 =
        createPortfolioObjects(numEntries, numEntries);

    addPortfoliosToHashIndexSet(collectionOfPorts1, his);
    addPortfoliosToHashIndexSet(collectionOfPorts2, his);

    assertEquals(numEntries * 2, his.size());
    Iterator iterator = his.get(keyToLookup);
    int numIterated = 0;
    while (iterator.hasNext()) {
      numIterated++;
      // verify that the returned values match what we lookedup
      assertEquals(keyToLookup, ((Portfolio) iterator.next()).indexKey);
    }
    assertEquals(2, numIterated);
  }

  @Test
  public void testGetByKeyLocatesAfterMultipleColiisionsAndRemoveToken() throws Exception {
    int numEntries = 20;
    int keyToLookup = 1;
    his = createHashIndexSet();
    Map<Integer, Portfolio> collectionOfPorts1 = createPortfolioObjects(numEntries, 0);
    Map<Integer, Portfolio> collectionOfPorts2 =
        createPortfolioObjects(numEntries, numEntries);
    Map<Integer, Portfolio> collectionOfPorts3 =
        createPortfolioObjects(numEntries, numEntries * 2);

    addPortfoliosToHashIndexSet(collectionOfPorts1, his);
    addPortfoliosToHashIndexSet(collectionOfPorts2, his);
    addPortfoliosToHashIndexSet(collectionOfPorts3, his);

    assertEquals(numEntries * 3, his.size());
    Iterator iterator = his.get(keyToLookup);
    int numIterated = 0;
    while (iterator.hasNext()) {
      numIterated++;
      // verify that the returned values match what we lookedup
      assertEquals(keyToLookup, ((Portfolio) iterator.next()).indexKey);
    }
    assertEquals(3, numIterated);

    // let's remove the second collision
    his.remove(keyToLookup, collectionOfPorts2.get(keyToLookup));

    iterator = his.get(keyToLookup);
    numIterated = 0;
    while (iterator.hasNext()) {
      numIterated++;
      // verify that the returned values match what we lookedup
      assertEquals(keyToLookup, ((Portfolio) iterator.next()).indexKey);
    }
    assertEquals(2, numIterated);

    // Add it back in and make sure we can iterate all 3 again
    his.add(keyToLookup, collectionOfPorts2.get(keyToLookup));
    iterator = his.get(keyToLookup);
    numIterated = 0;
    while (iterator.hasNext()) {
      numIterated++;
      // verify that the returned values match what we lookedup
      assertEquals(keyToLookup, ((Portfolio) iterator.next()).indexKey);
    }
    assertEquals(3, numIterated);

  }

  @Test
  public void testGetAllNotMatching() throws Exception {
    int numEntries = 20;
    his = createHashIndexSet();
    Map<Integer, Portfolio> collectionOfPorts1 = createPortfolioObjects(numEntries, 0);
    Map<Integer, Portfolio> collectionOfPorts2 =
        createPortfolioObjects(numEntries, numEntries);

    addPortfoliosToHashIndexSet(collectionOfPorts1, his);
    addPortfoliosToHashIndexSet(collectionOfPorts2, his);

    assertEquals(numEntries * 2, his.size());
    List<Integer> keysNotToMatch = new LinkedList<>();
    keysNotToMatch.add(3);
    keysNotToMatch.add(4);
    Iterator iterator = his.getAllNotMatching(keysNotToMatch);
    int numIterated = 0;
    while (iterator.hasNext()) {
      numIterated++;
      int idFound = ((Portfolio) iterator.next()).indexKey;
      assertTrue(idFound != 3 && idFound != 4);
    }
    // Make sure we iterated all the entries minus the entries that we decided not to match
    assertEquals(numEntries * 2 - 4, numIterated);
  }

  @Test
  public void testIndexOfObject() throws Exception {
    int numEntries = 10;
    his = createHashIndexSet();
    portfoliosMap = createPortfolioObjects(numEntries, 0);
    portfoliosMap.forEach((k, v) -> {
      try {
        int index = his.add(k, portfoliosMap.get(k));
        int foundIndex = his.index(portfoliosMap.get(k));
        assertEquals(index, foundIndex);
      } catch (TypeMismatchException ex) {
        throw new Error(ex);
      }
    });
  }

  /**
   * Add multiple portfolios with the same id they should collide, we should then be able to look up
   * each one correctly
   */
  @Test
  public void testIndexOfObjectWithCollision() throws Exception {
    int numEntries = 10;
    his = createHashIndexSet();
    Map<Integer, Portfolio> portfoliosMap1 = createPortfolioObjects(numEntries, 0);
    Map<Integer, Portfolio> portfoliosMap2 = createPortfolioObjects(numEntries, numEntries);

    portfoliosMap1.forEach((k, v) -> {
      try {
        int index = his.add(k, portfoliosMap1.get(k));
        int foundIndex = his.index(portfoliosMap1.get(k));
        assertEquals(index, foundIndex);
      } catch (TypeMismatchException ex) {
        throw new Error(ex);
      }
    });
    portfoliosMap2.forEach((k, v) -> {
      try {
        int index = his.add(k, portfoliosMap2.get(k));
        int foundIndex = his.index(portfoliosMap2.get(k));
        assertEquals(index, foundIndex);
      } catch (TypeMismatchException ex) {
        throw new Error(ex);
      }
    });
  }

  @Test
  public void testIndexWhenObjectNotInSet() {
    int numEntries = 10;
    his = createHashIndexSet();
    portfoliosMap = createPortfolioObjects(numEntries, 0);
    assertEquals(-1, his.index(portfoliosMap.get(1)));
  }

  @Test
  public void testIndexWhenObjectNotInSetWhenPopulated() {
    int numEntries = 10;
    setupHashIndexSet(numEntries);
    assertEquals(-1, his.index(new Portfolio(numEntries + 1)));
  }

  @Test
  public void testRemove() throws Exception {
    int numEntries = 20;
    setupHashIndexSet(numEntries);

    assertEquals(numEntries, his.size());
    portfoliosMap.forEach((k, v) -> his.remove(k, v));
    assertEquals(0, his.size());
  }

  /**
   * Test remove where we look for an instance that is not at the specified index slot
   */
  @Test
  public void testRemoveIgnoreSlot() throws Exception {
    int numEntries = 20;
    setupHashIndexSet(numEntries);

    assertEquals(numEntries, his.size());
    portfoliosMap.forEach((k, v) -> his.remove(k, v, his.index(v)));
    assertEquals(numEntries, his.size());
  }

  @Test
  public void testRemoveAtWithNull() throws Exception {
    his = createHashIndexSet();
    assertTrue(his.isEmpty());
    assertFalse(his.removeAt(0));
  }

  @Test
  public void testRemoveAtWithRemoveToken() throws Exception {
    his = createHashIndexSet();
    int index = his.add(1, new Portfolio(1));
    assertTrue(his.removeAt(index));
    assertFalse(his.removeAt(index));
  }

  @Test
  public void testHashIndexRemoveAll() throws Exception {
    int numEntries = 100;
    setupHashIndexSet(numEntries);

    assertEquals(numEntries, his.size());
    his.removeAll(portfolioSet);
    assertTrue(his.isEmpty());
  }

  /**
   * Remove all should still remove all portfolios provided, even if there are more provided then
   * contained
   */
  @Test
  public void testHashIndexRemoveAllWithAdditionalPortfolios() throws Exception {
    int numEntries = 100;
    setupHashIndexSet(numEntries);

    assertEquals(numEntries, his.size());
    portfolioSet.add(new Portfolio(numEntries + 1));
    his.removeAll(portfolioSet);
    assertTrue(his.isEmpty());
  }

  @Test
  public void testHashIndexContainsAll() throws Exception {
    int numEntries = 100;
    setupHashIndexSet(numEntries);

    assertEquals(numEntries, his.size());
    assertTrue(his.containsAll(portfolioSet));
  }

  @Test
  public void testHashIndexRetainAll() throws Exception {
    int numEntries = 10;
    setupHashIndexSet(numEntries);
    Set subset = new HashSet();
    portfolioSet.forEach(e -> {
      if (e.indexKey % 2 == 0) {
        subset.add(e);
      }
    });
    assertEquals(numEntries, his.size());
    his.retainAll(subset);
    his.iterator().forEachRemaining((subset::remove));
    assertTrue(subset.isEmpty());
    assertEquals(numEntries / 2, his.size());
  }

  @Test
  public void testHashIndexContainsAllShouldReturnFalse() throws Exception {
    int numEntries = 100;
    setupHashIndexSet(numEntries);

    assertEquals(numEntries, his.size());
    portfolioSet.add(new Portfolio(numEntries + 1));
    assertFalse(his.containsAll(portfolioSet));
  }

  @Test
  public void testClear() throws Exception {
    int numEntries = 100;
    setupHashIndexSet(numEntries);

    assertEquals(numEntries, his.size());
    his.clear();
    assertTrue(his.isEmpty());
    assertTrue(his.hashIndexSetProperties.removedTokens == 0);
  }

  @Test
  public void testAreNullObjectsEqual() throws Exception {
    his = createHashIndexSet();
    assertTrue(his.areObjectsEqual(null, null));
  }

  @Test
  public void testAreIndexeSetsEqualAndHashCodeSame() throws Exception {
    Map<Integer, Portfolio> portfolioMap = createPortfolioObjects(100, 0);
    HashIndexSet indexSet1 = createHashIndexSet();
    HashIndexSet indexSet2 = createHashIndexSet();

    addPortfoliosToHashIndexSet(portfolioMap, indexSet1);
    addPortfoliosToHashIndexSet(portfolioMap, indexSet2);

    assertTrue(indexSet1.equals(indexSet2));
    assertTrue(indexSet2.equals(indexSet1));
    assertEquals(indexSet1.hashCode(), indexSet2.hashCode());
  }

  @Test
  public void testAreIndexeSetsNotEqualAndHashCodeDifferent() throws Exception {
    Map<Integer, Portfolio> portfolioMap = createPortfolioObjects(100, 0);
    HashIndexSet indexSet1 = createHashIndexSet();
    HashIndexSet indexSet2 = createHashIndexSet();

    addPortfoliosToHashIndexSet(portfolioMap, indexSet1);

    indexSet2.add(1, portfolioMap.get(1));
    assertFalse(indexSet2.equals(indexSet1));
    assertFalse(indexSet1.equals(indexSet2));
    assertNotEquals(indexSet1.hashCode(), indexSet2.hashCode());
  }

  @Test
  public void testIndexSetNotEqualsOtherObjectType() {
    HashIndexSet indexSet = createHashIndexSet();
    assertFalse(indexSet.equals("Other type"));
    assertFalse(indexSet.equals(new Object()));
  }

  private static class EvaluateKeyAnswer implements Answer {

    @Override
    public Object answer(InvocationOnMock invocation) throws Throwable {
      Object evalOn = invocation.getArgument(0);
      if (evalOn instanceof Portfolio) {
        Portfolio p = (Portfolio) evalOn;
        return p.indexKey;
      }
      return null;
    }

  }

}
