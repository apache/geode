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
package com.gemstone.gemfire.cache.query.internal.index;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;
import static org.mockito.Matchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import java.util.HashSet;
import java.util.Iterator;
import java.util.Set;
import java.util.stream.IntStream;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.junit.experimental.categories.Category;

import com.gemstone.gemfire.cache.Region;
import com.gemstone.gemfire.cache.query.QueryService;
import com.gemstone.gemfire.cache.query.internal.index.AbstractIndex.InternalIndexStatistics;
import com.gemstone.gemfire.internal.cache.GemFireCacheImpl;
import com.gemstone.gemfire.internal.cache.LocalRegion;
import com.gemstone.gemfire.internal.cache.RegionEntry;
import com.gemstone.gemfire.test.junit.categories.UnitTest;

@Category(UnitTest.class)
public class MemoryIndexStoreJUnitTest {

	Region region;
	GemFireCacheImpl cache;
	InternalIndexStatistics mockStats;
	MemoryIndexStore store;
	RegionEntry[] mockEntries;
	int numMockEntries = 10;
  GemFireCacheImpl actualInstance;
  
	public void subclassPreSetup() {
		
	}
	
	protected Region createRegion() {
		return mock(LocalRegion.class);
	}
	
	@Before
	public void setup() {
		subclassPreSetup();
		region = createRegion();
		cache = mock(GemFireCacheImpl.class);
		actualInstance = GemFireCacheImpl.setInstanceForTests(cache);
		mockStats = mock(AbstractIndex.InternalIndexStatistics.class);
		
		store = new MemoryIndexStore(region, mockStats);
		store.setIndexOnValues(true);
		mockEntries = new RegionEntry[numMockEntries];
		IntStream.range(0, numMockEntries).forEach(i-> {
			mockEntries[i] = createRegionEntry(i, new Object());
		});
	}
	
	@After
	public void teardown() {
	  GemFireCacheImpl.setInstanceForTests(actualInstance);
	}
	
	@Test
	public void testSizeOfStoreReturnsNumberOfKeysAndNotActualNumberOfValues() {
		IntStream.range(0, 150).forEach(i -> {
			try {
				store.addMapping(i % 3, createRegionEntry(i, new Object()));
			}
			catch (Exception e) {
				fail();
			}
		});
		assertEquals(150, numObjectsInStore(store));
	}
	
	@Test
	public void testAddEnoughEntriesToCreateAConcurrentHashSet() {
		IntStream.range(0, 150).forEach(i -> {
			try {
				store.addMapping(1, createRegionEntry(i, new Object()));
			}
			catch (Exception e) {
				fail();
			}
		});
		assertEquals(150, numObjectsInStore(store));
	}
	
	@Test
	public void testUpdateAgainstAConcurrentHashSet() throws Exception{
		IntStream.range(0, 150).forEach(i -> {
			try {
				store.addMapping(1, createRegionEntry(1, new Object()));
			}
			catch (Exception e) {
				fail();
			}
		});
		RegionEntry entry = createRegionEntry(1, new Object());	
		store.addMapping(1, entry);
		store.updateMapping(2, 1, entry, entry.getValue(null));
		assertEquals(151, numObjectsInStore(store));
	}
	
	@Test
	public void testCanAddObjectWithUndefinedKey() throws Exception {
		store.addMapping(QueryService.UNDEFINED, mockEntries[0]);
		assertEquals(1, numObjectsIterated(store.get(QueryService.UNDEFINED)));
		assertEquals(0, numObjectsInStore(store));
	}
	
	@Test
	public void testCanAddManyObjectsWithUndefinedKey() throws Exception {
		for (int i = 0; i < mockEntries.length; i++) {
			store.addMapping(QueryService.UNDEFINED, mockEntries[i]);
		}
		assertEquals(mockEntries.length, numObjectsIterated(store.get(QueryService.UNDEFINED)));
		//Undefined will not return without an explicit get for UNDEFINED);
		assertEquals(0, numObjectsInStore(store));
	}
	
	@Test
	public void testIteratorWithStartInclusiveAndNoKeysToRemoveReturnsCorrectNumberOfResults() throws Exception {
		addMockedEntries(numMockEntries);
		assertEquals(2, numObjectsIterated(store.iterator(numMockEntries - 2, true, null)));
	}
	
	@Test
	public void testIteratorWithStartExclusiveAndNoKeysToRemoveReturnsCorrectNumberOfResults() throws Exception {
		addMockedEntries(numMockEntries);
		assertEquals(1, numObjectsIterated(store.iterator(numMockEntries - 2, false, null)));
	}
	
	
	@Test
	public void testIteratorWithStartInclusiveAndKeyToRemoveReturnsCorrectNumberOfResults() throws Exception {
		addMockedEntries(numMockEntries);
		Set keysToRemove = new HashSet();
		keysToRemove.add("1");
		assertEquals(9, numObjectsIterated(store.iterator(1, true, keysToRemove)));
	}
	
	@Test
	public void testIteratorWithStartExclusiveAndKeyToRemoveReturnsCorrectNumberOfResults() throws Exception {
		addMockedEntries(numMockEntries);
		Set keysToRemove = new HashSet();
		keysToRemove.add("1");
		assertEquals(8, numObjectsIterated(store.iterator(1, false, keysToRemove)));
	}
	
	@Test
	public void testStartAndEndInclusiveReturnsCorrectResults() throws Exception {
		addMockedEntries(numMockEntries);
		assertEquals(6, numObjectsIterated(store.iterator(1, true, 6, true, null)));
	}
	
	@Test
	public void testStartInclusiveAndEndExclusiveReturnsCorrectResults() throws Exception {
		addMockedEntries(numMockEntries);
		assertEquals(5, numObjectsIterated(store.iterator(1, true, 6, false, null)));
	}
	
	@Test
	public void testStartExclusiveAndEndExclusiveReturnsCorrectResults() throws Exception {
		addMockedEntries(numMockEntries);
		assertEquals(4, numObjectsIterated(store.iterator(1, false, 6, false, null)));
	}
	
	@Test
	public void testStartExclusiveAndEndInclusiveReturnsCorrectResults() throws Exception {
		addMockedEntries(numMockEntries);
		assertEquals(5, numObjectsIterated(store.iterator(1, false, 6, true, null)));
	}
	
	@Test
	public void testStartIsNull() throws Exception {
		addMockedEntries(numMockEntries);
		assertEquals(6, numObjectsIterated(store.iterator(null, false, 6, false, null)));
	}
	
	@Test
	public void testDescendingIteratorReturnsExpectedOrderOfEntries() throws Exception {
		RegionEntry mockEntry1 = mockEntries[0];
		RegionEntry mockEntry2 = mockEntries[1];
		store.addMapping("1", mockEntry1);
		store.addMapping("2", mockEntry2);
		Iterator iteratorFirst = store.descendingIterator(null);
		assertEquals(2, numObjectsIterated(iteratorFirst));
		
		Iterator iterator = store.descendingIterator(null);
		iterator.hasNext();
		assertEquals(mockEntry2, ((MemoryIndexStore.MemoryIndexStoreEntry)iterator.next()).getRegionEntry());
		iterator.hasNext();
		assertEquals(mockEntry1, ((MemoryIndexStore.MemoryIndexStoreEntry)iterator.next()).getRegionEntry());
	}
	
	@Test
	public void testDescendingIteratorWithRemovedKeysReturnsExpectedOrderOfEntries() throws Exception {
		RegionEntry mockEntry1 = mockEntries[0];
		RegionEntry mockEntry2 = mockEntries[1];
		RegionEntry mockEntry3 = mockEntries[2];
		store.addMapping("1", mockEntry1);
		store.addMapping("2", mockEntry2);
		store.addMapping("3", mockEntry3);
		Set keysToRemove = new HashSet();
		keysToRemove.add("2");
		Iterator iteratorFirst = store.descendingIterator(keysToRemove);
		assertEquals(2, numObjectsIterated(iteratorFirst));
		
		//keysToRemove has been modified by the store, we need to readd the key to remove
		keysToRemove.add("2");
		Iterator iterator = store.descendingIterator(keysToRemove);
		iterator.hasNext();
		assertEquals(mockEntry3, ((MemoryIndexStore.MemoryIndexStoreEntry)iterator.next()).getRegionEntry());
		iterator.hasNext();
		assertEquals(mockEntry1, ((MemoryIndexStore.MemoryIndexStoreEntry)iterator.next()).getRegionEntry());
		assertFalse(iterator.hasNext());
	}
	
	@Test
	public void testDescendingIteratorWithMultipleRemovedKeysReturnsExpectedOrderOfEntries() throws Exception {
		RegionEntry mockEntry1 = mockEntries[0];
		RegionEntry mockEntry2 = mockEntries[1];
		RegionEntry mockEntry3 = mockEntries[2];
		store.addMapping("1", mockEntry1);
		store.addMapping("2", mockEntry2);
		store.addMapping("3", mockEntry3);
		Set keysToRemove = new HashSet();
		keysToRemove.add("2");
		keysToRemove.add("1");
		Iterator iteratorFirst = store.descendingIterator(keysToRemove);
		assertEquals(1, numObjectsIterated(iteratorFirst));
		
		//keysToRemove has been modified by the store, we need to readd the key to remove
		keysToRemove.add("2");
		keysToRemove.add("1");
		Iterator iterator = store.descendingIterator(keysToRemove);
		iterator.hasNext();
		assertEquals(mockEntry3, ((MemoryIndexStore.MemoryIndexStoreEntry)iterator.next()).getRegionEntry());
		assertFalse(iterator.hasNext());
	}
	
	@Test
	public void testSizeWithKeyArgumentReturnsCorrectSize() throws Exception {
		RegionEntry mockEntry1 = mockEntries[0];
		RegionEntry mockEntry2 = mockEntries[1];
		store.addMapping("1", mockEntry1);
		store.addMapping("2", mockEntry2);
		assertEquals(1, store.size("1"));
	}
	
	 @Test
	  public void testGetReturnsExpectedIteratorValue() throws Exception {
	    RegionEntry mockEntry1 = mockEntries[0];
	    RegionEntry mockEntry2 = mockEntries[1];
	    store.addMapping("1", mockEntry1);
	    store.addMapping("2", mockEntry2);
	    assertEquals(1, numObjectsIterated(store.get("1")));
	  }
	
	@Test
	public void testGetReturnsExpectedIteratorWithMultipleValues() throws Exception {
		RegionEntry mockEntry1 = mockEntries[0];
		RegionEntry mockEntry2 = mockEntries[1];
		RegionEntry mockEntry3 = mockEntries[2];
		RegionEntry mockEntry4 = mockEntries[3];
		store.addMapping("1", mockEntry1);
		store.addMapping("1", mockEntry2);
		store.addMapping("1", mockEntry3);
		store.addMapping("2", mockEntry4);
		assertEquals(3, numObjectsIterated(store.get("1")));
		assertEquals(4, numObjectsInStore(store));
	}
	
	@Test
	public void testGetWithIndexOnKeysReturnsExpectedIteratorValues() throws Exception {
		RegionEntry mockEntry1 = mockEntries[0];
		RegionEntry mockEntry2 = mockEntries[1];
		store.setIndexOnValues(false);
		store.setIndexOnRegionKeys(true);
		store.addMapping("1", mockEntry1);
		store.addMapping("2", mockEntry2);
		assertEquals(1, numObjectsIterated(store.get("1")));
	}

	@Test
	public void testCorrectlyRemovesEntryProvidedTheWrongKey() throws Exception {
		RegionEntry mockEntry1 = mockEntries[0];
		RegionEntry mockEntry2 = mockEntries[1];
		store.addMapping("1", mockEntry1);
		store.addMapping("2", mockEntry2);
		store.removeMapping("1", mockEntry2);
		assertEquals(1, numObjectsInStore(store));
		assertTrue(objectContainedIn(store, mockEntry1));
	}

	@Test
	public void testRemoveMappingRemovesFromBackingMap() throws Exception {
		RegionEntry mockEntry1 = mockEntries[0];
		RegionEntry mockEntry2 = mockEntries[1];
		store.addMapping("1", mockEntry1);
		store.addMapping("2", mockEntry2);
		store.removeMapping("1", mockEntry1);
		assertEquals(1, numObjectsInStore(store));
		assertTrue(objectContainedIn(store, mockEntry2));
	}

	@Test
	public void testAddMappingAddsToBackingMap() throws Exception {
		RegionEntry mockEntry1 = mockEntries[0];
		RegionEntry mockEntry2 = mockEntries[1];
		store.addMapping("1", mockEntry1);
		store.addMapping("2", mockEntry2);
		assertEquals(2, numObjectsInStore(store));
		assertTrue(objectContainedIn(store, mockEntry1));
		assertTrue(objectContainedIn(store, mockEntry2));
	}
	
	@Test
	public void testClear() throws Exception {
		RegionEntry mockEntry1 = mockEntries[0];
		RegionEntry mockEntry2 = mockEntries[1];
		store.addMapping("1", mockEntry1);
		store.addMapping("1", mockEntry2);
		store.clear();
		assertEquals(0, numObjectsInStore(store));
	}

	private int numObjectsInStore(MemoryIndexStore store) {
		Iterator iterator = store.iterator(null);
		return numObjectsIterated(iterator);
	}

	private int numObjectsIterated(Iterator iterator) {
		int count = 0;
		while (iterator.hasNext()) {
			iterator.next();
			count++;
		}
		return count;
	}

	private boolean objectContainedIn(MemoryIndexStore store, Object o) {
		Iterator iterator = store.valueToEntriesMap.values().iterator();
		return objectContainedIn(iterator, o);
	}
	
	private boolean objectContainedIn(Iterator iterator, Object o) {
		while (iterator.hasNext()) {
			if (iterator.next().equals(o)) {
				return true;
			}
		}
		return false;
	}
	
	private void addMockedEntries(int numEntriesToAdd) {
		IntStream.range(0, numEntriesToAdd).forEach(i -> {
			try {
				store.addMapping(mockEntries[i].getKey(), mockEntries[i]);
			}
			catch (Exception e) {
				fail();
			}
		});
	}
	
	private RegionEntry createRegionEntry(Object key, Object value) {
		RegionEntry mockEntry = mock(RegionEntry.class);
		when(mockEntry.getValue(any())).thenReturn(value);
		when(mockEntry.getKey()).thenReturn(key);
		return mockEntry;
	}
}
