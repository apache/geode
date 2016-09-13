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
package org.apache.geode.cache.query.internal;

import static org.junit.Assert.*;

import java.lang.reflect.Method;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.Comparator;
import java.util.Iterator;
import java.util.List;
import java.util.NoSuchElementException;
import java.util.SortedSet;
import java.util.TreeSet;

import org.junit.Test;
import org.junit.experimental.categories.Category;

import org.apache.geode.cache.query.Struct;
import org.apache.geode.cache.query.internal.types.ObjectTypeImpl;
import org.apache.geode.cache.query.internal.types.StructTypeImpl;
import org.apache.geode.cache.query.types.ObjectType;
import org.apache.geode.test.junit.categories.UnitTest;

@Category(UnitTest.class)
public class NWayMergeResultsJUnitTest {

  @Test
  public void testNonDistinct() throws Exception {
    final int numSortedLists = 40;
    Collection<List<Integer>> listOfSortedLists = new ArrayList<List<Integer>>();
    for (int i = 0; i < numSortedLists; ++i) {
      listOfSortedLists.add(new ArrayList<Integer>());
    }
    int step = 0;
    for (List<Integer> list : listOfSortedLists) {
      step = step + 1;
      for (int i = -500; i < 500; i = i + step) {
        list.add(i);
      }
    }
    int totalElements = 0;
    for (List<Integer> list : listOfSortedLists) {
      totalElements += list.size();
    }

    int combinedArray[] = new int[totalElements];
    int i = 0;
    for (List<Integer> list : listOfSortedLists) {
      for (Integer num : list) {
        combinedArray[i++] = num.intValue();
      }
    }
    Arrays.sort(combinedArray);

    NWayMergeResults<Integer> mergedResults = createSingleFieldMergedResult(
        listOfSortedLists, false, -1);
    Iterator<Integer> iter = mergedResults.iterator();
    for (int elem : combinedArray) {
      assertEquals(elem, iter.next().intValue());
    }
    assertFalse(iter.hasNext());
    try {
      iter.next();
      fail("next should have thrown NoSuchElementException");
    } catch (NoSuchElementException nsee) {
      // Ok.
    }

    assertEquals(combinedArray.length, mergedResults.size());
  }

  @Test
  public void testDistinct() throws Exception {
    final int numSortedLists = 40;
    Collection<List<Integer>> listOfSortedLists = new ArrayList<List<Integer>>();
    for (int i = 0; i < numSortedLists; ++i) {
      listOfSortedLists.add(new ArrayList<Integer>());
    }
    int step = 0;
    for (List<Integer> list : listOfSortedLists) {
      step = step + 1;
      for (int i = -500; i < 500; i = i + step) {
        list.add(i);
      }
    }

    SortedSet<Integer> sortedSet = new TreeSet<Integer>();

    for (List<Integer> list : listOfSortedLists) {
      for (Integer num : list) {
        sortedSet.add(num);
      }
    }

    NWayMergeResults<Integer> mergedResults = createSingleFieldMergedResult(
        listOfSortedLists, true, -1);

    Iterator<Integer> iter = mergedResults.iterator();
    for (int elem : sortedSet) {
      assertEquals(elem, iter.next().intValue());
    }
    assertFalse(iter.hasNext());

    try {
      iter.next();
      fail("next should have thrown NoSuchElementException");
    } catch (NoSuchElementException nsee) {
      // Ok.
    }

    assertEquals(sortedSet.size(), mergedResults.size());
  }

  @Test
  public void testLimitNoDistinct() throws Exception {
    final int numSortedLists = 40;
    final int limit = 53;
    Collection<List<Integer>> listOfSortedLists = new ArrayList<List<Integer>>();
    for (int i = 0; i < numSortedLists; ++i) {
      listOfSortedLists.add(new ArrayList<Integer>());
    }
    int step = 0;
    for (List<Integer> list : listOfSortedLists) {
      step = step + 1;
      for (int i = -500; i < 500; i = i + step) {
        list.add(i);
      }
    }
    int totalElements = 0;
    for (List<Integer> list : listOfSortedLists) {
      totalElements += list.size();
    }

    int combinedArray[] = new int[totalElements];
    int i = 0;
    for (List<Integer> list : listOfSortedLists) {
      for (Integer num : list) {
        combinedArray[i++] = num.intValue();
      }
    }
    Arrays.sort(combinedArray);

    NWayMergeResults<Integer> mergedResults = createSingleFieldMergedResult(
        listOfSortedLists, false, limit);

    Iterator<Integer> iter = mergedResults.iterator();
    int count = 0;
    for (int elem : combinedArray) {
      if (count == limit) {
        break;
      }
      assertEquals(elem, iter.next().intValue());
      ++count;
    }
    assertFalse(iter.hasNext());
    try {
      iter.next();
      fail("next should have thrown NoSuchElementException");
    } catch (NoSuchElementException nsee) {
      // Ok.
    }

    assertEquals(limit, mergedResults.size());
  }

  @Test
  public void testLimitDistinct() throws Exception {
    final int numSortedLists = 40;
    final int limit = 53;
    Collection<List<Integer>> listOfSortedLists = new ArrayList<List<Integer>>();
    for (int i = 0; i < numSortedLists; ++i) {
      listOfSortedLists.add(new ArrayList<Integer>());
    }
    int step = 0;
    for (List<Integer> list : listOfSortedLists) {
      step = step + 1;
      for (int i = -500; i < 500; i = i + step) {
        list.add(i);
      }
    }

    SortedSet<Integer> sortedSet = new TreeSet<Integer>();

    for (List<Integer> list : listOfSortedLists) {
      for (Integer num : list) {
        sortedSet.add(num);
      }
    }

    NWayMergeResults<Integer> mergedResults = createSingleFieldMergedResult(
        listOfSortedLists, true, limit);

    Iterator<Integer> iter = mergedResults.iterator();

    int count = 0;

    for (int elem : sortedSet) {
      if (count == limit) {
        break;
      }
      assertEquals(elem, iter.next().intValue());
      ++count;
    }
    assertFalse(iter.hasNext());

    try {
      iter.next();
      fail("next should have thrown NoSuchElementException");
    } catch (NoSuchElementException nsee) {
      // Ok.
    }
    assertEquals(limit, mergedResults.size());
  }

  @Test
  public void testNonDistinctStruct() throws Exception {
    final int numSortedLists = 40;
    StructTypeImpl structType = new StructTypeImpl(new String[] { "a", "b" },
        new ObjectType[] { new ObjectTypeImpl(Integer.TYPE),
            new ObjectTypeImpl(Integer.TYPE) });
    Collection<List<Struct>> listOfSortedLists = new ArrayList<List<Struct>>();
    for (int i = 0; i < numSortedLists; ++i) {
      listOfSortedLists.add(new ArrayList<Struct>());
    }
    int step = 0;
    for (List<Struct> list : listOfSortedLists) {
      step = step + 1;
      int j = 1000;
      for (int i = -500; i < 500; i = i + step) {
        Struct struct = new StructImpl(structType, new Object[] {
            Integer.valueOf(i), Integer.valueOf(j - step) });
        list.add(struct);
      }
    }
    int totalElements = 0;
    for (List<Struct> list : listOfSortedLists) {
      totalElements += list.size();
    }

    Struct combinedArray[] = new Struct[totalElements];
    int i = 0;
    for (List<Struct> list : listOfSortedLists) {
      for (Struct struct : list) {
        combinedArray[i++] = struct;
      }
    }
    Arrays.sort(combinedArray, new Comparator<Struct>() {
      @Override
      public int compare(Struct o1, Struct o2) {
        Object[] fields_1 = o1.getFieldValues();
        Object[] fields_2 = o2.getFieldValues();
        int compare = ((Comparable) fields_1[0])
            .compareTo((Comparable) fields_2[0]);
        if (compare == 0) {
          // second field is descending
          compare = ((Comparable) fields_2[1])
              .compareTo((Comparable) fields_1[1]);
        }
        return compare;
      }
    });

    NWayMergeResults<Struct> mergedResults = createStructFieldMergedResult(
        listOfSortedLists, false, -1, structType);

    Iterator<Struct> iter = mergedResults.iterator();
    for (Struct elem : combinedArray) {
      assertEquals(elem, iter.next());
    }
    assertFalse(iter.hasNext());
    try {
      iter.next();
      fail("next should have thrown NoSuchElementException");
    } catch (NoSuchElementException nsee) {
      // Ok.
    }
  }

  @Test
  public void testDistinctStruct() throws Exception {
    final int numSortedLists = 40;
    StructTypeImpl structType = new StructTypeImpl(new String[] { "a", "b" },
        new ObjectType[] { new ObjectTypeImpl(Integer.TYPE),
            new ObjectTypeImpl(Integer.TYPE) });
    Collection<List<Struct>> listOfSortedLists = new ArrayList<List<Struct>>();
    for (int i = 0; i < numSortedLists; ++i) {
      listOfSortedLists.add(new ArrayList<Struct>());
    }
    int step = 0;
    for (List<Struct> list : listOfSortedLists) {
      step = step + 1;
      int j = 1000;
      for (int i = -500; i < 500; i = i + step) {
        Struct struct = new StructImpl(structType, new Object[] {
            Integer.valueOf(i), Integer.valueOf(j - step) });
        list.add(struct);
      }
    }

    SortedSet<Struct> sortedSet = new TreeSet<Struct>(new Comparator<Struct>() {
      @Override
      public int compare(Struct o1, Struct o2) {
        Object[] fields_1 = o1.getFieldValues();
        Object[] fields_2 = o2.getFieldValues();
        int compare = ((Comparable) fields_1[0])
            .compareTo((Comparable) fields_2[0]);
        if (compare == 0) {
          // second field is descending
          compare = ((Comparable) fields_2[1])
              .compareTo((Comparable) fields_1[1]);
        }
        return compare;
      }
    });

    int i = 0;
    for (List<Struct> list : listOfSortedLists) {
      for (Struct struct : list) {
        sortedSet.add(struct);
      }
    }

    NWayMergeResults<Struct> mergedResults = createStructFieldMergedResult(
        listOfSortedLists, true, -1, structType);

    Iterator<Struct> iter = mergedResults.iterator();
    for (Struct elem : sortedSet) {
      assertEquals(elem, iter.next());
    }
    assertFalse(iter.hasNext());
    try {
      iter.next();
      fail("next should have thrown NoSuchElementException");
    } catch (NoSuchElementException nsee) {
      // Ok.
    }
  }

  @Test
  public void testOccurenceNonDistinct() throws Exception {
    final int numSortedLists = 40;
    Collection<List<Integer>> listOfSortedLists = new ArrayList<List<Integer>>();
    for (int i = 0; i < numSortedLists; ++i) {
      listOfSortedLists.add(new ArrayList<Integer>());
    }
    int step = 0;
    for (List<Integer> list : listOfSortedLists) {
      step = step + 1;
      for (int i = -500; i < 500; i = i + step) {
        list.add(i);
      }
    }
    int totalElements = 0;
    for (List<Integer> list : listOfSortedLists) {
      totalElements += list.size();
    }

    int combinedArray[] = new int[totalElements];
    int i = 0;
    for (List<Integer> list : listOfSortedLists) {
      for (Integer num : list) {
        combinedArray[i++] = num.intValue();
      }
    }
    Arrays.sort(combinedArray);
    // count occurrence of 70, 72,73 , 75
    int num70 = 0, num72 = 0, num73 = 0, num75 = 0;
    for (int num : combinedArray) {
      if (num == 70) {
        ++num70;
      } else if (num == 72) {
        ++num72;
      } else if (num == 73) {
        ++num73;
      } else if (num == 75) {
        ++num75;
      }
    }
    NWayMergeResults<Integer> mergedResults = createSingleFieldMergedResult(
        listOfSortedLists, false, -1);
    assertEquals(num70, mergedResults.occurrences(Integer.valueOf(70)));
    assertEquals(num72, mergedResults.occurrences(Integer.valueOf(72)));
    assertEquals(num73, mergedResults.occurrences(Integer.valueOf(73)));
    assertEquals(num75, mergedResults.occurrences(Integer.valueOf(75)));
  }

  @Test
  public void testOccurenceDistinct() throws Exception {
    final int numSortedLists = 40;
    Collection<List<Integer>> listOfSortedLists = new ArrayList<List<Integer>>();
    for (int i = 0; i < numSortedLists; ++i) {
      listOfSortedLists.add(new ArrayList<Integer>());
    }
    int step = 0;
    for (List<Integer> list : listOfSortedLists) {
      step = step + 1;
      for (int i = -500; i < 500; i = i + step) {
        list.add(i);
      }
    }

    SortedSet<Integer> sortedSet = new TreeSet<Integer>();

    for (List<Integer> list : listOfSortedLists) {
      for (Integer num : list) {
        sortedSet.add(num);
      }
    }

    NWayMergeResults<Integer> mergedResults = createSingleFieldMergedResult(
        listOfSortedLists, true, -1);

    assertEquals(1, mergedResults.occurrences(Integer.valueOf(70)));
    assertEquals(1, mergedResults.occurrences(Integer.valueOf(72)));
    assertEquals(1, mergedResults.occurrences(Integer.valueOf(73)));
    assertEquals(1, mergedResults.occurrences(Integer.valueOf(75)));

  }

  private <E> NWayMergeResults<E> createSingleFieldMergedResult(
      Collection<? extends Collection<E>> sortedResults, boolean isDistinct,
      int limit) throws Exception {
    CompiledSortCriterion csc = new CompiledSortCriterion(false,
        CompiledSortCriterion.ProjectionField.getProjectionField());
    Method method = CompiledSortCriterion.class.getDeclaredMethod(
        "substituteExpressionWithProjectionField", Integer.TYPE);
    method.setAccessible(true);
    method.invoke(csc, 0);
    List<CompiledSortCriterion> orderByAttribs = new ArrayList<CompiledSortCriterion>();
    orderByAttribs.add(csc);
    ExecutionContext context = new ExecutionContext(null, null);
    ObjectType elementType = new ObjectTypeImpl(Object.class);

    return new NWayMergeResults<E>(sortedResults, isDistinct, limit,
        orderByAttribs, context, elementType);
  }

  private NWayMergeResults<Struct> createStructFieldMergedResult(
      Collection<? extends Collection<Struct>> sortedResults,
      boolean isDistinct, int limit, StructTypeImpl structType)
      throws Exception {
    CompiledSortCriterion csc1 = new CompiledSortCriterion(false,
        CompiledSortCriterion.ProjectionField.getProjectionField());
    CompiledSortCriterion csc2 = new CompiledSortCriterion(true,
        CompiledSortCriterion.ProjectionField.getProjectionField());
    Method method = CompiledSortCriterion.class.getDeclaredMethod(
        "substituteExpressionWithProjectionField", Integer.TYPE);
    method.setAccessible(true);
    method.invoke(csc1, 0);
    method.invoke(csc2, 1);
    List<CompiledSortCriterion> orderByAttribs = new ArrayList<CompiledSortCriterion>();
    orderByAttribs.add(csc1);
    orderByAttribs.add(csc2);
    ExecutionContext context = new ExecutionContext(null, null);

    return new NWayMergeResults<Struct>(sortedResults, false, -1,
        orderByAttribs, context, structType);
  }

  @Test
  public void testCombination() throws Exception {
    List<String> results1 = new ArrayList<String>();
    results1.add("IBM");
    results1.add("YHOO");

    List<String> results2 = new ArrayList<String>();
    results2.add("APPL");
    results2.add("ORCL");

    List<String> results3 = new ArrayList<String>();
    results3.add("DELL");
    results3.add("RHAT");

    List<String> results4 = new ArrayList<String>();
    results4.add("ORCL");
    results4.add("SAP");

    List<String> results5 = Collections.emptyList();
    List<String> results6 = Collections.emptyList();
    List<String> results7 = Collections.emptyList();

    List<Collection<String>> sortedLists = new ArrayList<Collection<String>>();
    sortedLists.add(results1);
    sortedLists.add(results2);
    sortedLists.add(results3);
    sortedLists.add(results4);
    sortedLists.add(results5);
    sortedLists.add(results6);
    sortedLists.add(results7);

    NWayMergeResults<String> mergedResults = createSingleFieldMergedResult(
        sortedLists, true, -1);

    List<String> results8 = new ArrayList<String>();
    results8.add("DELL");
    results8.add("GOOG");
    results8.add("HP");
    results8.add("MSFT");
    results8.add("SAP");
    results8.add("SUN");

    List<String> results9 = new ArrayList<String>();
    results9.add("AOL");
    results9.add("APPL");
    results9.add("GOOG");
    results9.add("IBM");
    results9.add("SUN");
    results9.add("YHOO");

    List<Collection<String>> sortedLists1 = new ArrayList<Collection<String>>();
    sortedLists1.add(mergedResults);
    sortedLists1.add(results8);
    sortedLists1.add(results9);

    NWayMergeResults<String> netMergedResults = createSingleFieldMergedResult(
        sortedLists1, true, -1);

    assertEquals(12, netMergedResults.size());
  }

}
