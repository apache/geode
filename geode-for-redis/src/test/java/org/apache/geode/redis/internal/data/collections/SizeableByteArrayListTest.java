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
package org.apache.geode.redis.internal.data.collections;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.LinkedList;
import java.util.List;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

import junitparams.Parameters;
import junitparams.naming.TestCaseName;
import org.junit.Test;
import org.junit.runner.RunWith;

import org.apache.geode.cache.util.ObjectSizer;
import org.apache.geode.internal.size.ReflectionObjectSizer;
import org.apache.geode.test.junit.runners.GeodeParamsRunner;

@RunWith(GeodeParamsRunner.class)
public class SizeableByteArrayListTest {
  private final ObjectSizer sizer = ReflectionObjectSizer.getInstance();
  private final int INITIAL_NUMBER_OF_ELEMENTS = 20;

  @Test
  public void removeNElements_removesElementsFromHeadWhenCountIsPositive() {
    String repeatedElement = "anElement";
    SizeableByteArrayList list = setupListWithDuplicateValues(repeatedElement);

    String[] expectedContents =
        {"0", "1", repeatedElement, "2", repeatedElement, "3", repeatedElement, "4",
            repeatedElement, "5", repeatedElement};
    List<byte[]> expectedListContents =
        Arrays.stream(expectedContents).map(String::getBytes).collect(
            Collectors.toList());
    Integer[] expectedIndexes = {0, 2};
    List<Integer> removedIndexes = list.removeNElements(repeatedElement.getBytes(), 2);
    assertThat(list).containsExactlyElementsOf(expectedListContents);
    assertThat(removedIndexes).containsExactly(expectedIndexes);
  }

  @Test
  public void removeNElements_removesElementsFromTailWhenCountIsNegative() {
    String repeatedElement = "anElement";
    SizeableByteArrayList list = setupListWithDuplicateValues(repeatedElement);

    String[] expectedContents =
        {repeatedElement, "0", repeatedElement, "1", repeatedElement, "2", repeatedElement, "3",
            repeatedElement, "4",
            "5"};
    List<byte[]> expectedListContents =
        Arrays.stream(expectedContents).map(String::getBytes).collect(
            Collectors.toList());
    Integer[] expectedIndexes = {10, 12};
    List<Integer> removedIndexes = list.removeNElements(repeatedElement.getBytes(), -2);
    assertThat(list).containsExactlyElementsOf(expectedListContents);
    assertThat(removedIndexes).containsExactly(expectedIndexes);
  }

  @Test
  public void removeNElements_removesAllElementsWhenCountIsZero() {
    String repeatedElement = "anElement";
    SizeableByteArrayList list = setupListWithDuplicateValues(repeatedElement);

    String[] expectedContents = {"0", "1", "2", "3", "4", "5"};
    List<byte[]> expectedListContents =
        Arrays.stream(expectedContents).map(String::getBytes).collect(
            Collectors.toList());
    Integer[] expectedIndexes = {0, 2, 4, 6, 8, 10, 12};
    List<Integer> removedIndexes = list.removeNElements(repeatedElement.getBytes(), 0);
    assertThat(list).containsExactlyElementsOf(expectedListContents);
    assertThat(removedIndexes).containsExactly(expectedIndexes);
  }

  @Test
  public void removeNElements_removesAllElementsWhenCountIsGreaterThanSize() {
    String repeatedElement = "anElement";
    SizeableByteArrayList list = setupListWithDuplicateValues(repeatedElement);

    String[] expectedContents = {"0", "1", "2", "3", "4", "5"};
    List<byte[]> expectedListContents =
        Arrays.stream(expectedContents).map(String::getBytes).collect(
            Collectors.toList());
    Integer[] expectedIndexes = {0, 2, 4, 6, 8, 10, 12};
    List<Integer> removedIndexes = list.removeNElements(repeatedElement.getBytes(), 100);
    assertThat(list).containsExactlyElementsOf(expectedListContents);
    assertThat(removedIndexes).containsExactly(expectedIndexes);
  }

  @Test
  public void removeNElements_removesAllElementsWhenCountIsGreaterThanSizeWithNegativeCount() {
    String repeatedElement = "anElement";
    SizeableByteArrayList list = setupListWithDuplicateValues(repeatedElement);

    String[] expectedContents = {"0", "1", "2", "3", "4", "5"};
    List<byte[]> expectedListContents =
        Arrays.stream(expectedContents).map(String::getBytes).collect(
            Collectors.toList());
    Integer[] expectedIndexes = {0, 2, 4, 6, 8, 10, 12};
    List<Integer> removedIndexes = list.removeNElements(repeatedElement.getBytes(), -100);
    assertThat(list).containsExactlyElementsOf(expectedListContents);
    assertThat(removedIndexes).containsExactly(expectedIndexes);
  }

  @Test
  @Parameters(method = "getValidRanges")
  @TestCaseName("{method}: fromIndex:{0}, toIndex:{1}")
  public void clearSublist_clearsElementsFromValidSubList(int fromIndex, int toIndex) {
    int size = 5;
    SizeableByteArrayList list = setupList(size);
    List<byte[]> expectedList =
        IntStream.range(0, size).mapToObj(i -> String.valueOf(i).getBytes())
            .collect(Collectors.toList());
    assertThat(list).containsExactlyElementsOf(expectedList);
    list.clearSublist(fromIndex, toIndex);
    expectedList.subList(fromIndex, toIndex).clear();
    assertThat(list).containsExactlyElementsOf(expectedList);
  }

  @SuppressWarnings("unused")
  private Object[] getValidRanges() {
    // Values are fromIndex, toIndex
    // For initial list of size 5
    return new Object[] {
        "0, 0",
        "0, 3",
        "0, 5",
        "5, 5",
        "2, 5",
        "1, 4"
    };
  }

  @Test
  public void clearSublist_throwsExceptionWithInvalidArguments() {
    int size = 5;
    SizeableByteArrayList list = setupList(size);
    assertThatThrownBy(() -> list.clearSublist(-1, 3)).isInstanceOf(
        IndexOutOfBoundsException.class);
    assertThatThrownBy(() -> list.clearSublist(0, size + 1)).isInstanceOf(
        IndexOutOfBoundsException.class);
    assertThatThrownBy(() -> list.clearSublist(3, 2)).isInstanceOf(
        IllegalArgumentException.class);
  }

  @Test
  public void indexOf_returnsIndexOfElementWhenElementExist() {
    SizeableByteArrayList list = setupList(INITIAL_NUMBER_OF_ELEMENTS);
    for (int i = 0; i < INITIAL_NUMBER_OF_ELEMENTS; i++) {
      assertThat(list.indexOf(list.get(i))).isEqualTo(i);
    }
  }

  @Test
  public void indexOf_returnsNegativeOneWhenElementDoesNotExist() {
    SizeableByteArrayList list = setupList(2);
    Object nonExisting = "nonExisting".getBytes();
    assertThat(list.indexOf(nonExisting)).isEqualTo(-1);
  }

  @Test
  public void lastIndexOf_throwsUnsupportedException() {
    SizeableByteArrayList list = setupList(2);
    assertThatThrownBy(() -> list.lastIndexOf(list.get(0))).isInstanceOf(
        UnsupportedOperationException.class);
  }

  @Test
  @Parameters(method = "getValidIndexList")
  @TestCaseName("{method}: removalList:{0}, expected:{1}")
  public void removeIndexes_removesGivenIndexesForValidIndexList(List<Integer> removalList,
      byte[][] expected) {
    int size = 5;
    SizeableByteArrayList list = setupList(size);
    list.removeIndexes(removalList);
    assertThat(list).containsExactly(expected);
  }

  @SuppressWarnings("unused")
  private Object[] getValidIndexList() {
    // Values are removalList, expected
    // For initial list of size 5
    return new Object[] {
        new Object[] {Arrays.asList(0, 4),
            new byte[][] {"1".getBytes(), "2".getBytes(), "3".getBytes()}},
        new Object[] {Arrays.asList(1, 2, 3, 4), new byte[][] {"0".getBytes()}},
        new Object[] {Arrays.asList(2, 3),
            new byte[][] {"0".getBytes(), "1".getBytes(), "4".getBytes()}}
    };
  }

  @Test
  public void removeIndexes_throwsIndexOutOfBoundsExceptionForInvalidIndexList() {
    int size = 5;
    SizeableByteArrayList list = setupList(size);
    assertThatThrownBy(() -> list.removeIndexes(Arrays.asList(-1))).isInstanceOf(
        IndexOutOfBoundsException.class);
    assertThat(list.size()).isEqualTo(size);
    assertThatThrownBy(() -> list.removeIndexes(Arrays.asList(1, size))).isInstanceOf(
        IndexOutOfBoundsException.class);
    assertThat(list.size()).isEqualTo(size);
    assertThatThrownBy(() -> list.removeIndexes(Arrays.asList(-1, 2, 3, 4))).isInstanceOf(
        IndexOutOfBoundsException.class);
    assertThat(list.size()).isEqualTo(size);
    assertThatThrownBy(() -> list.removeIndexes(Collections.singletonList(size))).isInstanceOf(
        IndexOutOfBoundsException.class);
    assertThat(list.size()).isEqualTo(size);
    assertThatThrownBy(() -> list.removeIndexes(Arrays.asList(2, 3, 4, size))).isInstanceOf(
        IndexOutOfBoundsException.class);
    assertThat(list.size()).isEqualTo(size);

  }

  @Test
  public void removeIndexes_throwsIllegalArgumentExceptionForUnsortedIndexList() {
    int size = 5;
    SizeableByteArrayList list = setupList(size);
    assertThatThrownBy(() -> list.removeIndexes(Arrays.asList(0, 0))).isInstanceOf(
        IllegalArgumentException.class);
    assertThatThrownBy(() -> list.removeIndexes(Arrays.asList(1, 3, 2, 4))).isInstanceOf(
        IllegalArgumentException.class);
  }

  @Test
  public void remove_WithElement_removesGivenElementIfElementToRemoveExists() {
    int size = 5;
    SizeableByteArrayList list = setupList(size);
    byte[] elementToRemove = list.get(2);

    LinkedList<byte[]> expectedList =
        IntStream.range(0, size).mapToObj(i -> String.valueOf(i).getBytes())
            .collect(Collectors.toCollection(LinkedList::new));
    byte[] elementToRemoveInExpectedList = expectedList.get(2);
    assertThat(list).containsExactlyElementsOf(expectedList);

    boolean isRemoved = list.remove(elementToRemove);
    boolean expectedIsRemoved = expectedList.remove(elementToRemoveInExpectedList);
    assertThat(isRemoved).isEqualTo(expectedIsRemoved);
    assertThat(list).containsExactlyElementsOf(expectedList);
  }

  @Test
  public void remove_WithElement_doesNotRemoveElementIfElementToRemoveDoesNotExist() {
    int size = 5;
    SizeableByteArrayList list = setupList(size);
    byte[] elementToRemove = "non-existing".getBytes();

    LinkedList<byte[]> expectedList =
        IntStream.range(0, size).mapToObj(i -> String.valueOf(i).getBytes())
            .collect(Collectors.toCollection(LinkedList::new));
    assertThat(list).containsExactlyElementsOf(expectedList);

    boolean isRemoved = list.remove(elementToRemove);
    boolean expectedIsRemoved = expectedList.remove(elementToRemove);
    assertThat(isRemoved).isEqualTo(expectedIsRemoved);
    assertThat(list).containsExactlyElementsOf(expectedList);
  }

  @Test
  public void remove_WithIndex_removesIndexWhenGivenIndexToRemoveIsValid() {
    int size = 5;
    int elementToRemove = 1;
    SizeableByteArrayList list = setupList(size);

    List<byte[]> expectedList =
        IntStream.range(0, size).mapToObj(i -> String.valueOf(i).getBytes())
            .collect(Collectors.toCollection(LinkedList::new));
    assertThat(list).containsExactlyElementsOf(expectedList);

    byte[] elementRemoved = list.remove(elementToRemove);
    byte[] expectedElementRemoved = expectedList.remove(elementToRemove);
    assertThat(elementRemoved).isEqualTo(expectedElementRemoved);
    assertThat(list).containsExactlyElementsOf(expectedList);
  }

  @Test
  public void remove_WithIndex_throwsIndexOutOfBoundsExceptionIfIndexIsInvalid() {
    int size = 5;
    SizeableByteArrayList list = setupList(size);
    assertThatThrownBy(() -> list.remove(size)).isInstanceOf(
        IndexOutOfBoundsException.class);
    assertThatThrownBy(() -> list.remove(-1)).isInstanceOf(
        IndexOutOfBoundsException.class);
  }

  @Test
  public void removeLastOccurrence_throwsUnsupportedException() {
    SizeableByteArrayList list = setupList(2);
    assertThatThrownBy(() -> list.removeLastOccurrence(list.get(0))).isInstanceOf(
        UnsupportedOperationException.class);
  }

  @Test
  public void set_setsNewElementAtIndex_withValidIndex() {
    int size = 5;
    SizeableByteArrayList list = setupList(size);

    List<byte[]> expectedList =
        IntStream.range(0, size).mapToObj(i -> String.valueOf(i).getBytes())
            .collect(Collectors.toCollection(LinkedList::new));
    assertThat(list).containsExactlyElementsOf(expectedList);

    byte[] newElement = "new Element".getBytes();
    int indexToBeSet = 1;

    byte[] oldElement = list.set(indexToBeSet, newElement);
    byte[] expectedOldElement = expectedList.set(indexToBeSet, newElement);
    assertThat(oldElement).isEqualTo(expectedOldElement);
    assertThat(list).containsExactlyElementsOf(expectedList);
  }

  @Test
  public void set_throwsIndexOutOfBoundsExceptionIfIndexIsInvalid() {
    int size = 5;
    byte[] newElement = "new Element".getBytes();
    SizeableByteArrayList list = setupList(size);
    assertThatThrownBy(() -> list.set(size, newElement)).isInstanceOf(
        IndexOutOfBoundsException.class);
    assertThatThrownBy(() -> list.set(-1, newElement)).isInstanceOf(
        IndexOutOfBoundsException.class);
  }

  @Test
  @Parameters({"0", "2", "5"})
  public void add_addsNewElementAtIndex_withValidIndex(int indexToAdd) {
    int size = 5;
    SizeableByteArrayList list = setupList(size);

    List<byte[]> expectedList =
        IntStream.range(0, size).mapToObj(i -> String.valueOf(i).getBytes())
            .collect(Collectors.toCollection(LinkedList::new));
    assertThat(list).containsExactlyElementsOf(expectedList);

    byte[] newElement = "new Element".getBytes();

    list.add(indexToAdd, newElement);
    expectedList.add(indexToAdd, newElement);
    assertThat(list).containsExactlyElementsOf(expectedList);
  }

  @Test
  public void add_throwsIndexOutOfBoundsExceptionIfIndexElementIsInvalid() {
    int size = 5;
    byte[] newElement = "new Element".getBytes();
    SizeableByteArrayList list = setupList(size);
    assertThatThrownBy(() -> list.add(size + 1, newElement)).isInstanceOf(
        IndexOutOfBoundsException.class);
    assertThatThrownBy(() -> list.add(-1, newElement)).isInstanceOf(
        IndexOutOfBoundsException.class);
  }

  @Test
  public void addFirst_addsNewElementAtFirstIndex() {
    int size = 5;
    SizeableByteArrayList list = setupList(size);

    LinkedList<byte[]> expectedList =
        IntStream.range(0, size).mapToObj(i -> String.valueOf(i).getBytes())
            .collect(Collectors.toCollection(LinkedList::new));
    assertThat(list).containsExactlyElementsOf(expectedList);

    byte[] newElement = "new Element".getBytes();

    list.addFirst(newElement);
    expectedList.addFirst(newElement);
    assertThat(list).containsExactlyElementsOf(expectedList);
  }

  @Test
  public void addLast_addsNewElementAtLastIndex() {
    int size = 5;
    SizeableByteArrayList list = setupList(size);

    LinkedList<byte[]> expectedList =
        IntStream.range(0, size).mapToObj(i -> String.valueOf(i).getBytes())
            .collect(Collectors.toCollection(LinkedList::new));
    assertThat(list).containsExactlyElementsOf(expectedList);

    byte[] newElement = "new Element".getBytes();

    list.addLast(newElement);
    expectedList.addLast(newElement);
    assertThat(list).containsExactlyElementsOf(expectedList);
  }

  @Test
  @Parameters(method = "getValidArgumentsForInsert")
  @TestCaseName("{method}: referenceElement:{1}, before:{2}")
  public void insert_insertsGivenElementForValidInputs(byte[] elementToInsert,
      String referenceElement,
      boolean before,
      int expectedIndex,
      byte[][] expectedList) {
    int size = 5;
    SizeableByteArrayList list = setupList(size);
    int insertedIndex = list.insert(elementToInsert, referenceElement.getBytes(), before);
    assertThat(insertedIndex).isEqualTo(expectedIndex);
    assertThat(list).containsExactly(expectedList);
  }

  @SuppressWarnings("unused")
  private Object[] getValidArgumentsForInsert() {
    // Values are elementToInsert, referenceElement, before, expectedIndex, expectedList
    // For initial list of size 5
    byte[] newElement = "newElement".getBytes();
    return new Object[] {
        // Before first element
        new Object[] {newElement, "0", true, 0,
            new byte[][] {newElement, "0".getBytes(), "1".getBytes(), "2".getBytes(),
                "3".getBytes(), "4".getBytes()}},
        // After first element
        new Object[] {newElement, "0", false, 1,
            new byte[][] {"0".getBytes(), newElement, "1".getBytes(), "2".getBytes(),
                "3".getBytes(), "4".getBytes()}},
        // Before last element
        new Object[] {newElement, "4", true, 4,
            new byte[][] {"0".getBytes(), "1".getBytes(), "2".getBytes(), "3".getBytes(),
                newElement, "4".getBytes()}},
        // After last element
        new Object[] {newElement, "4", false, 5,
            new byte[][] {"0".getBytes(), "1".getBytes(), "2".getBytes(), "3".getBytes(),
                "4".getBytes(), newElement}},
        // Before middle element
        new Object[] {newElement, "2", true, 2,
            new byte[][] {"0".getBytes(), "1".getBytes(), newElement, "2".getBytes(),
                "3".getBytes(), "4".getBytes()}},
        // After middle element
        new Object[] {newElement, "2", false, 3,
            new byte[][] {"0".getBytes(), "1".getBytes(), "2".getBytes(), newElement,
                "3".getBytes(), "4".getBytes()}},
        // Before nonexistent element
        new Object[] {newElement, "non-existent", true, -1,
            new byte[][] {"0".getBytes(), "1".getBytes(), "2".getBytes(), "3".getBytes(),
                "4".getBytes()}},
        // After nonexistent element
        new Object[] {newElement, "non-existent", false, -1,
            new byte[][] {"0".getBytes(), "1".getBytes(), "2".getBytes(), "3".getBytes(),
                "4".getBytes()}}
    };
  }

  @Test
  public void getSizeInBytes_isAccurateForEmptySizeableByteArrayList() {
    SizeableByteArrayList list = new SizeableByteArrayList();
    assertThat(list.getSizeInBytes()).isEqualTo(sizer.sizeof(list));
  }

  @Test
  public void getSizeInBytes_isAccurateForSizeableByteArrayListElements() {
    int elementsToAdd = 100;

    // Create a list with an initial size and confirm that it correctly reports its size
    SizeableByteArrayList list = setupList(INITIAL_NUMBER_OF_ELEMENTS);
    assertThat(list.getSizeInBytes()).isEqualTo(sizer.sizeof(list));

    // Add elements and assert that the size is correct after each add
    for (int i = INITIAL_NUMBER_OF_ELEMENTS; i < INITIAL_NUMBER_OF_ELEMENTS + elementsToAdd; ++i) {
      list.addFirst(makeByteArrayOfSpecifiedLength(i));
      assertThat(list.getSizeInBytes()).isEqualTo(sizer.sizeof(list));
    }
    assertThat(list.size()).isEqualTo(INITIAL_NUMBER_OF_ELEMENTS + elementsToAdd);

    // Remove all the elements and assert that the size is correct after each remove
    for (int i = 0; i < INITIAL_NUMBER_OF_ELEMENTS + elementsToAdd; ++i) {
      list.remove(0);
      assertThat(list.getSizeInBytes()).isEqualTo(sizer.sizeof(list));
    }
    assertThat(list.size()).isEqualTo(0);
  }

  @Test
  public void equals_returnsTrueIfBothListsAreEqual() {
    int size = 5;
    SizeableByteArrayList list1 = setupList(size);
    SizeableByteArrayList list2 = setupList(size);

    assertThat(list1.equals(list2)).isTrue();
    byte[] someElement = "dummy".getBytes();
    list2.addLast(someElement);
    assertThat(list1.equals(list2)).isFalse();

  }

  @Test
  public void getSizeInBytesIsAccurate_ForEmptySizeableByteArrayList() {
    SizeableByteArrayList list = new SizeableByteArrayList();
    assertThat(list.getSizeInBytes()).isEqualTo(sizer.sizeof(list));
  }

  @Test
  public void getSizeInBytesIsAccurate_ForSizeableByteArrayListElements() {
    int elementsToAdd = 100;

    // Create a list with an initial size and confirm that it correctly reports its size
    SizeableByteArrayList list = createList();
    assertThat(list.getSizeInBytes()).isEqualTo(sizer.sizeof(list));

    // Add elements and assert that the size is correct after each add
    for (int i = INITIAL_NUMBER_OF_ELEMENTS; i < INITIAL_NUMBER_OF_ELEMENTS + elementsToAdd; ++i) {
      list.addFirst(makeByteArrayOfSpecifiedLength(i));
      assertThat(list.getSizeInBytes()).isEqualTo(sizer.sizeof(list));
    }
    assertThat(list.size()).isEqualTo(INITIAL_NUMBER_OF_ELEMENTS + elementsToAdd);

    // Remove all the elements and assert that the size is correct after each remove
    for (int i = 0; i < INITIAL_NUMBER_OF_ELEMENTS + elementsToAdd; ++i) {
      list.remove(0);
      assertThat(list.getSizeInBytes()).isEqualTo(sizer.sizeof(list));
    }
    assertThat(list.size()).isEqualTo(0);
  }

  @Test
  public void getSizeInBytesIsAccurate_ForCopiedList() {
    SizeableByteArrayList original = createList();
    SizeableByteArrayList copy = new SizeableByteArrayList(original);

    assertThat(original.getSizeInBytes()).isEqualTo(copy.getSizeInBytes());
  }

  @Test
  public void clearSublist_getSizeInBytesIsAccurate() {
    // Create a list with an initial size and confirm that it correctly reports its size
    SizeableByteArrayList list = createList();
    assertThat(list.getSizeInBytes()).isEqualTo(sizer.sizeof(list));

    // Remove subset of elements and assert that the size is correct
    list.clearSublist(INITIAL_NUMBER_OF_ELEMENTS / 5, INITIAL_NUMBER_OF_ELEMENTS / 2);
    assertThat(list.getSizeInBytes()).isEqualTo(sizer.sizeof(list));

    list.clearSublist(0, list.size());
    assertThat(list.size()).isEqualTo(0);
  }

  @Test
  public void removeObjects_getSizeInBytesIsAccurate() {
    // Create a list with only duplicate elements and confirm that it correctly reports its size
    SizeableByteArrayList list = new SizeableByteArrayList();
    byte[] bytes = "anElement".getBytes();
    for (int i = 0; i < INITIAL_NUMBER_OF_ELEMENTS; ++i) {
      // Clone the byte array because otherwise we end up with the list containing multiple
      // references to the same object in memory rather than references to multiple different
      // objects
      list.addFirst(bytes.clone());
    }
    assertThat(list.getSizeInBytes()).isEqualTo(sizer.sizeof(list));

    // Remove elements from the head
    list.removeNElements(bytes, INITIAL_NUMBER_OF_ELEMENTS / 4);
    assertThat(list.getSizeInBytes()).isEqualTo(sizer.sizeof(list));

    // Remove elements from the tail
    list.removeNElements(bytes, INITIAL_NUMBER_OF_ELEMENTS / 4);
    assertThat(list.getSizeInBytes()).isEqualTo(sizer.sizeof(list));

    // Remove all of the remaining elements
    list.removeNElements(bytes, 0);
    assertThat(list.getSizeInBytes()).isEqualTo(sizer.sizeof(list));
    assertThat(list).isEmpty();
  }

  @Test
  public void removeObject_getSizeInBytesIsAccurate() {
    // Create a list with an initial size and confirm that it correctly reports its size
    SizeableByteArrayList list = createList();
    assertThat(list.getSizeInBytes()).isEqualTo(sizer.sizeof(list));

    // Remove all the elements and assert that the size is correct after each remove
    for (int i = 0; i < INITIAL_NUMBER_OF_ELEMENTS; ++i) {
      list.remove(makeByteArrayOfSpecifiedLength(i + 1));
      assertThat(list.getSizeInBytes()).isEqualTo(sizer.sizeof(list));
    }
    assertThat(list.size()).isEqualTo(0);
  }

  @Test
  public void removeIndexes_getSizeInBytesIsAccurate() {
    // Create a list with an initial size and confirm that it correctly reports its size
    SizeableByteArrayList list = createList();
    assertThat(list.getSizeInBytes()).isEqualTo(sizer.sizeof(list));

    // Remove all the elements and assert that the size is correct after each remove
    for (int i = INITIAL_NUMBER_OF_ELEMENTS - 1; 0 <= i;) {
      // Remove in batches of 5
      List<Integer> indexesToRemove = new ArrayList<>(5);
      for (int j = 0; j < 5 && i >= 0; j++) {
        indexesToRemove.add(0, i--);
      }
      list.removeIndexes(indexesToRemove);
      assertThat(list.getSizeInBytes()).isEqualTo(sizer.sizeof(list));
    }
    assertThat(list.size()).isEqualTo(0);
  }

  @Test
  public void removeIndex_getSizeInBytesIsAccurate() {
    // Create a list with an initial size and confirm that it correctly reports its size
    SizeableByteArrayList list = createList();
    assertThat(list.getSizeInBytes()).isEqualTo(sizer.sizeof(list));

    // Remove all the elements and assert that the size is correct after each remove
    for (int i = INITIAL_NUMBER_OF_ELEMENTS - 1; 0 <= i; --i) {
      list.remove(i);
      assertThat(list.getSizeInBytes()).isEqualTo(sizer.sizeof(list));
    }
    assertThat(list.size()).isEqualTo(0);
  }

  @Test
  public void set_getSizeInBytesIsAccurate() {
    // Create a list with one initial element and confirm that it correctly reports its size
    SizeableByteArrayList list = new SizeableByteArrayList();
    byte[] element = "element name".getBytes();
    list.addFirst(element);
    long initialSize = list.getSizeInBytes();
    assertThat(initialSize).isEqualTo(sizer.sizeof(list));

    // Set the list's element to a larger element and ensure the size is correct
    byte[] largerElement = "a larger updated element name".getBytes();
    list.set(0, largerElement);
    assertThat(list.getSizeInBytes()).isEqualTo(sizer.sizeof(list));

    // Revert the list's element to the original value and ensure size is consistent
    list.set(0, element);
    assertThat(list.getSizeInBytes()).isEqualTo(initialSize);
  }

  @Test
  public void addElementAtIndex_getSizeInBytesIsAccurate() {
    // Create a new list and confirm that it correctly reports its size
    SizeableByteArrayList list = createList();
    assertThat(list.getSizeInBytes()).isEqualTo(sizer.sizeof(list));

    // Add an element by index and assert size is updated
    byte[] element = "element name".getBytes();
    list.add(1, element);
    long sizeAfterAddingElement = list.getSizeInBytes();
    assertThat(sizeAfterAddingElement).isEqualTo(sizer.sizeof(list));
  }

  @Test
  public void addFirst_getSizeInBytesIsAccurate() {
    // Create a new list and confirm that it correctly reports its size
    SizeableByteArrayList list = createList();
    assertThat(list.getSizeInBytes()).isEqualTo(sizer.sizeof(list));

    // Add an element and assert size is updated
    byte[] element = "element name".getBytes();
    list.addFirst(element);
    long sizeAfterAddingElement = list.getSizeInBytes();
    assertThat(sizeAfterAddingElement).isEqualTo(sizer.sizeof(list));
  }

  @Test
  public void addLast_getSizeInBytesIsAccurate() {
    // Create a new list and confirm that it correctly reports its size
    SizeableByteArrayList list = createList();
    assertThat(list.getSizeInBytes()).isEqualTo(sizer.sizeof(list));

    // Add an element and assert size is updated
    byte[] element = "element name".getBytes();
    list.addLast(element);
    long sizeAfterAddingElement = list.getSizeInBytes();
    assertThat(sizeAfterAddingElement).isEqualTo(sizer.sizeof(list));
  }

  @Test
  public void insertElementBeforeReferenceElement_getSizeInBytesIsAccurate() {
    // Create a new list and confirm that it correctly reports its size
    SizeableByteArrayList list = new SizeableByteArrayList();
    byte[] referenceElement = "element".getBytes();
    list.addFirst(referenceElement);
    long initialSize = list.getSizeInBytes();
    assertThat(initialSize).isEqualTo(sizer.sizeof(list));

    // Insert an element by reference and assert size is updated
    byte[] beforeElement = "before".getBytes();
    list.insert(beforeElement, referenceElement, true);

    long sizeAfterAddingElement = list.getSizeInBytes();
    assertThat(sizeAfterAddingElement).isEqualTo(sizer.sizeof(list));
  }

  @Test
  public void insertElementAfterReferenceElement_getSizeInBytesIsAccurate() {
    // Create a new list and confirm that it correctly reports its size
    SizeableByteArrayList list = new SizeableByteArrayList();
    byte[] referenceElement = "element".getBytes();
    list.addFirst(referenceElement);
    long initialSize = list.getSizeInBytes();
    assertThat(initialSize).isEqualTo(sizer.sizeof(list));

    // Insert an element by reference and assert size is updated
    byte[] beforeElement = "after".getBytes();
    list.insert(beforeElement, referenceElement, false);

    long sizeAfterAddingElement = list.getSizeInBytes();
    assertThat(sizeAfterAddingElement).isEqualTo(sizer.sizeof(list));
  }

  @Test
  public void insertElementBeforeReferenceElement_placesElementCorrectly() {
    // Create a new list with a single element
    SizeableByteArrayList list = new SizeableByteArrayList();
    byte[] referenceElement = "element".getBytes();
    list.addFirst(referenceElement);

    // Insert new element before reference element
    byte[] beforeElement = "before".getBytes();
    list.insert(beforeElement, referenceElement, true);

    // Assert list contains exactly the elements in the expected order
    assertThat(list).containsExactly(beforeElement, referenceElement);
  }

  @Test
  public void insertElementAfterReferenceElement_placesElementCorrectly() {
    // Create a new list with a single element
    SizeableByteArrayList list = new SizeableByteArrayList();
    byte[] referenceElement = "element".getBytes();
    list.addFirst(referenceElement);

    // Insert new element after reference element
    byte[] afterElement = "after".getBytes();
    list.insert(afterElement, referenceElement, false);

    // Assert list contains exactly the elements in the expected order
    assertThat(list).containsExactly(referenceElement, afterElement);
  }

  @Test
  public void insertElementAfterNonexistentReferenceElement_doesNotPlaceElement() {
    // Create a new list with a single element
    SizeableByteArrayList list = new SizeableByteArrayList();
    byte[] nonExistentElement = "non-existent-element".getBytes();

    // Attempt to insert an element after a non-existent reference element
    byte[] afterElement = "after".getBytes();
    list.insert(afterElement, nonExistentElement, false);

    // Assert that no elements were added to the list
    assertThat(list).isEmpty();
  }

  private SizeableByteArrayList setupList(int size) {
    SizeableByteArrayList list = new SizeableByteArrayList();
    for (int i = 0; i < size; i++) {
      list.addLast(String.valueOf(i).getBytes());
    }
    return list;
  }



  private SizeableByteArrayList setupListWithDuplicateValues(String repeatedElement) {
    SizeableByteArrayList list = new SizeableByteArrayList();
    byte[] bytes = repeatedElement.getBytes();
    for (int i = 0; i < 6; i++) {
      list.addLast(bytes);
      list.addLast(String.valueOf(i).getBytes());
    }
    list.addLast(bytes);
    return list;
  }

  private SizeableByteArrayList createList() {
    SizeableByteArrayList list = new SizeableByteArrayList();
    for (int i = 0; i < INITIAL_NUMBER_OF_ELEMENTS; ++i) {
      list.addFirst(makeByteArrayOfSpecifiedLength(i + 1));
    }
    return list;
  }

  private byte[] makeByteArrayOfSpecifiedLength(int length) {
    byte[] newByteArray = new byte[length];
    for (int i = 0; i < length; i++) {
      newByteArray[i] = (byte) i;
    }
    return newByteArray;
  }

}
