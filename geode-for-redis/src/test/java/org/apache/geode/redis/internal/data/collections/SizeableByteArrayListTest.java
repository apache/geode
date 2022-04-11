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

import java.util.Arrays;
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
  public void remove_removesElementsFromHeadWhenCountIsPositive() {
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
  public void remove_removesElementsFromTailWhenCountIsNegative() {
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
  public void remove_removesAllElementsWhenCountIsZero() {
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
  public void remove_removesAllElementsWhenCountIsGreaterThanSize() {
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
  public void remove_removesAllElementsWhenCountIsGreaterThanSizeWithNegativeCount() {
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
    for (int i = 0; i > INITIAL_NUMBER_OF_ELEMENTS; i++) {
      assertThat(list.indexOf(list.get(i))).isEqualTo(i);
    }
  }

  @Test
  public void indexOf_returnsNegativeOneWhenElementDoesNotExist() {
    SizeableByteArrayList list = setupList(2);
    assertThat(list.getSizeInBytes()).isEqualTo(sizer.sizeof(list));
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
        new Object[] {Arrays.asList(0, 0),
            new byte[][] {"1".getBytes(), "2".getBytes(), "3".getBytes(), "4".getBytes()}},
        new Object[] {Arrays.asList(4, 0),
            new byte[][] {"0".getBytes(), "1".getBytes(), "2".getBytes(), "3".getBytes(),
                "4".getBytes()}},
        new Object[] {Arrays.asList(2, 2),
            new byte[][] {"0".getBytes(), "1".getBytes(), "3".getBytes(), "4".getBytes()}},
        new Object[] {Arrays.asList(5, 1),
            new byte[][] {"0".getBytes(), "1".getBytes(), "2".getBytes(), "3".getBytes(),
                "4".getBytes()}},
        new Object[] {Arrays.asList(0, 4),
            new byte[][] {"1".getBytes(), "2".getBytes(), "3".getBytes()}},
        new Object[] {Arrays.asList(1, 2, 3, 4), new byte[][] {"0".getBytes()}},
        new Object[] {Arrays.asList(2, 3),
            new byte[][] {"0".getBytes(), "1".getBytes(), "4".getBytes()}},
        new Object[] {Arrays.asList(1, 2, -1),
            new byte[][] {"0".getBytes(), "1".getBytes(), "2".getBytes(), "3".getBytes(),
                "4".getBytes()}},
    };
  }

  @Test
  public void removeIndexes_throwsIndexOutOfBoundsExceptionForInvalidIndexList() {
    int size = 5;
    SizeableByteArrayList list = setupList(size);
    assertThatThrownBy(() -> list.removeIndexes(Arrays.asList(-1))).isInstanceOf(
        IndexOutOfBoundsException.class);
    assertThatThrownBy(() -> list.removeIndexes(Arrays.asList(-1, 2, 3, 4))).isInstanceOf(
        IndexOutOfBoundsException.class);
  }

  @Test
  public void remove_WithElement_removesGivenElementIfElementToRemoveDoesExists() {
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
  public void remove_WithIndex_removesGivenIndexElementIsValid() {
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
  public void remove_WithIndex_throwsIndexOutOfBoundsExceptionIfIndexElementIsInvalid() {
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
  public void set_setsNewElementAtGivenIndexIsValid() {
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
  public void set_throwsIndexOutOfBoundsExceptionIfIndexElementIsInvalid() {
    int size = 5;
    byte[] newElement = "new Element".getBytes();
    SizeableByteArrayList list = setupList(size);
    assertThatThrownBy(() -> list.set(size, newElement)).isInstanceOf(
        IndexOutOfBoundsException.class);
    assertThatThrownBy(() -> list.set(-1, newElement)).isInstanceOf(
        IndexOutOfBoundsException.class);
  }

  @Test
  public void add_addsNewElementAtGivenIndexIsValid() {
    int size = 5;
    SizeableByteArrayList list = setupList(size);

    List<byte[]> expectedList =
        IntStream.range(0, size).mapToObj(i -> String.valueOf(i).getBytes())
            .collect(Collectors.toCollection(LinkedList::new));
    assertThat(list).containsExactlyElementsOf(expectedList);

    byte[] newElement = "new Element".getBytes();
    int indexToBeSet = 5;

    list.add(indexToBeSet, newElement);
    expectedList.add(indexToBeSet, newElement);
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
  @TestCaseName("{method}: elementToInsert:{0}, referenceElement:{1}, before{2}, expectedList{3}, expectedIndex{4}")
  public void insert_insertsGivenElementForValidInputs(byte[] elementToInsert,
      byte[] referenceElement,
      boolean before,
      byte[][] expectedList,
      int expectedIndex) {
    int size = 5;
    SizeableByteArrayList list = setupList(size);
    int insertedIndex = list.insert(elementToInsert, referenceElement, before);
    assertThat(insertedIndex).isEqualTo(expectedIndex);
    assertThat(list).containsExactly(expectedList);
  }

  @SuppressWarnings("unused")
  private Object[] getValidArgumentsForInsert() {
    // Values are elementToInsert,referenceElement,before, expectedList, expectedIndex
    // For initial list of size 5
    byte[] newElement = "newElement".getBytes();
    return new Object[] {
        new Object[] {newElement, "2".getBytes(), true,
            new byte[][] {"0".getBytes(), "1".getBytes(), newElement, "2".getBytes(),
                "3".getBytes(),
                "4".getBytes()},
            2},
        new Object[] {newElement, "3".getBytes(), false,
            new byte[][] {"0".getBytes(), "1".getBytes(), "2".getBytes(), "3".getBytes(),
                newElement,
                "4".getBytes()},
            4},
        new Object[] {newElement, "0".getBytes(), true,
            new byte[][] {newElement, "0".getBytes(), "1".getBytes(), "2".getBytes(),
                "3".getBytes(),
                "4".getBytes()},
            0},
        new Object[] {newElement, "4".getBytes(), true,
            new byte[][] {"0".getBytes(), "1".getBytes(), "2".getBytes(), "3".getBytes(),
                newElement,
                "4".getBytes()},
            4},
        new Object[] {newElement, "0".getBytes(), false,
            new byte[][] {"0".getBytes(), newElement, "1".getBytes(), "2".getBytes(),
                "3".getBytes(),
                "4".getBytes()},
            1},
        new Object[] {newElement, "4".getBytes(), false,
            new byte[][] {"0".getBytes(), "1".getBytes(), "2".getBytes(), "3".getBytes(),
                "4".getBytes(), newElement},
            5},
        new Object[] {newElement, "5".getBytes(), true,
            new byte[][] {"0".getBytes(), "1".getBytes(), "2".getBytes(), "3".getBytes(),
                "4".getBytes()},
            -1},
        new Object[] {newElement, "-1".getBytes(), true,
            new byte[][] {"0".getBytes(), "1".getBytes(), "2".getBytes(), "3".getBytes(),
                "4".getBytes()},
            -1}
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

  private byte[] makeByteArrayOfSpecifiedLength(int length) {
    byte[] newByteArray = new byte[length];
    for (int i = 0; i < length; i++) {
      newByteArray[i] = (byte) i;
    }
    return newByteArray;
  }

}
