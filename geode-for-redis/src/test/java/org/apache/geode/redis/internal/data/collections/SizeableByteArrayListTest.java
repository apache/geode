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

import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;
import java.util.Random;

import org.junit.Test;

import org.apache.geode.cache.util.ObjectSizer;
import org.apache.geode.internal.size.ReflectionObjectSizer;

public class SizeableByteArrayListTest {
  private final ObjectSizer sizer = ReflectionObjectSizer.getInstance();
  private final int INITIAL_NUMBER_OF_ELEMENTS = 20;

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
  public void removeObjects_getSizeInBytesIsAccurate() {
    // Create a list with an initial size and confirm that it correctly reports its size
    SizeableByteArrayList list = createList();
    assertThat(list.getSizeInBytes()).isEqualTo(sizer.sizeof(list));

    // Remove all the elements and assert that the size is correct after each remove
    Random rand = new Random();
    for (int i = 0; i < INITIAL_NUMBER_OF_ELEMENTS; ++i) {
      list.remove(makeByteArrayOfSpecifiedLength(i + 1), rand.nextInt(3) - 1);
      assertThat(list.getSizeInBytes()).isEqualTo(sizer.sizeof(list));
    }
    assertThat(list.size()).isEqualTo(0);
  }

  @Test
  public void removeObject_getSizeInBytesIsAccurate() {
    // Create a list with an initial size and confirm that it correctly reports its size
    SizeableByteArrayList list = createList();
    assertThat(list.getSizeInBytes()).isEqualTo(sizer.sizeof(list));

    // Remove all the elements and assert that the size is correct after each remove
    Random rand = new Random();
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
    byte[] element = "element name".getBytes(StandardCharsets.UTF_8);
    list.addFirst(element);
    long initialSize = list.getSizeInBytes();
    assertThat(initialSize).isEqualTo(sizer.sizeof(list));

    // Set the list's element to a larger element and ensure the size is correct
    byte[] largerElement = "a larger updated element name".getBytes(StandardCharsets.UTF_8);
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
    byte[] element = "element name".getBytes(StandardCharsets.UTF_8);
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
    byte[] element = "element name".getBytes(StandardCharsets.UTF_8);
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
    byte[] element = "element name".getBytes(StandardCharsets.UTF_8);
    list.addLast(element);
    long sizeAfterAddingElement = list.getSizeInBytes();
    assertThat(sizeAfterAddingElement).isEqualTo(sizer.sizeof(list));
  }

  @Test
  public void insertElementBeforeReferenceElement_getSizeInBytesIsAccurate() {
    // Create a new list and confirm that it correctly reports its size
    SizeableByteArrayList list = new SizeableByteArrayList();
    byte[] referenceElement = "element".getBytes(StandardCharsets.UTF_8);
    list.addFirst(referenceElement);
    long initialSize = list.getSizeInBytes();
    assertThat(initialSize).isEqualTo(sizer.sizeof(list));

    // Insert an element by reference and assert size is updated
    byte[] beforeElement = "before".getBytes(StandardCharsets.UTF_8);
    list.insert(beforeElement, referenceElement, true);

    long sizeAfterAddingElement = list.getSizeInBytes();
    assertThat(sizeAfterAddingElement).isEqualTo(sizer.sizeof(list));
  }

  @Test
  public void insertElementAfterReferenceElement_getSizeInBytesIsAccurate() {
    // Create a new list and confirm that it correctly reports its size
    SizeableByteArrayList list = new SizeableByteArrayList();
    byte[] referenceElement = "element".getBytes(StandardCharsets.UTF_8);
    list.addFirst(referenceElement);
    long initialSize = list.getSizeInBytes();
    assertThat(initialSize).isEqualTo(sizer.sizeof(list));

    // Insert an element by reference and assert size is updated
    byte[] beforeElement = "after".getBytes(StandardCharsets.UTF_8);
    list.insert(beforeElement, referenceElement, false);

    long sizeAfterAddingElement = list.getSizeInBytes();
    assertThat(sizeAfterAddingElement).isEqualTo(sizer.sizeof(list));
  }

  @Test
  public void insertElementBeforeReferenceElement_placesElementCorrectly() {
    // Create a new list with a single element
    SizeableByteArrayList list = new SizeableByteArrayList();
    byte[] referenceElement = "element".getBytes(StandardCharsets.UTF_8);
    list.addFirst(referenceElement);

    // Insert new element before reference element
    byte[] beforeElement = "before".getBytes(StandardCharsets.UTF_8);
    list.insert(beforeElement, referenceElement, true);

    // Assert list contains exactly the elements in the expected order
    assertThat(list).containsExactly(beforeElement, referenceElement);
  }

  @Test
  public void insertElementAfterReferenceElement_placesElementCorrectly() {
    // Create a new list with a single element
    SizeableByteArrayList list = new SizeableByteArrayList();
    byte[] referenceElement = "element".getBytes(StandardCharsets.UTF_8);
    list.addFirst(referenceElement);

    // Insert new element after reference element
    byte[] afterElement = "after".getBytes(StandardCharsets.UTF_8);
    list.insert(afterElement, referenceElement, false);

    // Assert list contains exactly the elements in the expected order
    assertThat(list).containsExactly(referenceElement, afterElement);
  }

  @Test
  public void insertElementAfterNonexistentReferenceElement_doesNotPlaceElement() {
    // Create a new list with a single element
    SizeableByteArrayList list = new SizeableByteArrayList();
    byte[] nonExistentElement = "non-existent-element".getBytes(StandardCharsets.UTF_8);

    // Attempt to insert an element after a non-existent reference element
    byte[] afterElement = "after".getBytes(StandardCharsets.UTF_8);
    list.insert(afterElement, nonExistentElement, false);

    // Assert that no elements were added to the list
    assertThat(list).isEmpty();
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
