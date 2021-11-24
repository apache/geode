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

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import com.pholser.junit.quickcheck.Property;
import com.pholser.junit.quickcheck.generator.InRange;
import com.pholser.junit.quickcheck.generator.Size;
import com.pholser.junit.quickcheck.runner.JUnitQuickcheck;
import org.junit.runner.RunWith;

@RunWith(JUnitQuickcheck.class)
public class SizeableBytes2ObjectOpenCustomHashMapWithCursorQuickCheckTest {

  private static class Bytes2IntegerMap
      extends SizeableBytes2ObjectOpenCustomHashMapWithCursor<Integer> {

    public Bytes2IntegerMap() {
      super();
    }

    @Override
    protected int sizeValue(Integer value) {
      return 0;
    }
  }


  @Property
  public void scanWithConcurrentModifications_ReturnsExpectedElements(
      @Size(min = 2, max = 500) Set<@InRange(minInt = 0, maxInt = 500) Integer> initialData,
      @Size(max = 500) Set<@InRange(minInt = 0, maxInt = 1000) Integer> dataToAdd,
      @Size(max = 500) Set<@InRange(minInt = 0, maxInt = 500) Integer> keysToRemove) {
    Bytes2IntegerMap map = new Bytes2IntegerMap();
    initialData.forEach(i -> map.put(i.toString().getBytes(), i));

    List<Integer> scanned = new ArrayList<>();
    int cursor =
        map.scan(0, initialData.size() / 2, (data, key, value) -> data.add(value), scanned);

    dataToAdd.forEach(i -> map.put(i.toString().getBytes(), i));
    keysToRemove.forEach(i -> map.remove(i.toString().getBytes()));

    cursor = map.scan(cursor, 100000, (data, key, value) -> data.add(value), scanned);
    assertThat(cursor).isEqualTo(0);

    // Test that we can scan all of the entries what were in the map the entire time.
    Set<Integer> expectedKeys = new HashSet<>(initialData);
    expectedKeys.removeAll(keysToRemove);
    assertThat(scanned).containsAll(expectedKeys);
  }

  @Property
  public void scanWithNoModificationsDoesNotReturnDuplicates(
      @Size(min = 2, max = 500) Set<@InRange(minInt = 0, maxInt = 500) Integer> initialData) {
    Bytes2IntegerMap map = new Bytes2IntegerMap();
    initialData.forEach(i -> map.put(i.toString().getBytes(), i));

    List<Integer> scanned = new ArrayList<>();
    int cursor =
        map.scan(0, initialData.size() / 2, (data, key, value) -> data.add(value), scanned);

    cursor = map.scan(cursor, 100000, (data, key, value) -> data.add(value), scanned);
    assertThat(cursor).isEqualTo(0);

    // Test that no duplicate entries were added and no entries were missed.
    assertThat(scanned).hasSize(initialData.size());
    assertThat(scanned).containsExactlyInAnyOrderElementsOf(initialData);
  }
}
