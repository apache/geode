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
package org.apache.geode.test.dunit.rules.tests;

import static java.util.Arrays.asList;
import static org.apache.geode.test.dunit.VM.getAllVMs;
import static org.apache.geode.test.dunit.VM.getController;
import static org.apache.geode.test.dunit.VM.getVM;
import static org.apache.geode.test.dunit.VM.getVMId;
import static org.apache.geode.test.dunit.VM.toArray;
import static org.apache.geode.test.junit.runners.TestRunner.runTestWithValidation;
import static org.assertj.core.api.Assertions.assertThat;

import java.io.Serializable;
import java.util.Collection;
import java.util.HashMap;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;
import java.util.concurrent.atomic.AtomicReference;

import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;

import org.apache.geode.test.dunit.VM;
import org.apache.geode.test.dunit.rules.DistributedMap;

@SuppressWarnings("serial")
public class DistributedMapTest implements Serializable {

  @Rule
  public DistributedMap<Object, Object> distributedMap = new DistributedMap<>();

  @Test
  public void isEmptyIsTrueByDefault() {
    boolean empty = distributedMap.isEmpty();

    assertThat(empty).isTrue();
  }

  @Test
  public void sizeIsZeroWhenEmpty() {
    int size = distributedMap.size();

    assertThat(size).isZero();
  }

  @Test
  public void getReturnsNullByDefault() {
    Object value = distributedMap.get("key");

    assertThat(value).isNull();
  }

  @Test
  public void getReturnsValueOfPut() {
    distributedMap.put("key", "value");

    Object value = distributedMap.get("key");

    assertThat(value).isEqualTo("value");
  }

  @Test
  public void getReturnsValueOfLatestPut() {
    distributedMap.put("key", "value1");
    distributedMap.put("key", "value2");
    distributedMap.put("key", "value3");

    Object value = distributedMap.get("key");

    assertThat(value).isEqualTo("value3");
  }

  @Test
  public void getReturnsSameValueRepeatedly() {
    distributedMap.put("key", "value");

    Object value1 = distributedMap.get("key");
    Object value2 = distributedMap.get("key");
    Object value3 = distributedMap.get("key");

    assertThat(value3).isEqualTo(value2).isEqualTo(value1);
  }

  @Test
  public void containsKeyIsFalseWhenEntryDoesNotExist() {
    boolean containsKey = distributedMap.containsKey("key");

    assertThat(containsKey).isFalse();
  }

  @Test
  public void containsKeyIsTrueWhenEntryExists() {
    distributedMap.put("key", "value");

    boolean containsKey = distributedMap.containsKey("key");

    assertThat(containsKey).isTrue();
  }

  @Test
  public void containsValueIsFalseWhenEntryDoesNotExist() {
    boolean containsValue = distributedMap.containsValue("value");

    assertThat(containsValue).isFalse();
  }

  @Test
  public void containsValueIsTrueWhenEntryExists() {
    distributedMap.put("key", "value");

    boolean containsValue = distributedMap.containsValue("value");

    assertThat(containsValue).isTrue();
  }

  @Test
  public void getReturnsNullAfterRemove() {
    distributedMap.put("key", "value");

    distributedMap.remove("key");

    assertThat(distributedMap.get("key")).isNull();
  }

  @Test
  public void containsKeyIsFalseAfterRemove() {
    distributedMap.put("key", "value");

    distributedMap.remove("key");

    assertThat(distributedMap.containsKey("key")).isFalse();
  }

  @Test
  public void containsValueIsFalseAfterRemove() {
    distributedMap.put("key", "value");

    distributedMap.remove("key");

    assertThat(distributedMap.containsValue("value")).isFalse();
  }

  @Test
  public void otherKeyStillExistsAfterRemove() {
    distributedMap.put("key1", "value1");
    distributedMap.put("key2", "value2");

    distributedMap.remove("key1");

    assertThat(distributedMap.containsKey("key2")).isTrue();
  }

  @Test
  public void otherValueStillExistsAfterRemove() {
    distributedMap.put("key1", "value1");
    distributedMap.put("key2", "value2");

    distributedMap.remove("key1");

    assertThat(distributedMap.containsValue("value2")).isTrue();
  }

  @Test
  public void isEmptyIsFalseAfterPut() {
    distributedMap.put("key", "value");

    boolean isEmpty = distributedMap.isEmpty();

    assertThat(isEmpty).isFalse();
  }

  @Test
  public void isEmptyIsTrueAfterClear() {
    distributedMap.put("key", "value");

    distributedMap.clear();

    assertThat(distributedMap.isEmpty()).isTrue();
  }

  @Test
  public void sizeReturnsNumberOfEntries() {
    distributedMap.put("key1", "value1");
    distributedMap.put("key2", "value2");
    distributedMap.put("key3", "value3");

    int size = distributedMap.size();

    assertThat(size).isEqualTo(3);
  }

  @Test
  public void sizeIsZeroAfterClear() {
    distributedMap.put("key1", "value1");
    distributedMap.put("key2", "value2");
    distributedMap.put("key3", "value3");

    distributedMap.clear();

    assertThat(distributedMap.size()).isZero();
  }

  @Test
  public void keySetReturnsSetOfAllKeys() {
    distributedMap.put("key1", "value1");
    distributedMap.put("key2", "value2");
    distributedMap.put("key3", "value3");

    Set<Object> keySet = distributedMap.keySet();

    assertThat(keySet).containsExactly("key1", "key2", "key3");
  }

  @Test
  public void valuesReturnsCollectionOfAllValues() {
    distributedMap.put("key1", "value1");
    distributedMap.put("key2", "value2");
    distributedMap.put("key3", "value3");

    Collection<Object> values = distributedMap.values();

    assertThat(values).containsExactlyInAnyOrder("value1", "value2", "value3");
  }

  @Test
  public void entrySetReturnsSetOfAllEntries() {
    Map<Object, Object> expectedEntries = new HashMap<>();
    expectedEntries.put("key1", "value1");
    expectedEntries.put("key2", "value2");
    expectedEntries.put("key3", "value3");
    distributedMap.put("key1", "value1");
    distributedMap.put("key2", "value2");
    distributedMap.put("key3", "value3");

    Set<Entry<Object, Object>> entrySet = distributedMap.entrySet();

    assertThat(entrySet).isEqualTo(expectedEntries.entrySet());
  }

  @Test
  public void equalsReturnsTrueIfOtherMapContainsSameEntries() {
    Map<Object, Object> otherMap = new HashMap<>();
    otherMap.put("key1", "value1");
    otherMap.put("key2", "value2");
    otherMap.put("key3", "value3");
    distributedMap.put("key1", "value1");
    distributedMap.put("key2", "value2");
    distributedMap.put("key3", "value3");

    boolean equals = distributedMap.equals(otherMap);
    boolean otherEquals = otherMap.equals(distributedMap);

    assertThat(equals).isEqualTo(otherEquals).isTrue();
  }

  @Test
  public void equalsReturnsFalseIfOtherMapIsMissingEntry() {
    Map<Object, Object> otherMap = new HashMap<>();
    otherMap.put("key1", "value1");
    otherMap.put("key2", "value2");
    distributedMap.put("key1", "value1");
    distributedMap.put("key2", "value2");
    distributedMap.put("key3", "value3");

    boolean equals = distributedMap.equals(otherMap);
    boolean otherEquals = otherMap.equals(distributedMap);

    assertThat(equals).isEqualTo(otherEquals).isFalse();
  }

  @Test
  public void equalsReturnsFalseIfOtherMapContainsExtraEntry() {
    Map<Object, Object> otherMap = new HashMap<>();
    otherMap.put("key1", "value1");
    otherMap.put("key2", "value2");
    otherMap.put("key3", "value3");
    otherMap.put("key4", "value4");
    distributedMap.put("key1", "value1");
    distributedMap.put("key2", "value2");
    distributedMap.put("key3", "value3");

    boolean equals = distributedMap.equals(otherMap);
    boolean otherEquals = otherMap.equals(distributedMap);

    assertThat(equals).isEqualTo(otherEquals).isFalse();
  }

  @Test
  public void equalsReturnsFalseIfOtherMapContainsDifferentEntries() {
    Map<Object, Object> otherMap = new HashMap<>();
    otherMap.put("keyA", "valueA");
    otherMap.put("keyB", "valueB");
    otherMap.put("keyC", "valueC");
    distributedMap.put("key1", "value1");
    distributedMap.put("key2", "value2");
    distributedMap.put("key3", "value3");

    boolean equals = distributedMap.equals(otherMap);
    boolean otherEquals = otherMap.equals(distributedMap);

    assertThat(equals).isEqualTo(otherEquals).isFalse();
  }

  @Test
  public void hashCodeEqualsOtherHashCodeIfOtherMapContainsSameEntries() {
    Map<Object, Object> otherMap = new HashMap<>();
    otherMap.put("key1", "value1");
    otherMap.put("key2", "value2");
    otherMap.put("key3", "value3");
    distributedMap.put("key1", "value1");
    distributedMap.put("key2", "value2");
    distributedMap.put("key3", "value3");

    int thisHashCode = distributedMap.hashCode();
    int otherHashCode = otherMap.hashCode();

    assertThat(thisHashCode).isEqualTo(otherHashCode);
  }

  @Test
  public void hashCodeDoesNotEqualOtherHashCodeIfOtherMapContainsIsMissingEntry() {
    Map<Object, Object> otherMap = new HashMap<>();
    otherMap.put("key1", "value1");
    otherMap.put("key2", "value2");
    distributedMap.put("key1", "value1");
    distributedMap.put("key2", "value2");
    distributedMap.put("key3", "value3");

    int thisHashCode = distributedMap.hashCode();
    int otherHashCode = otherMap.hashCode();

    assertThat(thisHashCode).isNotEqualTo(otherHashCode);
  }

  @Test
  public void hashCodeDoesNotEqualOtherHashCodeIfOtherMapContainsExtraEntry() {
    Map<Object, Object> otherMap = new HashMap<>();
    otherMap.put("key1", "value1");
    otherMap.put("key2", "value2");
    otherMap.put("key3", "value3");
    otherMap.put("key4", "value4");
    distributedMap.put("key1", "value1");
    distributedMap.put("key2", "value2");
    distributedMap.put("key3", "value3");

    int thisHashCode = distributedMap.hashCode();
    int otherHashCode = otherMap.hashCode();

    assertThat(thisHashCode).isNotEqualTo(otherHashCode);
  }

  @Test
  public void hashCodeDoesNotEqualOtherHashCodeIfOtherMapContainsDifferentEntries() {
    Map<Object, Object> otherMap = new HashMap<>();
    otherMap.put("keyA", "valueA");
    otherMap.put("keyB", "valueB");
    otherMap.put("keyC", "valueC");
    distributedMap.put("key1", "value1");
    distributedMap.put("key2", "value2");
    distributedMap.put("key3", "value3");

    int thisHashCode = distributedMap.hashCode();
    int otherHashCode = otherMap.hashCode();

    assertThat(thisHashCode).isNotEqualTo(otherHashCode);
  }

  @Test
  public void toStringReturnsSameToStringAsHashMap() {
    Map<Object, Object> otherMap = new HashMap<>();
    otherMap.put("key1", "value1");
    otherMap.put("key2", "value2");
    otherMap.put("key3", "value3");
    distributedMap.put("key1", "value1");
    distributedMap.put("key2", "value2");
    distributedMap.put("key3", "value3");

    String toString = distributedMap.toString();
    String otherToString = otherMap.toString();

    assertThat(toString).isEqualTo(otherToString);
  }

  @Test
  public void getReturnsSameValueInControllerAsPutInController() {
    getController().invoke(() -> {
      distributedMap.put("key1", "value1");
    });

    getController().invoke(() -> {
      Object value = distributedMap.get("key1");

      assertThat(value).isEqualTo("value1");
    });
  }

  @Test
  public void getReturnsSameValueInControllerAsPutInController2() {
    getController().invoke(() -> {
      distributedMap.put("key1", "value1");
    });

    Object value = distributedMap.get("key1");

    assertThat(value).isEqualTo("value1");
  }

  @Test
  public void getReturnsSameValueInControllerAsPutInController3() {
    distributedMap.put("key1", "value1");

    getController().invoke(() -> {
      Object value = distributedMap.get("key1");

      assertThat(value).isEqualTo("value1");
    });
  }

  @Test
  public void getReturnsSameValueInControllerAsPutInOtherVM() {
    getVM(0).invoke(() -> {
      distributedMap.put("key1", "value1");
    });

    Object value = distributedMap.get("key1");

    assertThat(value).isEqualTo("value1");
  }

  @Test
  public void getReturnsSameValueInEveryVmAsPutInController() {
    distributedMap.put("key1", "value1");

    for (VM vm : toArray(getAllVMs(), getController())) {
      vm.invoke(() -> {
        Object value = distributedMap.get("key1");

        assertThat(value).isEqualTo("value1");
      });
    }
  }

  @Test
  public void getReturnsSameValueInSameVmAsPut() {
    getVM(0).invoke(() -> {
      distributedMap.put("key1", "value1");
    });

    getVM(0).invoke(() -> {
      Object value = distributedMap.get("key1");

      assertThat(value).isEqualTo("value1");
    });
  }

  @Test
  public void getReturnsSameValueInSameVmAsPut2() {
    getVM(0).invoke(() -> {
      distributedMap.put("key1", "value1");

      Object value = distributedMap.get("key1");

      assertThat(value).isEqualTo("value1");
    });
  }

  @Test
  public void getReturnsSameValueInEveryVmAsPutInOtherVm() {
    getVM(0).invoke(() -> {
      distributedMap.put("key1", "value1");
    });

    for (VM vm : toArray(getAllVMs(), getController())) {
      vm.invoke(() -> {
        Object value = distributedMap.get("key1");

        assertThat(value)
            .as("value in " + getVMId())
            .isEqualTo("value1");
      });
    }
  }

  @Test
  public void accessesDistributedMapInEachVm() {
    runTestWithValidation(HasDistributedMap.class);
  }

  @Test
  public void tearsDownDistributedMapInEachVm() {
    runTestWithValidation(HasDistributedMap.class);

    getController().invoke(() -> {
      assertThat(HasDistributedMap.map.get()).isEmpty();
    });
  }

  @Test
  public void accessesTwoDistributedMapsInEachVm() {
    runTestWithValidation(HasTwoDistributedMaps.class);
  }

  @Test
  public void tearsDownTwoDistributedMapsInEachVm() {
    runTestWithValidation(HasTwoDistributedMaps.class);

    getController().invoke(() -> {
      assertThat(HasTwoDistributedMaps.map1.get()).isEmpty();
      assertThat(HasTwoDistributedMaps.map2.get()).isEmpty();
    });
  }

  @Test
  public void accessesManyDistributedMapsInEachVm() {
    runTestWithValidation(HasManyDistributedMaps.class);
  }

  @Test
  public void tearsDownManyDistributedMapsInEachVm() {
    runTestWithValidation(HasManyDistributedMaps.class);

    getController().invoke(() -> {
      assertThat(HasManyDistributedMaps.map1.get()).isEmpty();
      assertThat(HasManyDistributedMaps.map2.get()).isEmpty();
      assertThat(HasManyDistributedMaps.map3.get()).isEmpty();
    });
  }

  public static class HasDistributedMap implements Serializable {

    private static final AtomicReference<Map<Object, Object>> map = new AtomicReference<>();

    @Rule
    public DistributedMap<Object, Object> distributedMap = new DistributedMap<>();

    @Before
    public void setUp() {
      getController().invoke(() -> {
        map.set(distributedMap.map());
        distributedMap.put("key1", "value1");
      });
    }

    @Test
    public void distributedMapIsAccessibleInEveryVm() {
      for (VM vm : asList(getVM(0), getVM(1), getVM(2), getVM(3), getController())) {
        vm.invoke(() -> {
          assertThat(distributedMap.map()).isSameAs(map.get());
          assertThat(distributedMap.get("key1")).isEqualTo("value1");
        });
      }
    }
  }

  public static class HasTwoDistributedMaps implements Serializable {

    private static final AtomicReference<Map<Object, Object>> map1 = new AtomicReference<>();
    private static final AtomicReference<Map<Object, Object>> map2 = new AtomicReference<>();

    @Rule
    public DistributedMap<Object, Object> distributedMap1 = new DistributedMap<>();
    @Rule
    public DistributedMap<Object, Object> distributedMap2 = new DistributedMap<>();

    @Before
    public void setUp() {
      getController().invoke(() -> {
        map1.set(distributedMap1.map());
        distributedMap1.put("key1", "value1");

        map2.set(distributedMap2.map());
        distributedMap2.put("key2", "value2");
      });
    }

    @Test
    public void twoDistributedMapsAreAccessibleInEveryVm() {
      for (VM vm : asList(getVM(0), getVM(1), getVM(2), getVM(3), getController())) {
        vm.invoke(() -> {
          assertThat(distributedMap1.map()).isSameAs(map1.get());
          assertThat(distributedMap1.get("key1")).isEqualTo("value1");
          assertThat(distributedMap1.get("key2")).isNull();

          assertThat(distributedMap2.map()).isSameAs(map2.get());
          assertThat(distributedMap2.get("key1")).isNull();
          assertThat(distributedMap2.get("key2")).isEqualTo("value2");
        });
      }
    }
  }

  public static class HasManyDistributedMaps implements Serializable {

    private static final AtomicReference<Map<Object, Object>> map1 = new AtomicReference<>();
    private static final AtomicReference<Map<Object, Object>> map2 = new AtomicReference<>();
    private static final AtomicReference<Map<Object, Object>> map3 = new AtomicReference<>();

    @Rule
    public DistributedMap<Object, Object> distributedMap1 = new DistributedMap<>();
    @Rule
    public DistributedMap<Object, Object> distributedMap2 = new DistributedMap<>();
    @Rule
    public DistributedMap<Object, Object> distributedMap3 = new DistributedMap<>();

    @Before
    public void setUp() {
      getController().invoke(() -> {
        map1.set(distributedMap1.map());
        distributedMap1.put("key1", "value1");

        map2.set(distributedMap2.map());
        distributedMap2.put("key2", "value2");

        map3.set(distributedMap3.map());
        distributedMap3.put("key3", "value3");
      });
    }

    @Test
    public void manyDistributedMapsAreAccessibleInEveryVm() {
      for (VM vm : asList(getVM(0), getVM(1), getVM(2), getVM(3), getController())) {
        vm.invoke(() -> {
          assertThat(distributedMap1.map()).isSameAs(map1.get());
          assertThat(distributedMap1.get("key1")).isEqualTo("value1");
          assertThat(distributedMap1.get("key2")).isNull();
          assertThat(distributedMap1.get("key3")).isNull();

          assertThat(distributedMap2.map()).isSameAs(map2.get());
          assertThat(distributedMap2.get("key1")).isNull();
          assertThat(distributedMap2.get("key2")).isEqualTo("value2");
          assertThat(distributedMap2.get("key3")).isNull();

          assertThat(distributedMap3.map()).isSameAs(map3.get());
          assertThat(distributedMap3.get("key1")).isNull();
          assertThat(distributedMap3.get("key2")).isNull();
          assertThat(distributedMap3.get("key3")).isEqualTo("value3");
        });
      }
    }
  }
}
