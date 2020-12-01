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
package org.apache.geode.test.dunit.rules;

import static java.lang.System.identityHashCode;
import static org.apache.geode.test.dunit.VM.DEFAULT_VM_COUNT;
import static org.apache.geode.test.dunit.VM.getController;
import static org.apache.geode.util.internal.UncheckedUtils.uncheckedCast;

import java.util.Collection;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.atomic.AtomicReference;

import org.apache.geode.test.dunit.VM;

/**
 * JUnit Rule that provides a shared distributed map in DistributedTest VMs.
 *
 * <p>
 * {@code DistributedMap} can be used in DistributedTests as a {@code Rule}:
 *
 * <pre>
 * {@literal @}Rule
 * public DistributedRule distributedRule = new DistributedRule();
 *
 * {@literal @}Rule
 * public DistributedMap&lt;String, String&gt; distributedMap = new DistributedMap&lt;&gt;();
 *
 * {@literal @}Test
 * public void accessDistributedMapInEveryVm() {
 *   distributedMap.put("key", "value);
 *
 *   for (VM vm : getAllVMs()) {
 *     vm.invoke(() -> {
 *       assertThat(distributedMap.get("key")).isEqualTo("value");
 *     });
 *   }
 * }
 * </pre>
 */
@SuppressWarnings("serial,unused")
public class DistributedMap<K, V> extends AbstractDistributedRule implements Map<K, V> {

  private static final AtomicReference<Map<Integer, Map<?, ?>>> MAPS =
      new AtomicReference<>(new HashMap<>());

  private final AtomicReference<VM> controller = new AtomicReference<>();
  private final Map<K, V> initialEntries = new HashMap<>();
  private final int identity;

  public static Builder builder() {
    return new Builder();
  }

  public DistributedMap() {
    this(new Builder<>());
  }

  public DistributedMap(int vmCount) {
    this(new Builder<K, V>().vmCount(vmCount));
  }

  private DistributedMap(Builder<K, V> builder) {
    this(builder.vmCount, builder.initialEntries);
  }

  private DistributedMap(int vmCount, Map<K, V> initialEntries) {
    super(vmCount);
    this.initialEntries.putAll(initialEntries);
    identity = identityHashCode(this);
  }

  @Override
  protected void before() {
    controller.set(getController());

    MAPS.get().put(identity, new HashMap<K, V>());
    map().clear();
    map().putAll(initialEntries);
  }

  @Override
  protected void after() {
    for (Map<?, ?> map : MAPS.get().values()) {
      map.clear();
    }
    MAPS.get().clear();
  }

  @Override
  public int size() {
    return controller().invoke(() -> map().size());
  }

  @Override
  public boolean isEmpty() {
    return controller().invoke(() -> map().isEmpty());
  }

  @Override
  public boolean containsKey(Object key) {
    return controller().invoke(() -> map().containsKey(key));
  }

  @Override
  public boolean containsValue(Object value) {
    return controller().invoke(() -> map().containsValue(value));
  }

  @Override
  public V get(Object key) {
    return controller().invoke(() -> map().get(key));
  }

  @Override
  public V put(K key, V value) {
    return controller().invoke(() -> map().put(key, value));
  }

  @Override
  public V remove(Object key) {
    return controller().invoke(() -> map().remove(key));
  }

  @Override
  public void putAll(Map<? extends K, ? extends V> m) {
    controller().invoke(() -> map().putAll(m));
  }

  @Override
  public void clear() {
    controller().invoke(() -> map().clear());
  }

  @Override
  public Set<K> keySet() {
    return controller().invoke(() -> map().keySet());
  }

  @Override
  public Collection<V> values() {
    return controller().invoke(() -> map().values());
  }

  @Override
  public Set<Entry<K, V>> entrySet() {
    return controller().invoke(() -> map().entrySet());
  }

  @Override
  public boolean equals(Object obj) {
    return controller().invoke(() -> map().equals(obj));
  }

  @Override
  public int hashCode() {
    return controller().invoke(() -> map().hashCode());
  }

  @Override
  public String toString() {
    return controller().invoke(() -> map().toString());
  }

  public Map<K, V> map() {
    return uncheckedCast(MAPS.get().get(identity));
  }

  private VM controller() {
    VM vm = controller.get();
    if (vm == null) {
      String message = getClass().getSimpleName() + " has not been initialized by JUnit";
      throw new IllegalStateException(message);
    }
    return vm;
  }

  /**
   * Builds an instance of {@code DistributedMap}.
   */
  public static class Builder<K, V> {

    private final Map<K, V> initialEntries = new HashMap<>();
    private int vmCount = DEFAULT_VM_COUNT;

    public Builder<K, V> vmCount(int vmCount) {
      this.vmCount = vmCount;
      return this;
    }

    /**
     * Initialize with specified entry when {@code DistributedMap} is built.
     */
    public Builder<K, V> put(K key, V value) {
      initialEntries.put(key, value);
      return this;
    }

    /**
     * Initialize with specified entries when {@code DistributedMap} is built.
     */
    public Builder<K, V> putAll(Map<? extends K, ? extends V> m) {
      initialEntries.putAll(m);
      return this;
    }

    /**
     * Build an instance of {@code DistributedMap}.
     */
    public DistributedMap<K, V> build() {
      return new DistributedMap<>(this);
    }
  }
}
