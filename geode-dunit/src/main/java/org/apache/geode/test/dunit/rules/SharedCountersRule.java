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

import static org.apache.geode.test.dunit.VM.getAllVMs;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicInteger;

import org.apache.geode.test.dunit.VM;

/**
 * JUnit Rule that provides SharedCounters in DistributedTest VMs.
 *
 * <p>
 * {@code SharedCountersRule} can be used in DistributedTests as a {@code Rule}:
 *
 * <pre>
 * {@literal @}Rule
 * public DistributedRule distributedRule = new DistributedRule();
 *
 * {@literal @}Rule
 * public SharedCountersRule sharedCountersRule = new SharedCountersRule();
 *
 * {@literal @}Before
 * public void setUp() {
 *   sharedCountersRule.initialize("counter");
 * }
 *
 * {@literal @}Test
 * public void incrementCounterInEveryVm() {
 *   sharedCountersRule.initialize("counter");
 *   for (VM vm : getAllVMs()) {
 *     vm.invoke(() -> {
 *       sharedCountersRule.increment("counter");
 *     });
 *   }
 *   assertThat(sharedCountersRule.getTotal("counter")).isEqualTo(getVMCount());
 * }
 * </pre>
 *
 * <p>
 * {@link SharedCountersRule.Builder} can also be used to construct an instance with more options:
 *
 * <pre>
 * {@literal @}Rule
 * public DistributedRule distributedRule = new DistributedRule();
 *
 * {@literal @}Rule
 * public SharedCountersRule sharedCountersRule = SharedCountersRule.builder().withId("counter").build();
 *
 * {@literal @}Test
 * public void incrementCounterInEveryVm() {
 *   for (VM vm : getAllVMs()) {
 *     vm.invoke(() -> {
 *       sharedCountersRule.increment("counter");
 *     });
 *   }
 *   assertThat(sharedCountersRule.getTotal("counter")).isEqualTo(getVMCount());
 * }
 * </pre>
 *
 * <p>
 * For a more thorough example, please see
 * {@code org.apache.geode.cache.ReplicateCacheListenerDistributedTest} in the tests of geode-core.
 */
@SuppressWarnings("serial,unused")
public class SharedCountersRule extends AbstractDistributedRule {

  private static volatile Map<Serializable, AtomicInteger> counters;

  private final List<Serializable> idsToInitInBefore = new ArrayList<>();

  public static Builder builder() {
    return new Builder();
  }

  public SharedCountersRule() {
    this(new Builder());
  }

  SharedCountersRule(final Builder builder) {
    idsToInitInBefore.addAll(builder.ids);
  }

  @Override
  protected void before() {
    invoker().invokeInEveryVMAndController(() -> counters = new ConcurrentHashMap<>());
    for (Serializable id : idsToInitInBefore) {
      invoker().invokeInEveryVMAndController(() -> initialize(id));
    }
    idsToInitInBefore.clear();
  }

  @Override
  protected void after() {
    invoker().invokeInEveryVMAndController(() -> counters = null);
  }

  /**
   * Initialize an {@code AtomicInteger} with value of zero identified by {@code id} in every
   * {@code VM}.
   */
  public SharedCountersRule initialize(final Serializable id) {
    AtomicInteger value = new AtomicInteger();
    invoker().invokeInEveryVMAndController(() -> counters.putIfAbsent(id, value));
    return this;
  }

  /**
   * Returns the {@code AtomicInteger} identified by the specified {@code id}.
   */
  public AtomicInteger reference(final Serializable id) {
    return counters.get(id);
  }

  /**
   * Increments the {@code AtomicInteger} identified by the specified {@code id}.
   */
  public SharedCountersRule increment(final Serializable id) {
    counters.get(id).incrementAndGet();
    return this;
  }

  /**
   * Increments the {@code AtomicInteger} by the specified {@code delta} which may be a positive or
   * negative integer.
   */
  public SharedCountersRule increment(final Serializable id, final int delta) {
    counters.get(id).addAndGet(delta);
    return this;
  }

  /**
   * Increments the {@code AtomicInteger} identified by the specified {@code id}.
   */
  public SharedCountersRule decrement(final Serializable id) {
    counters.get(id).decrementAndGet();
    return this;
  }

  /**
   * Decrements the {@code AtomicInteger} by the specified {@code delta}.
   */
  public SharedCountersRule decrement(final Serializable id, final int delta) {
    counters.get(id).addAndGet(-delta);
    return this;
  }

  /**
   * Returns the total value of the {@code AtomicInteger} combined across every VM.
   */
  public int getTotal(final Serializable id) {
    int total = counters.get(id).get();
    for (VM vm : getAllVMs()) {
      total += vm.invoke(() -> counters.get(id).get());
    }
    return total;
  }

  /**
   * Returns the local value of the {@code AtomicInteger} identified by the specified {@code id}.
   */
  public int getLocal(final Serializable id) {
    return counters.get(id).get();
  }

  /**
   * Builds an instance of SharedCountersRule
   */
  public static class Builder {

    private final List<Serializable> ids = new ArrayList<>();

    public Builder() {
      // nothing
    }

    /**
     * Initialize specified id when {@code SharedCountersRule} is built.
     */
    public Builder withId(final Serializable id) {
      ids.add(id);
      return this;
    }

    public SharedCountersRule build() {
      return new SharedCountersRule(this);
    }
  }
}
