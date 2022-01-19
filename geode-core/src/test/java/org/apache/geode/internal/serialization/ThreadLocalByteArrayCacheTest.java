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
package org.apache.geode.internal.serialization;

import static org.assertj.core.api.Assertions.assertThat;

import java.util.concurrent.atomic.AtomicReference;

import org.junit.Test;


public class ThreadLocalByteArrayCacheTest {
  private final ThreadLocalByteArrayCache instance = new ThreadLocalByteArrayCache(2);

  @Test
  public void emptyArrayReturned() {
    byte[] byteArray = instance.get(0);

    assertThat(byteArray).hasSize(0);
  }

  @Test
  public void largerRequestCreatesNewArray() {
    byte[] byteArrayZero = instance.get(0);
    byte[] byteArrayOne = instance.get(1);

    assertThat(byteArrayZero).hasSize(0);
    assertThat(byteArrayOne).hasSize(1);
  }

  @Test
  public void smallerRequestCreatesReturnsPreviousArray() {
    byte[] byteArrayOne = instance.get(1);
    byte[] byteArrayZero = instance.get(0);

    assertThat(byteArrayOne).hasSize(1);
    assertThat(byteArrayZero).isSameAs(byteArrayOne);
  }

  @Test
  public void requestsLargerThanMaximumAreNotCached() {
    byte[] byteArrayLarge = instance.get(100);
    byte[] byteArrayZero = instance.get(0);

    assertThat(byteArrayLarge).hasSize(100);
    assertThat(byteArrayZero).isNotSameAs(byteArrayLarge);

  }

  @Test
  public void equalRequestCreatesReturnsPreviousArray() {
    byte[] byteArrayFirst = instance.get(1);
    byte[] byteArraySecond = instance.get(1);

    assertThat(byteArrayFirst).hasSize(1);
    assertThat(byteArraySecond).isSameAs(byteArrayFirst);
  }

  @Test
  public void threadsGetDifferentByteArrays() throws InterruptedException {
    byte[] byteArrayZero = instance.get(0);
    final AtomicReference<byte[]> byteArrayHolder = new AtomicReference<>();
    Thread thread = new Thread(() -> {
      byteArrayHolder.set(instance.get(0));
    });
    thread.start();
    thread.join();

    assertThat(byteArrayZero).hasSize(0);
    assertThat(byteArrayHolder.get()).hasSize(0);
    assertThat(byteArrayHolder.get()).isNotSameAs(byteArrayZero);

  }
}
