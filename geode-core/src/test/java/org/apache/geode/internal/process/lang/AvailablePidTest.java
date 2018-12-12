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
package org.apache.geode.internal.process.lang;

import static java.util.concurrent.TimeUnit.SECONDS;
import static org.apache.geode.internal.process.ProcessUtils.identifyPid;
import static org.apache.geode.internal.process.ProcessUtils.isProcessAlive;
import static org.apache.geode.internal.process.lang.AvailablePid.DEFAULT_LOWER_BOUND;
import static org.apache.geode.internal.process.lang.AvailablePid.DEFAULT_UPPER_BOUND;
import static org.apache.geode.test.awaitility.GeodeAwaitility.await;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.ArgumentMatchers.anyInt;
import static org.mockito.Mockito.atLeastOnce;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.verify;

import java.util.Arrays;
import java.util.Random;
import java.util.Set;
import java.util.stream.Collectors;

import com.google.common.base.Stopwatch;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.Timeout;


/**
 * Unit tests for {@link AvailablePid}.
 */
public class AvailablePidTest {

  private AvailablePid availablePid;

  @Rule
  public Timeout timeout = Timeout.builder().withTimeout(20, SECONDS).build();

  @Before
  public void before() throws Exception {
    availablePid = new AvailablePid();
  }

  @Test
  public void lowerBoundShouldBeLegalPid() throws Exception {
    assertThat(isProcessAlive(DEFAULT_LOWER_BOUND)).isIn(true, false);
  }

  @Test
  public void upperBoundShouldBeLegalPid() throws Exception {
    assertThat(isProcessAlive(DEFAULT_UPPER_BOUND)).isIn(true, false);
  }

  @Test
  public void findAvailablePidShouldNotReturnLocalPid() throws Exception {
    int pid = availablePid.findAvailablePid();

    assertThat(pid).isNotEqualTo(identifyPid());
  }

  @Test
  public void findAvailablePidShouldNotReturnLivePid() throws Exception {
    int pid = availablePid.findAvailablePid();

    assertThat(isProcessAlive(pid)).isFalse();
  }

  @Test
  public void findAvailablePidShouldUseRandom() throws Exception {
    Random random = spy(new Random());
    availablePid = new AvailablePid(random);

    availablePid.findAvailablePid();

    verify(random, atLeastOnce()).nextInt(anyInt());
  }

  @Test
  public void findAvailablePidsShouldReturnSpecifiedNumberOfPids() throws Exception {
    assertThat(availablePid.findAvailablePids(1)).hasSize(1);
    assertThat(availablePid.findAvailablePids(2)).hasSize(2);
    assertThat(availablePid.findAvailablePids(3)).hasSize(3);
    assertThat(availablePid.findAvailablePids(5)).hasSize(5);
  }

  @Test
  public void findAvailablePidsShouldReturnNoDuplicatedPids() throws Exception {
    assertThatNoPidIsDuplicated(availablePid.findAvailablePids(1));
    assertThatNoPidIsDuplicated(availablePid.findAvailablePids(2));
    assertThatNoPidIsDuplicated(availablePid.findAvailablePids(3));
    assertThatNoPidIsDuplicated(availablePid.findAvailablePids(5));
  }

  @Test
  public void findAvailablePidShouldReturnGreaterThanOrEqualToLowerBound() throws Exception {
    availablePid = new AvailablePid(new AvailablePid.Bounds(1, 10));
    Stopwatch stopwatch = Stopwatch.createStarted();

    do {
      assertThat(availablePid.findAvailablePid()).isGreaterThanOrEqualTo(1);
    } while (stopwatch.elapsed(SECONDS) < 2);
  }

  @Test
  public void findAvailablePidShouldReturnLessThanOrEqualToUpperBound() throws Exception {
    availablePid = new AvailablePid(new AvailablePid.Bounds(1, 10));
    Stopwatch stopwatch = Stopwatch.createStarted();

    do {
      assertThat(availablePid.findAvailablePid()).isLessThanOrEqualTo(10);
    } while (stopwatch.elapsed(SECONDS) < 2);
  }

  @Test
  public void randomLowerBoundIsInclusive() throws Exception {
    availablePid = new AvailablePid(new AvailablePid.Bounds(1, 3));

    await().untilAsserted(() -> assertThat(availablePid.random()).isEqualTo(1));
  }

  @Test
  public void randomUpperBoundIsInclusive() throws Exception {
    availablePid = new AvailablePid(new AvailablePid.Bounds(1, 3));

    await().untilAsserted(() -> assertThat(availablePid.random()).isEqualTo(3));
  }

  @Test
  public void lowerBoundMustBeGreaterThanZero() throws Exception {
    assertThatThrownBy(() -> new AvailablePid(new AvailablePid.Bounds(0, 1)))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessage("lowerBound must be greater than '0'");
  }

  @Test
  public void upperBoundMustBeGreaterThanLowerBound() throws Exception {
    assertThatThrownBy(() -> new AvailablePid(new AvailablePid.Bounds(1, 1)))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessage("upperBound must be greater than lowerBound '1'");
  }

  private void assertThatNoPidIsDuplicated(final int[] pids) {
    Set<Integer> pidSet = Arrays.stream(pids).boxed().collect(Collectors.toSet());
    assertThat(pidSet).hasSize(pids.length);
  }
}
