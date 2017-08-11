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

import static org.apache.geode.internal.process.ProcessUtils.identifyPid;
import static org.apache.geode.internal.process.ProcessUtils.isProcessAlive;
import static org.apache.geode.internal.process.lang.AvailablePid.DEFAULT_TIMEOUT_MILLIS;
import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.anyInt;
import static org.mockito.Mockito.atLeastOnce;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.verify;

import java.util.Arrays;
import java.util.Random;
import java.util.Set;
import java.util.stream.Collectors;

import org.junit.Before;
import org.junit.Test;
import org.junit.experimental.categories.Category;

import org.apache.geode.test.junit.categories.UnitTest;

/**
 * Unit tests for {@link AvailablePid}.
 */
@Category(UnitTest.class)
public class AvailablePidTest {

  private AvailablePid availablePid;

  @Before
  public void before() throws Exception {
    availablePid = new AvailablePid();
  }

  @Test
  public void lowerBoundShouldBeLegalPid() throws Exception {
    assertThat(isProcessAlive(AvailablePid.LOWER_BOUND)).isIn(true, false);
  }

  @Test
  public void upperBoundShouldBeLegalPid() throws Exception {
    assertThat(isProcessAlive(AvailablePid.UPPER_BOUND)).isIn(true, false);
  }

  @Test(timeout = DEFAULT_TIMEOUT_MILLIS)
  public void findAvailablePidShouldNotReturnLocalPid() throws Exception {
    int pid = availablePid.findAvailablePid();

    assertThat(pid).isNotEqualTo(identifyPid());
  }

  @Test(timeout = DEFAULT_TIMEOUT_MILLIS)
  public void findAvailablePidShouldNotReturnLivePid() throws Exception {
    int pid = availablePid.findAvailablePid();

    assertThat(isProcessAlive(pid)).isFalse();
  }

  @Test(timeout = DEFAULT_TIMEOUT_MILLIS)
  public void findAvailablePidShouldReturnRandomPid() throws Exception {
    Random random = spy(new Random());
    availablePid = new AvailablePid(random, DEFAULT_TIMEOUT_MILLIS);

    availablePid.findAvailablePid();

    verify(random, atLeastOnce()).nextInt(anyInt());
  }

  @Test(timeout = DEFAULT_TIMEOUT_MILLIS)
  public void findAvailablePidsShouldReturnSpecifiedNumberOfPids() throws Exception {
    assertThat(availablePid.findAvailablePids(1)).hasSize(1);
    assertThat(availablePid.findAvailablePids(2)).hasSize(2);
    assertThat(availablePid.findAvailablePids(3)).hasSize(3);
    assertThat(availablePid.findAvailablePids(5)).hasSize(5);
    assertThat(availablePid.findAvailablePids(8)).hasSize(8);
  }

  @Test(timeout = DEFAULT_TIMEOUT_MILLIS)
  public void findAvailablePidsShouldReturnNoDuplicatedPids() throws Exception {
    assertThatNoPidIsDuplicated(availablePid.findAvailablePids(1));
    assertThatNoPidIsDuplicated(availablePid.findAvailablePids(2));
    assertThatNoPidIsDuplicated(availablePid.findAvailablePids(3));
    assertThatNoPidIsDuplicated(availablePid.findAvailablePids(5));
    assertThatNoPidIsDuplicated(availablePid.findAvailablePids(8));
  }

  private void assertThatNoPidIsDuplicated(int[] pids) {
    Set<Integer> pidSet = Arrays.stream(pids).boxed().collect(Collectors.toSet());
    assertThat(pidSet).hasSize(pids.length);
  }
}
