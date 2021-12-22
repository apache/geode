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

package org.apache.geode.cache.client.internal.pooling;

import static org.apache.geode.test.concurrency.Utilities.availableProcessors;
import static org.apache.geode.test.concurrency.Utilities.repeat;
import static org.assertj.core.api.Assertions.assertThat;

import java.util.concurrent.ExecutionException;

import org.junit.Test;
import org.junit.runner.RunWith;

import org.apache.geode.test.concurrency.ConcurrentTestRunner;
import org.apache.geode.test.concurrency.ParallelExecutor;

@RunWith(ConcurrentTestRunner.class)
public class ConnectionAccountingConcurrentTest {
  private final int count = availableProcessors() * 2;

  @Test
  public void tryPrefillStaysBelowOrAtMin(ParallelExecutor executor)
      throws ExecutionException, InterruptedException {
    final int min = count - 1;
    ConnectionAccounting accountant = new ConnectionAccounting(min, min + 4);

    executor.inParallel(() -> {
      accountant.tryPrefill();
      assertThat(accountant.getCount()).isGreaterThan(0).isLessThanOrEqualTo(min);
    }, count);

    executor.execute();

    assertThat(accountant.getCount()).isEqualTo(min);
  }

  @Test
  public void cancelTryPrefillStaysUnderMin(ParallelExecutor executor)
      throws ExecutionException, InterruptedException {
    final int min = count - 1;
    ConnectionAccounting accountant = new ConnectionAccounting(min, min + 4);

    executor.inParallel(() -> {
      if (accountant.tryPrefill()) {
        accountant.cancelTryPrefill();
        assertThat(accountant.getCount()).isGreaterThanOrEqualTo(0).isLessThanOrEqualTo(min);
      }
    }, count);

    executor.execute();

    assertThat(accountant.getCount()).isEqualTo(0);
  }

  @Test
  public void creates(ParallelExecutor executor) throws Exception {
    ConnectionAccounting accountant = new ConnectionAccounting(0, 1);

    executor.inParallel(() -> {
      accountant.create();
      assertThat(accountant.getCount()).isGreaterThan(0).isLessThanOrEqualTo(count);
    }, count);

    executor.execute();

    assertThat(accountant.getCount()).isEqualTo(count);
  }

  @Test
  public void tryCreateStaysWithinMax(ParallelExecutor executor) throws Exception {
    ConnectionAccounting accountant = new ConnectionAccounting(1, count);

    executor.inParallel(() -> {
      if (accountant.tryCreate()) {
        assertThat(accountant.getCount()).isGreaterThan(0).isLessThanOrEqualTo(count);
      }
    }, count + 1);

    executor.execute();

    assertThat(accountant.getCount()).isEqualTo(count);
  }

  @Test
  public void cancelTryCreateStaysWithinMax(ParallelExecutor executor) throws Exception {
    ConnectionAccounting accountant = new ConnectionAccounting(1, count);

    executor.inParallel(() -> {
      if (accountant.tryCreate()) {
        accountant.cancelTryCreate();
        assertThat(accountant.getCount()).isGreaterThanOrEqualTo(0).isLessThanOrEqualTo(count);
      }
    }, count + 1);

    executor.execute();

    assertThat(accountant.getCount()).isEqualTo(0);
  }

  @Test
  public void destroyAndIsUnderMinimum(ParallelExecutor executor) throws Exception {
    ConnectionAccounting accountant = new ConnectionAccounting(2, 4);
    repeat(accountant::create, count);

    executor.inParallel(() -> {
      if (accountant.destroyAndIsUnderMinimum(1)) {
        assertThat(accountant.getCount()).isGreaterThanOrEqualTo(0).isLessThan(2);
      } else {
        assertThat(accountant.getCount()).isGreaterThanOrEqualTo(0).isLessThan(count);
      }
    }, count);

    executor.execute();

    assertThat(accountant.getCount()).isEqualTo(0);
  }

  @Test
  public void tryDestroyNeverGoesBelowMax(ParallelExecutor executor) throws Exception {
    final int overfillMax = Math.max(count, 4);
    final int max = overfillMax / 2;
    ConnectionAccounting accountant = new ConnectionAccounting(1, max);
    repeat(accountant::create, overfillMax);

    executor.inParallel(() -> {
      if (accountant.tryDestroy()) {
        assertThat(accountant.getCount()).isGreaterThanOrEqualTo(max).isLessThan(overfillMax);
      }
    }, overfillMax + 1);

    executor.execute();

    assertThat(accountant.getCount()).isEqualTo(max);
  }

  @Test
  public void cancelTryDestroyStaysAboveMax(ParallelExecutor executor) throws Exception {
    final int overfillMax = Math.max(count, 4);
    final int max = overfillMax / 2;
    ConnectionAccounting accountant = new ConnectionAccounting(1, max);
    repeat(accountant::create, overfillMax);

    executor.inParallel(() -> {
      if (accountant.tryDestroy()) {
        accountant.cancelTryDestroy();
        assertThat(accountant.getCount()).isGreaterThanOrEqualTo(max)
            .isLessThanOrEqualTo(overfillMax);
      }
    }, max + 1);

    executor.execute();

    assertThat(accountant.getCount()).isEqualTo(overfillMax);
  }


  @Test
  public void mixItUp(ParallelExecutor executor) throws Exception {
    final int overfill = Math.max(count, 8);
    final int max = overfill / 4;
    final int overfillMax = overfill + max;
    ConnectionAccounting accountant = new ConnectionAccounting(1, max);
    repeat(accountant::create, overfill);

    executor.inParallel(() -> {
      if (accountant.tryDestroy()) {
        accountant.cancelTryDestroy();
      }
    }, max);

    executor.inParallel(() -> {
      if (accountant.tryCreate()) {
        accountant.cancelTryCreate();
      }
    }, max);

    executor.inParallel(accountant::create, max);

    executor.inParallel(() -> {
      if (accountant.tryCreate()) {
        accountant.destroyAndIsUnderMinimum(1);
      }
    }, max);

    executor.inParallel(() -> {
      if (accountant.tryPrefill()) {
        accountant.cancelTryPrefill();
      }
    }, max);

    executor.execute();

    assertThat(accountant.getCount()).isGreaterThanOrEqualTo(0).isLessThanOrEqualTo(overfillMax);
  }

}
