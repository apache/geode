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
 *
 */

package org.apache.geode.management.internal.operation;

import static org.assertj.core.api.Assertions.assertThat;

import java.util.Date;
import java.util.concurrent.atomic.AtomicLong;

import org.junit.Test;
import org.junit.runner.RunWith;

import org.apache.geode.test.concurrency.ConcurrentTestRunner;
import org.apache.geode.test.concurrency.ParallelExecutor;

@RunWith(ConcurrentTestRunner.class)
public class OperationStateConcurrentTest {
  private static final int PARALLEL_COUNT = 100;
  public static final int CALLS_PER_TASK = 20;

  private static class DateThrowable extends Throwable {
    private final Date date;

    public DateThrowable(Date date) {
      this.date = date;
    }

    public Date getDate() {
      return date;
    }
  }

  private static AtomicLong dateLong = new AtomicLong();

  private static void setOperationEnd(OperationState<?, ?> operationState) {
    Date date = new Date(dateLong.incrementAndGet());
    operationState.setOperationEnd(date, null, new DateThrowable(date));
  }

  @Test
  public void createCopyAndSetOperationEndThreadSafe(ParallelExecutor executor)
      throws Exception {
    final OperationState<?, ?> operationState = new OperationState<>(null, null, null);
    setOperationEnd(operationState);

    executor.inParallel(() -> {
      for (int i = 0; i < CALLS_PER_TASK; i++) {
        setOperationEnd(operationState);
      }
    }, PARALLEL_COUNT);
    executor.inParallel(() -> {
      for (int i = 0; i < CALLS_PER_TASK; i++) {
        OperationState<?, ?> copy = operationState.createCopy();
        assertThat(copy.getOperationEnd())
            .isSameAs(((DateThrowable) copy.getThrowable()).getDate());
      }
    }, 1);
    executor.execute();
  }
}
