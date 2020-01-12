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
package org.apache.geode.internal.util.concurrent;

import static org.junit.Assert.fail;

import java.util.concurrent.CancellationException;
import java.util.concurrent.TimeUnit;

import org.junit.Test;

import org.apache.geode.CancelCriterion;
import org.apache.geode.distributed.DistributedSystemDisconnectedException;

/**
 * Unit tests for {@link FutureResult}.
 *
 */
public class FutureResultJUnitTest {

  @Test
  public void cancelledFutureResultThrowsExceptionOnGet() throws Exception {
    TestCancelCriterion cancelCriterion = new TestCancelCriterion();
    FutureResult futureResult = new FutureResult(cancelCriterion);
    cancelCriterion.cancelReason = "cancelling for test";
    try {
      futureResult.get(1000, TimeUnit.MILLISECONDS);
      fail("expected a DistributedSystemDisconnectedException to be thrown");
    } catch (DistributedSystemDisconnectedException e) {
      // expected - thrown by the StoppableCountDownLatch in the FutureResult
    }
    cancelCriterion.cancelReason = "";
    futureResult = new FutureResult(cancelCriterion);
    futureResult.cancel(false);
    try {
      futureResult.get(1000, TimeUnit.MILLISECONDS);
      fail("expected a CancellationException to be thrown");
    } catch (CancellationException e) {
      // expected - thrown by the FutureResult
    }
  }

  static class TestCancelCriterion extends CancelCriterion {
    String cancelReason = "";

    @Override
    public String cancelInProgress() {
      return cancelReason;
    }

    @Override
    public RuntimeException generateCancelledException(Throwable e) {
      String reason = cancelInProgress();
      if (reason == null) {
        return null;
      } else {
        if (e == null) {
          return new DistributedSystemDisconnectedException(reason);
        } else {
          return new DistributedSystemDisconnectedException(reason, e);
        }
      }
    }
  }
}
