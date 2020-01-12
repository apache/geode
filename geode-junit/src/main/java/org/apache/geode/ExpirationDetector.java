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
package org.apache.geode;

import static org.apache.geode.test.awaitility.GeodeAwaitility.await;
import static org.assertj.core.api.Assertions.assertThat;

import java.util.concurrent.TimeUnit;

import org.apache.geode.internal.cache.ExpiryTask;
import org.apache.geode.internal.cache.ExpiryTask.ExpiryTaskListener;

/**
 * Used to detect that a particular ExpiryTask has expired.
 */
public class ExpirationDetector implements ExpiryTaskListener {

  private volatile boolean executed;
  private volatile boolean expired;
  private volatile boolean rescheduled;

  private final ExpiryTask expiryTask;

  public ExpirationDetector(ExpiryTask expiry) {
    assertThat(expiry).isNotNull();
    expiryTask = expiry;
  }

  @Override
  public void afterCancel(ExpiryTask expiryTask) {
    // nothing
  }

  @Override
  public void afterSchedule(ExpiryTask expiryTask) {
    // nothing
  }

  @Override
  public void afterReschedule(ExpiryTask expiryTask) {
    if (expiryTask == this.expiryTask) {
      if (!hasExpired()) {
        ExpiryTask.suspendExpiration();
      }
      rescheduled = true;
    }
  }

  @Override
  public void afterExpire(ExpiryTask expiryTask) {
    if (expiryTask == this.expiryTask) {
      expired = true;
    }
  }

  @Override
  public void afterTaskRan(ExpiryTask expiryTask) {
    if (expiryTask == this.expiryTask) {
      executed = true;
    }
  }

  public void awaitExecuted(long timeout, TimeUnit unit) {
    await().until(() -> executed);
  }

  public boolean wasRescheduled() {
    return rescheduled;
  }

  public boolean hasExpired() {
    return expired;
  }
}
