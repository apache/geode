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
package org.apache.geode.internal.cache.persistence;

import static java.util.Objects.requireNonNull;

import java.time.Duration;
import java.util.function.BooleanSupplier;

import org.apache.geode.CancelCriterion;

/**
 * Creates instances of {@link MembershipChangeListener} with validation of arguments.
 *
 * <p>
 * The warning delay must be less than the poll duration. If the values are identical then the
 * persistence advisors will fail to log warnings about missing persistent members.
 */
public class MembershipChangeListenerFactory {

  public static BooleanSupplier cancelCondition(InternalPersistenceAdvisor persistenceAdvisor,
      CancelCriterion cancelCriterion) {
    return () -> {
      persistenceAdvisor.checkInterruptedByShutdownAll();
      cancelCriterion.checkCancelInProgress(null);
      return persistenceAdvisor.isClosed();
    };
  }

  private Duration warningDelay;
  private Duration pollDuration;
  private BooleanSupplier cancelCondition;
  private Runnable warning;

  public MembershipChangeListenerFactory setWarningDelay(Duration warningDelay) {
    this.warningDelay = warningDelay;
    return this;
  }

  public MembershipChangeListenerFactory setPollDuration(Duration pollDuration) {
    this.pollDuration = pollDuration;
    return this;
  }

  public MembershipChangeListenerFactory setCancelCondition(BooleanSupplier cancelCondition) {
    this.cancelCondition = cancelCondition;
    return this;
  }

  public MembershipChangeListenerFactory setWarning(Runnable warning) {
    this.warning = warning;
    return this;
  }

  public MembershipChangeListener create() {
    return create(warningDelay, pollDuration, cancelCondition, warning);
  }

  public MembershipChangeListener create(Duration warningDelay, Duration pollDuration,
      BooleanSupplier cancelCondition, Runnable warning) {
    requireNonNull(warningDelay);
    requireNonNull(pollDuration);

    int diff = warningDelay.compareTo(pollDuration);
    if (diff >= 0) {
      throw new IllegalArgumentException("Warning delay \"" + warningDelay.getSeconds()
          + "\" seconds must be less than poll duration \"" + pollDuration.getSeconds()
          + "\" seconds.");
    }

    return new MembershipChangeListener(warningDelay, pollDuration, cancelCondition, warning);
  }
}
