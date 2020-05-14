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
package org.apache.geode.test.management.internal;

import java.util.Objects;

import javax.management.Notification;

import org.assertj.core.internal.StandardComparisonStrategy;
import org.assertj.core.util.Streams;

public class NotificationAttributeComparisonStrategy extends StandardComparisonStrategy {

  private static final NotificationAttributeComparisonStrategy INSTANCE =
      new NotificationAttributeComparisonStrategy();

  /**
   * Returns the singleton instance of this class.
   *
   * @return the singleton instance of this class.
   */
  public static NotificationAttributeComparisonStrategy instance() {
    return INSTANCE;
  }

  protected NotificationAttributeComparisonStrategy() {
    // empty
  }

  /**
   * Returns true if actual and other are equal based on attribute comparison, false otherwise.
   *
   * @param actual the object to compare to other
   * @param other the object to compare to actual
   * @return true if actual and other are equal based on attribute comparison, false otherwise.
   */
  @Override
  public boolean areEqual(Object actual, Object other) {
    // return Objects.areEqual(actual, other);
    if (!(actual instanceof Notification) || !(other instanceof Notification)) {
      throw new IllegalArgumentException(this + " only supports " + Notification.class.getName());
    }
    Notification actualNotification = (Notification) Objects.requireNonNull(actual);
    Notification otherNotification = (Notification) Objects.requireNonNull(other);
    return equals(actualNotification, otherNotification);
  }

  /**
   * Returns true if given {@link Iterable} contains given value based on attribute comparison,
   * false otherwise.<br>
   * If given {@link Iterable} is null, return false.
   *
   * @param iterable the {@link Iterable} to search value in
   * @param value the object to look for in given {@link Iterable}
   * @return true if given {@link Iterable} contains given value based on attribute comparison,
   *         false otherwise.
   */
  @Override
  public boolean iterableContains(Iterable<?> iterable, Object value) {
    if (iterable == null) {
      return false;
    }
    return Streams.stream(iterable).anyMatch(object -> areEqual(object, value));
  }

  @Override
  public String toString() {
    return getClass().getSimpleName() + "@" + Integer.toHexString(hashCode());
  }

  private boolean equals(Notification a, Notification b) {
    if (a == b) {
      return true;
    }
    if (a == null || b == null) {
      return false;
    }
    if (!Objects.equals(a.getMessage(), b.getMessage())) {
      return false;
    }
    if (!Objects.equals(a.getType(), b.getType())) {
      return false;
    }
    return Objects.equals(a.getUserData(), b.getUserData());
  }
}
