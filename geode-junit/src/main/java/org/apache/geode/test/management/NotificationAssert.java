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
package org.apache.geode.test.management;

import java.util.List;

import javax.management.Notification;

import org.assertj.core.api.Assertions;
import org.assertj.core.api.ListAssert;

import org.apache.geode.test.management.internal.AbstractNotificationAssert;
import org.apache.geode.test.management.internal.NotificationComparator;

/**
 * Assertion methods for {@code Notification}s.
 *
 * <p>
 * To create a new instance of this class, invoke
 * <code>{@link NotificationAssert#assertThat(Notification)}</code>.
 */
public class NotificationAssert extends AbstractNotificationAssert<NotificationAssert> {

  /**
   * Creates a new instance of {@code NotificationAssert} from a {@code Notification}.
   *
   * @param actual the actual value.
   * @return the created assertion object.
   */
  public static AbstractNotificationAssert<?> assertThat(Notification actual) {
    return new NotificationAssert(actual);
  }

  /**
   * Creates a new instance of {@code ListAssert} using {@code NotificationComparator}.
   *
   * @param actual the actual value.
   * @return the created assertion object.
   */
  public static ListAssert<Notification> assertThat(List<Notification> actual) {
    return Assertions.assertThat(actual)
        .usingElementComparator(new NotificationComparator());
  }

  protected NotificationAssert(Notification actual) {
    super(actual, NotificationAssert.class);
  }
}
