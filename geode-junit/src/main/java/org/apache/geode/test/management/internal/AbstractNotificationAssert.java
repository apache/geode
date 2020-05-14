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

import static org.apache.geode.test.management.error.ShouldHaveNoUserData.shouldHaveNoUserData;
import static org.apache.geode.test.management.error.ShouldHaveUserData.shouldHaveUserData;

import javax.management.Notification;

import org.assertj.core.api.AbstractAssert;
import org.assertj.core.api.AssertionInfo;
import org.assertj.core.internal.Comparables;
import org.assertj.core.internal.Failures;
import org.assertj.core.internal.Objects;
import org.assertj.core.internal.Strings;

public abstract class AbstractNotificationAssert<SELF extends AbstractNotificationAssert<SELF>>
    extends AbstractAssert<SELF, Notification> {

  protected Objects objects = Objects.instance();
  protected Strings strings = Strings.instance();
  protected Comparables comparables = new Comparables();
  protected Failures failures = Failures.instance();

  public AbstractNotificationAssert(Notification actual, Class<?> selfType) {
    super(actual, selfType);
  }

  public SELF hasType(String type) {
    assertHasType(info, actual, type);
    return myself;
  }

  public SELF hasSource(Object source) {
    assertHasSource(info, actual, source);
    return myself;
  }

  public SELF hasUserData(Object userData) {
    assertHasUserData(info, actual, userData);
    return myself;
  }

  public SELF hasNoUserData() {
    assertHasNoUserData(info, actual);
    return myself;
  }

  private void assertHasType(AssertionInfo info, Notification actual, String type) {
    doCommonChecksForStringValue(info, actual, type);
    objects.assertEqual(info, actual.getType(), type);
  }

  private void assertHasSource(AssertionInfo info, Notification actual, Object source) {
    doCommonChecksForObjectValue(info, actual, source);
    objects.assertEqual(info, actual.getSource(), source);
  }

  private void assertHasUserData(AssertionInfo info, Notification actual, Object userData) {
    doCommonChecksForObjectValue(info, actual, userData);
    Object actualUserData = actual.getUserData();
    if (actualUserData == userData)
      return;
    if (null == userData) {
      assertHasNoUserData(info, actual);
      return;
    }
    if (actualUserData == null) {
      throw failures.failure(info, shouldHaveUserData(actual, userData));
    }
    // throw failures.failure(info, shouldHaveMessage(actual, expectedMessage),
    // actual.getUserData(), expectedMessage);
  }

  private void assertHasNoUserData(AssertionInfo info, Notification actual) {
    objects.assertNotNull(info, actual);
    Object actualUserData = actual.getUserData();
    if (actualUserData == null) {
      return;
    }
    throw failures.failure(info, shouldHaveNoUserData(actual));
  }

  private void doCommonChecksForObjectValue(AssertionInfo info, Notification actual, Object value) {
    objects.assertNotNull(info, actual);
    objects.assertNotNull(info, value);
  }

  private void doCommonChecksForStringValue(AssertionInfo info, Notification actual, String value) {
    objects.assertNotNull(info, actual);
    objects.assertNotNull(info, value);
    strings.assertNotEmpty(info, value);
  }
}
