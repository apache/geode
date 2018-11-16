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
package org.apache.geode.internal.alerting;

import static org.apache.geode.internal.alerting.AlertingAction.execute;
import static org.apache.geode.internal.alerting.AlertingAction.isThreadAlerting;
import static org.assertj.core.api.Assertions.assertThat;
import static org.hamcrest.Matchers.is;

import org.junit.Rule;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.rules.ErrorCollector;

import org.apache.geode.test.junit.categories.AlertingTest;

/**
 * Unit tests for {@link AlertingAction}.
 */
@Category(AlertingTest.class)
public class AlertingActionTest {

  @Rule
  public ErrorCollector errorCollector = new ErrorCollector();

  @Test
  public void isThreadAlertingIsFalseByDefault() {
    assertThat(isThreadAlerting()).isFalse();
  }

  @Test
  public void isThreadAlertingIsTrueWhileExecuting() {
    execute(() -> errorCollector.checkThat(isThreadAlerting(), is(true)));
  }

  @Test
  public void isThreadAlertingIsFalseAfterExecuting() {
    execute(() -> System.out.println("hi"));

    assertThat(isThreadAlerting()).isFalse();
  }
}
