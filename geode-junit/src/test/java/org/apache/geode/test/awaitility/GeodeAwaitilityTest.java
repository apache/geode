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
package org.apache.geode.test.awaitility;

import static java.lang.String.valueOf;
import static java.time.Duration.ofMinutes;
import static org.apache.geode.test.awaitility.GeodeAwaitility.TIMEOUT_SECONDS_PROPERTY;
import static org.apache.geode.test.awaitility.GeodeAwaitility.await;
import static org.apache.geode.test.awaitility.GeodeAwaitility.getTimeout;
import static org.apache.geode.test.awaitility.GeodeAwaitility.toTimeDuration;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.catchThrowable;
import static org.awaitility.Duration.FIVE_MINUTES;
import static org.awaitility.Duration.ONE_MINUTE;
import static org.awaitility.Duration.ONE_SECOND;

import java.time.Duration;

import org.awaitility.core.ConditionFactory;
import org.awaitility.core.ConditionTimeoutException;
import org.junit.Rule;
import org.junit.Test;
import org.junit.contrib.java.lang.system.RestoreSystemProperties;
import org.junit.rules.TestName;

/**
 * Unit tests for {@link GeodeAwaitility}.
 */
public class GeodeAwaitilityTest {

  @Rule
  public RestoreSystemProperties restoreSystemProperties = new RestoreSystemProperties();

  @Rule
  public TestName testName = new TestName();

  @Test
  public void getTimeoutIsFiveMinutesByDefault() {
    assertThat(getTimeout()).isEqualTo(FIVE_MINUTES);
  }

  @Test
  public void getTimeoutIsOverriddenWithSystemProperty() {
    System.setProperty(TIMEOUT_SECONDS_PROPERTY, valueOf(ONE_MINUTE.getValue()));

    assertThat(getTimeout()).isEqualTo(ONE_MINUTE);
  }

  @Test
  public void awaitReturnsConditionFactory() {
    assertThat(await()).isNotNull().isInstanceOf(ConditionFactory.class);
  }

  @Test
  public void awaitWithAliasReturnsConditionFactory() {
    assertThat(await(testName.getMethodName())).isNotNull().isInstanceOf(ConditionFactory.class);
  }

  @Test
  public void awaitWithAliasActuallyUsesAlias() {
    System.setProperty(TIMEOUT_SECONDS_PROPERTY, valueOf(ONE_SECOND.getValue()));
    String alias = testName.getMethodName();

    Throwable thrown = catchThrowable(() -> await(alias).until(() -> false));

    assertThat(thrown).isInstanceOf(ConditionTimeoutException.class).hasMessageContaining(alias);
  }

  @Test
  public void toTimeDurationConverts() {
    assertThat(ONE_MINUTE).isNotEqualTo(ofMinutes(1));

    assertThat(toTimeDuration(ONE_MINUTE)).isInstanceOf(Duration.class).isEqualTo(ofMinutes(1));
  }
}
