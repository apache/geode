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
package org.apache.geode.internal.serialization.filter;

import static java.util.Collections.emptySet;
import static org.apache.geode.util.internal.UncheckedUtils.uncheckedCast;
import static org.assertj.core.api.Assertions.assertThatCode;
import static org.assertj.core.api.Assumptions.assumeThat;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoInteractions;

import java.util.function.Consumer;

import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.contrib.java.lang.system.RestoreSystemProperties;

/**
 * Note: This is a manual test that requires running on an older version of Java 8.
 *
 * 1. Please install an OpenJDK version of Java 8 that is older than 8u121 (which is the version
 * that first introduced sun.misc.ObjectInputFilter for serialization filtering).
 *
 * 2. Configure Geode's gradle.properties so that testJVMVer=8 and testJVM=path_to_pre-8u121.
 *
 * 3. Change USING_PRE_8u121 to true so that the tests run.
 *
 * Note: Do NOT commit these changes.
 */
public class GlobalSerialFilterConfigurationOnOlderJdkIntegrationTest {

  /**
   * TODO: Update test to parse and compare Java update versions (u121) and automate this test.
   * These tests should only run on pre-8u121 versions which would typically only ever be installed
   * and used for testing on a developer's machine.
   */
  private static final boolean USING_PRE_8u121 = false;

  private SerializableObjectConfig config;
  private Consumer<String> infoLogger;
  private Consumer<String> warnLogger;
  private Consumer<String> errorLogger;

  @Rule
  public RestoreSystemProperties restoreSystemProperties = new RestoreSystemProperties();

  @Before
  public void useJdkOlderThan8u121() {
    assumeThat(USING_PRE_8u121).isTrue();

    config = mock(SerializableObjectConfig.class);
    infoLogger = uncheckedCast(mock(Consumer.class));
    warnLogger = uncheckedCast(mock(Consumer.class));
    errorLogger = uncheckedCast(mock(Consumer.class));
  }

  @Test
  public void doesNotThrow_whenEnableGlobalSerialFilterIsTrue_onLessThanJava8u121() {
    System.setProperty("geode.enableGlobalSerialFilter", "true");

    assertThatCode(() -> {
      FilterConfiguration filterConfiguration = new GlobalSerialFilterConfiguration(
          config,
          infoLogger,
          warnLogger,
          errorLogger,
          (pattern, sanctionedClasses) -> new ReflectiveFacadeGlobalSerialFilterFactory()
              .create("", emptySet()));

      filterConfiguration.configure();
    })
        .doesNotThrowAnyException();
  }

  @Test
  public void doesNotThrow_whenEnableGlobalSerialFilterIsFalse_onLessThanJava8u121() {
    System.clearProperty("geode.enableGlobalSerialFilter");

    assertThatCode(() -> {
      FilterConfiguration filterConfiguration = new GlobalSerialFilterConfiguration(config);

      filterConfiguration.configure();
    })
        .doesNotThrowAnyException();
  }

  @Test
  public void logsError_whenEnableGlobalSerialFilterIsTrue_onLessThanJava8u121() {
    System.setProperty("geode.enableGlobalSerialFilter", "true");

    FilterConfiguration filterConfiguration = new GlobalSerialFilterConfiguration(
        config,
        infoLogger,
        warnLogger,
        errorLogger,
        (pattern, sanctionedClasses) -> new ReflectiveFacadeGlobalSerialFilterFactory()
            .create("", emptySet()));

    filterConfiguration.configure();

    verifyNoInteractions(infoLogger);
    verifyNoInteractions(warnLogger);
    verify(errorLogger)
        .accept(
            "Geode was unable to configure a global serialization filter because ObjectInputFilter not found.");
  }

  @Test
  public void logsNothing_whenEnableGlobalSerialFilterIsFalse_onLessThanJava8u121() {
    System.clearProperty("geode.enableGlobalSerialFilter");

    FilterConfiguration filterConfiguration = new GlobalSerialFilterConfiguration(config);

    filterConfiguration.configure();

    verifyNoInteractions(infoLogger);
    verifyNoInteractions(warnLogger);
    verifyNoInteractions(errorLogger);
  }
}
