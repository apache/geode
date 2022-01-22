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

import static org.apache.commons.lang3.JavaVersion.JAVA_1_8;
import static org.apache.commons.lang3.SystemUtils.isJavaVersionAtMost;
import static org.assertj.core.api.Assertions.assertThatCode;
import static org.assertj.core.api.Assumptions.assumeThat;
import static org.mockito.Mockito.mock;

import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.contrib.java.lang.system.RestoreSystemProperties;

public class SystemPropertyGlobalSerialFilterConfigurationFactoryIntegrationTest {

  private SerializableObjectConfig serializableObjectConfig;

  @Rule
  public RestoreSystemProperties restoreSystemProperties = new RestoreSystemProperties();

  @Before
  public void setUp() {
    serializableObjectConfig = mock(SerializableObjectConfig.class);
  }

  @Test
  public void throwsUnsupportedOperationException_whenEnableGlobalSerialFilterIsTrue_onLessThanJava8u121() {
    assumeThat(isJavaVersionAtMost(JAVA_1_8)).isTrue();

    System.setProperty("geode.enableGlobalSerialFilter", "true");

    // TODO:KIRK: move to acceptance test to validate expected logging
    // TODO:KIRK: improve consistency between GlobalSerialFilter and ObjectInputFilter
    // TODO:KIRK: uplift logging severity to warn or error

    FilterConfiguration configuration = new SystemPropertyGlobalSerialFilterConfigurationFactory()
        .create(serializableObjectConfig);
    configuration.configure();
  }

  @Test
  public void doesNothing_whenEnableGlobalSerialFilterIsFalse_onLessThanJava8u121() {
    assumeThat(isJavaVersionAtMost(JAVA_1_8)).isTrue();

    System.clearProperty("geode.enableGlobalSerialFilter");

    assertThatCode(() -> {
      FilterConfiguration configuration = new SystemPropertyGlobalSerialFilterConfigurationFactory()
          .create(serializableObjectConfig);
      configuration.configure();
    })
        .doesNotThrowAnyException();
  }
}
