/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.geode.distributed;

import static org.apache.commons.lang3.JavaVersion.JAVA_1_8;
import static org.apache.commons.lang3.SystemUtils.isJavaVersionAtMost;
import static org.apache.geode.distributed.ConfigurationProperties.VALIDATE_SERIALIZABLE_OBJECTS;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assumptions.assumeThat;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import org.junit.Rule;
import org.junit.Test;
import org.junit.contrib.java.lang.system.RestoreSystemProperties;

import org.apache.geode.distributed.internal.InternalLocator;

/**
 * Unit tests for {@link LocatorLauncher}.
 *
 * @since GemFire 7.0
 */
public class LocatorLauncherTest {

  @Rule
  public RestoreSystemProperties restoreSystemProperties = new RestoreSystemProperties();

  @Test
  public void canBeMocked() {
    LocatorLauncher mockLocatorLauncher = mock(LocatorLauncher.class);
    InternalLocator mockInternalLocator = mock(InternalLocator.class);

    when(mockLocatorLauncher.getLocator()).thenReturn(mockInternalLocator);
    when(mockLocatorLauncher.getId()).thenReturn("ID");

    assertThat(mockLocatorLauncher.getLocator()).isSameAs(mockInternalLocator);
    assertThat(mockLocatorLauncher.getId()).isEqualTo("ID");
  }

  @Test // TODO:KIRK: test that LocatorLauncher sets filter on Java 8
  public void configuresJdkSerialFilter() {
    assumeThat(isJavaVersionAtMost(JAVA_1_8)).isTrue();
    LocatorLauncher locatorLauncher = new LocatorLauncher.Builder().build();
    locatorLauncher.start();
    assertThat(
        locatorLauncher.getDistributedSystemProperties().getProperty(VALIDATE_SERIALIZABLE_OBJECTS))
            .isEqualTo("true");

    // GeodePropertiesFilterConfigurationuration.configureJdkSerialFilter(new Properties());
    //
    // assertThat(System.getProperty("jdk.serialFilter"))
    // .isEqualTo(new SanctionedSerializablesFilterPattern().pattern());
  }
}
