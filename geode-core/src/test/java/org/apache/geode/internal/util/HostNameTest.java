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
package org.apache.geode.internal.util;

import static org.apache.geode.internal.lang.SystemUtils.*;
import static org.apache.geode.internal.util.HostName.*;
import static org.assertj.core.api.Assertions.*;

import java.io.IOException;

import org.apache.geode.test.junit.categories.UnitTest;

import org.junit.Rule;
import org.junit.Test;
import org.junit.contrib.java.lang.system.EnvironmentVariables;
import org.junit.contrib.java.lang.system.RestoreSystemProperties;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import junitparams.JUnitParamsRunner;
import junitparams.Parameters;

@Category(UnitTest.class)
@RunWith(JUnitParamsRunner.class)
public class HostNameTest {

  private static final String EXPECTED_HOSTNAME = "expected-hostname";
  private static final String UNKNOWN = "unknown";

  @Rule
  public final EnvironmentVariables env = new EnvironmentVariables();

  @Rule
  public final RestoreSystemProperties sysProps = new RestoreSystemProperties();

  @Test
  public void execHostNameShouldNeverReturnNull() throws IOException {
    String result = new HostName().execHostName();
    assertThat(result).isNotNull();
  }

  @Test
  @Parameters({MAC_OSX_NAME, LINUX_OS_NAME, SOLARIS_OS_NAME, WINDOWS_OS_NAME})
  public void shouldExecHostNameIfEnvValueNotAvailableOnOS(String osName) throws IOException {
    setHostNamePropertiesNull(osName);
    String result = new HostName().determineHostName();
    assertThat(result).isNotNull();
  }

  @Test
  @Parameters({MAC_OSX_NAME, LINUX_OS_NAME, SOLARIS_OS_NAME, WINDOWS_OS_NAME})
  public void shouldUseComputerNameIfAvailableOnOS(String osName) throws IOException {
    setHostNameProperties(osName);
    String result = new HostName().determineHostName();
    assertThat(result).isEqualTo(EXPECTED_HOSTNAME);
  }

  @Test
  @Parameters({MAC_OSX_NAME, LINUX_OS_NAME, SOLARIS_OS_NAME, WINDOWS_OS_NAME})
  public void shouldBeNullIfEnvValueNotAvailableOnOS(String osName) throws IOException {
    setHostNamePropertiesNull(osName);
    String result = new HostName().getHostNameFromEnv();
    assertThat(result).isEqualTo(null);
  }

  private void setHostNameProperties(String osName) {
    System.setProperty("os.name", osName);
    if (isWindows()) {
      this.env.set(COMPUTER_NAME_PROPERTY, EXPECTED_HOSTNAME);
      this.env.set(HOSTNAME_PROPERTY, null);
    } else {
      this.env.set(COMPUTER_NAME_PROPERTY, null);
      this.env.set(HOSTNAME_PROPERTY, EXPECTED_HOSTNAME);
    }

    assertThat(System.getProperty("os.name")).isEqualTo(osName);
    if (isWindows()) {
      assertThat(System.getenv(COMPUTER_NAME_PROPERTY)).isEqualTo(EXPECTED_HOSTNAME);
      assertThat(System.getenv(HOSTNAME_PROPERTY)).isNull();
    } else {
      assertThat(System.getenv(COMPUTER_NAME_PROPERTY)).isNull();
      assertThat(System.getenv(HOSTNAME_PROPERTY)).isEqualTo(EXPECTED_HOSTNAME);
    }
  }

  private void setHostNamePropertiesNull(String osName) {
    System.setProperty("os.name", osName);
    if (isWindows()) {
      this.env.set(COMPUTER_NAME_PROPERTY, null);
      this.env.set(HOSTNAME_PROPERTY, null);
    } else {
      this.env.set(COMPUTER_NAME_PROPERTY, null);
      this.env.set(HOSTNAME_PROPERTY, null);
    }

    assertThat(System.getProperty("os.name")).isEqualTo(osName);
    if (isWindows()) {
      assertThat(System.getenv(COMPUTER_NAME_PROPERTY)).isNull();
      assertThat(System.getenv(HOSTNAME_PROPERTY)).isNull();
    } else {
      assertThat(System.getenv(COMPUTER_NAME_PROPERTY)).isNull();
      assertThat(System.getenv(HOSTNAME_PROPERTY)).isNull();
    }
  }


}
