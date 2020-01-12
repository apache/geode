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
package org.apache.geode.internal.statistics;

import static org.assertj.core.api.Assertions.assertThat;

import org.junit.Rule;
import org.junit.Test;
import org.junit.contrib.java.lang.system.RestoreSystemProperties;

public class OsStatisticsProviderTest {

  @Rule
  public RestoreSystemProperties restoreSystemProperties = new RestoreSystemProperties();

  @Test
  public void givenLinuxOs_thenOsStatsAreSupported() {
    System.setProperty("os.name", "Linux");
    assertThat(OsStatisticsProvider.build().osStatsSupported()).isTrue();
  }

  @Test
  public void givenWindowsOs_thenOsStatsAreNotSupported() {
    System.setProperty("os.name", "Windows");
    assertThat(OsStatisticsProvider.build().osStatsSupported()).isFalse();
  }

  @Test
  public void givenMacOs_thenOsStatsAreNotSupported() {
    System.setProperty("os.name", "Mac OS X");
    assertThat(OsStatisticsProvider.build().osStatsSupported()).isFalse();
  }

  @Test
  public void givenSolarisOs_thenOsStatsAreNotSupported() {
    System.setProperty("os.name", "SunOS");
    assertThat(OsStatisticsProvider.build().osStatsSupported()).isFalse();
  }

  @Test
  public void givenUnknownOs_thenOsStatsAreNotSupported() {
    System.setProperty("os.name", "AnyOtherOS");
    assertThat(OsStatisticsProvider.build().osStatsSupported()).isFalse();
  }

}
