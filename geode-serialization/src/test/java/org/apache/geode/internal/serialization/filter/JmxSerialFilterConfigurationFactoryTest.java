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
import static org.apache.commons.lang3.JavaVersion.JAVA_9;
import static org.apache.commons.lang3.SystemUtils.isJavaVersionAtLeast;
import static org.apache.commons.lang3.SystemUtils.isJavaVersionAtMost;
import static org.apache.geode.internal.serialization.filter.SerialFilterAssertions.assertThatSerialFilterIsNull;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assumptions.assumeThat;

import java.lang.reflect.InvocationTargetException;

import org.junit.After;
import org.junit.Rule;
import org.junit.Test;
import org.junit.contrib.java.lang.system.RestoreSystemProperties;

public class JmxSerialFilterConfigurationFactoryTest {

  @Rule
  public RestoreSystemProperties restoreSystemProperties = new RestoreSystemProperties();

  @After
  public void serialFilterIsNull() throws InvocationTargetException, IllegalAccessException {
    assertThatSerialFilterIsNull();
  }

  @Test
  public void createsConditionalJmxSerialFilterConfiguration_onJava9orGreater() {
    assumeThat(isJavaVersionAtLeast(JAVA_9)).isTrue();
    JmxSerialFilterConfigurationFactory factory =
        new SystemPropertyJmxSerialFilterConfigurationFactory();

    FilterConfiguration filterConfiguration = factory.create();

    assertThat(filterConfiguration).isInstanceOf(JmxSerialFilterConfiguration.class);
  }

  @Test
  public void createsNoOp_onJava8() {
    assumeThat(isJavaVersionAtMost(JAVA_1_8)).isTrue();
    JmxSerialFilterConfigurationFactory factory =
        new SystemPropertyJmxSerialFilterConfigurationFactory();

    FilterConfiguration filterConfiguration = factory.create();

    assertThat(filterConfiguration).isNotInstanceOf(JmxSerialFilterConfiguration.class);
  }

  @Test
  public void createsNoOp_whenJmxSerialFilter_isSet() {
    System.setProperty("jmx.remote.rmi.server.serial.filter.pattern", "*");
    JmxSerialFilterConfigurationFactory factory =
        new SystemPropertyJmxSerialFilterConfigurationFactory();

    FilterConfiguration filterConfiguration = factory.create();

    assertThat(filterConfiguration).isNotInstanceOf(JmxSerialFilterConfiguration.class);
  }
}
