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

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.mock;

import org.junit.Rule;
import org.junit.Test;
import org.junit.contrib.java.lang.system.RestoreSystemProperties;

public class SystemPropertyGlobalSerialFilterConfigurationFactoryTest {

  @Rule
  public RestoreSystemProperties restoreSystemProperties = new RestoreSystemProperties();

  @Test
  public void createsConditionalGlobalSerialFilterConfiguration_whenEnableGlobalSerialFilter_isSet() {
    System.setProperty("geode.enableGlobalSerialFilter", "true");
    GlobalSerialFilterConfigurationFactory factory =
        new SystemPropertyGlobalSerialFilterConfigurationFactory();

    FilterConfiguration filterConfiguration = factory.create(mock(SerializableObjectConfig.class));

    assertThat(filterConfiguration).isInstanceOf(GlobalSerialFilterConfiguration.class);
  }

  @Test
  public void createsNoOp_whenEnableGlobalSerialFilter_isNotSet() {
    GlobalSerialFilterConfigurationFactory factory =
        new SystemPropertyGlobalSerialFilterConfigurationFactory();

    FilterConfiguration filterConfiguration = factory.create(mock(SerializableObjectConfig.class));

    assertThat(filterConfiguration)
        .isNotInstanceOf(GlobalSerialFilterConfiguration.class);
  }

  @Test
  public void createsNoOp_whenJdkSerialFilter_isSet() {
    System.setProperty("jdk.serialFilter", "*");
    GlobalSerialFilterConfigurationFactory factory =
        new SystemPropertyGlobalSerialFilterConfigurationFactory();

    FilterConfiguration filterConfiguration = factory.create(mock(SerializableObjectConfig.class));

    assertThat(filterConfiguration)
        .isNotInstanceOf(GlobalSerialFilterConfiguration.class);
  }

  @Test
  public void createsNoOp_whenJdkSerialFilter_andEnableGlobalSerialFilter_areBothSet() {
    System.setProperty("jdk.serialFilter", "*");
    System.setProperty("geode.enableGlobalSerialFilter", "true");
    GlobalSerialFilterConfigurationFactory factory =
        new SystemPropertyGlobalSerialFilterConfigurationFactory();

    FilterConfiguration filterConfiguration = factory.create(mock(SerializableObjectConfig.class));

    assertThat(filterConfiguration)
        .isNotInstanceOf(GlobalSerialFilterConfiguration.class);
  }
}
