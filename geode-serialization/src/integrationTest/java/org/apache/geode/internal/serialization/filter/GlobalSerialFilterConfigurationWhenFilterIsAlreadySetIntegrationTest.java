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
import static org.assertj.core.api.Assertions.catchThrowable;
import static org.mockito.Mockito.mock;

import java.lang.reflect.InvocationTargetException;

import org.junit.Before;
import org.junit.Test;

public class GlobalSerialFilterConfigurationWhenFilterIsAlreadySetIntegrationTest {

  @Before
  public void filterIsAlreadySet() throws InvocationTargetException, IllegalAccessException {
    ObjectInputFilterApiFactory factory = new ReflectiveObjectInputFilterApiFactory();
    ObjectInputFilterApi api = factory.createObjectInputFilterApi();
    Object filter = api.createFilter("*");
    api.setSerialFilter(filter);
  }

  @Test
  public void throwsObjectInputFilterException_whenFilterIsAlreadySet() {
    FilterConfiguration configuration =
        new GlobalSerialFilterConfiguration(mock(SerializableObjectConfig.class));

    Throwable thrown = catchThrowable(() -> {
      configuration.configure();
    });

    assertThat(thrown)
        .isInstanceOf(UnableToSetSerialFilterException.class)
        .hasMessage("Unable to configure a global serialization filter.")
        .hasCauseInstanceOf(InvocationTargetException.class)
        .hasRootCauseInstanceOf(IllegalStateException.class)
        .hasRootCauseMessage("Serial filter can only be set once");
  }

}
