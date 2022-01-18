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
import static org.apache.commons.lang3.JavaVersion.JAVA_1_8;
import static org.apache.commons.lang3.JavaVersion.JAVA_9;
import static org.apache.commons.lang3.SystemUtils.isJavaVersionAtLeast;
import static org.apache.commons.lang3.SystemUtils.isJavaVersionAtMost;
import static org.apache.geode.internal.serialization.filter.ApiPackage.JAVA_IO;
import static org.apache.geode.internal.serialization.filter.ApiPackage.SUN_MISC;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assumptions.assumeThat;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import java.util.Set;

import org.junit.Before;
import org.junit.Test;

public class ReflectiveFacadeObjectInputFilterFactoryTest {

  private static final Set<String> SANCTIONED_CLASSES = emptySet();

  private SerializableObjectConfig config;

  @Before
  public void setUp() {
    config = mock(SerializableObjectConfig.class);
    when(config.getValidateSerializableObjects()).thenReturn(true);
  }

  @Test
  public void createsJava9InputStreamFilter_onJava9orGreater() {
    assumeThat(isJavaVersionAtLeast(JAVA_9)).isTrue();

    // arrange
    ObjectInputFilterFactory factory = new ReflectiveFacadeObjectInputFilterFactory();

    // act
    ObjectInputFilter objectInputFilter = factory.create(config, SANCTIONED_CLASSES);

    // assert
    assertThat(getApiPackage(getObjectInputFilterApi(objectInputFilter))).isEqualTo(JAVA_IO);
  }

  @Test
  public void createsJava8InputStreamFilter_onJava8() {
    assumeThat(isJavaVersionAtMost(JAVA_1_8)).isTrue();

    // arrange
    ObjectInputFilterFactory factory = new ReflectiveFacadeObjectInputFilterFactory();

    // act
    ObjectInputFilter objectInputFilter = factory.create(config, SANCTIONED_CLASSES);

    // assert
    assertThat(getApiPackage(getObjectInputFilterApi(objectInputFilter))).isEqualTo(SUN_MISC);
  }

  private static ObjectInputFilterApi getObjectInputFilterApi(ObjectInputFilter result) {
    ReflectiveFacadeObjectInputFilter impl = (ReflectiveFacadeObjectInputFilter) result;
    ObjectInputFilterApi objectInputFilterApi = impl.getObjectInputFilterApi();
    assertThat(objectInputFilterApi).isInstanceOf(ReflectiveObjectInputFilterApi.class);
    return objectInputFilterApi;
  }

  private static ApiPackage getApiPackage(ObjectInputFilterApi reflectionObjectInputFilterApi) {
    return ((ReflectiveObjectInputFilterApi) reflectionObjectInputFilterApi).getApiPackage();
  }
}
