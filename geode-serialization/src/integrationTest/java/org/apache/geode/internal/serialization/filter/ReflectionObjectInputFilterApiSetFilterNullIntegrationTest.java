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
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.catchThrowable;

import java.lang.reflect.InvocationTargetException;

import org.junit.Before;
import org.junit.Test;

/**
 * This test calls the real setSerialFilter which can only be set once within a JVM, so it's in an
 * integration test which forks a new JVM for the test class.
 */
public class ReflectionObjectInputFilterApiSetFilterNullIntegrationTest {

  private ApiPackage apiPackage;

  @Before
  public void setUp() {
    if (isJavaVersionAtLeast(JAVA_9)) {
      apiPackage = ApiPackage.JAVA_IO;
    }
    if (isJavaVersionAtLeast(JAVA_1_8)) {
      apiPackage = ApiPackage.SUN_MISC;
    }
  }

  @Test
  public void throwsNullPointerExceptionGivenNull()
      throws ClassNotFoundException, NoSuchMethodException {
    ObjectInputFilterApi api = new ReflectionObjectInputFilterApi(apiPackage);

    Throwable thrown = catchThrowable(() -> {
      api.setSerialFilter(null);
    });

    assertThat(thrown)
        .isInstanceOf(InvocationTargetException.class)
        .hasCauseInstanceOf(NullPointerException.class);
  }
}
