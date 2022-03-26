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

import static org.apache.geode.internal.serialization.filter.ApiPackage.JAVA_IO;
import static org.apache.geode.internal.serialization.filter.ApiPackage.SUN_MISC;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.catchThrowable;

import org.junit.Test;

public class ApiPackageTest {

  @Test
  public void JAVA_IO_prefix() {
    assertThat(JAVA_IO.getPrefix()).isEqualTo("java.io.");
  }

  @Test
  public void SUN_MISC_prefix() {
    assertThat(SUN_MISC.getPrefix()).isEqualTo("sun.misc.");
  }

  @Test
  public void qualifyEmptyStringEqualsPrefix() {
    assertThat(JAVA_IO.qualify("")).isEqualTo(JAVA_IO.getPrefix());
  }

  @Test
  public void qualifyNullStringThrowsNullPointerException() {
    Throwable thrown = catchThrowable(() -> {
      JAVA_IO.qualify(null);
    });

    assertThat(thrown)
        .isInstanceOf(NullPointerException.class)
        .hasMessage("className is required");
  }

  @Test
  public void qualifyReturnsPrefixedClassName() {
    ApiPackage apiPackage = JAVA_IO;
    String className = "className";

    String qualifiedClassName = apiPackage.qualify(className);

    assertThat(qualifiedClassName).isEqualTo(apiPackage.getPrefix() + className);
  }
}
