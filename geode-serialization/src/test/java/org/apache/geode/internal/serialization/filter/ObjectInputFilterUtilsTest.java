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
import static org.apache.geode.internal.serialization.filter.ObjectInputFilterUtils.supportsObjectInputFilter;
import static org.apache.geode.util.internal.UncheckedUtils.uncheckedCast;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assumptions.assumeThat;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.util.function.Function;

import org.junit.Test;

public class ObjectInputFilterUtilsTest {

  @Test
  public void supportsObjectInputFilter_delegatesToClassUtils() {
    Function<String, Boolean> isClassAvailable = uncheckedCast(mock(Function.class));
    when(isClassAvailable.apply(anyString())).thenReturn(true);

    supportsObjectInputFilter(isClassAvailable);

    verify(isClassAvailable).apply(anyString());
  }

  @Test
  public void supportsObjectInputFilter_checksSunMisc() {
    Function<String, Boolean> isClassAvailable = uncheckedCast(mock(Function.class));
    when(isClassAvailable.apply(anyString())).thenReturn(false);

    supportsObjectInputFilter(isClassAvailable);

    verify(isClassAvailable).apply("sun.misc.ObjectInputFilter");
  }

  @Test
  public void supportsObjectInputFilter_checksJavaIo() {
    Function<String, Boolean> isClassAvailable = uncheckedCast(mock(Function.class));
    when(isClassAvailable.apply(anyString())).thenReturn(false);

    supportsObjectInputFilter(isClassAvailable);

    verify(isClassAvailable).apply("java.io.ObjectInputFilter");
  }

  @Test
  public void supportsObjectInputFilter_checksSunMiscAndJavaIo() {
    Function<String, Boolean> isClassAvailable = uncheckedCast(mock(Function.class));
    when(isClassAvailable.apply(anyString())).thenReturn(false);

    supportsObjectInputFilter(isClassAvailable);

    verify(isClassAvailable).apply("sun.misc.ObjectInputFilter");
    verify(isClassAvailable).apply("java.io.ObjectInputFilter");
  }

  @Test
  public void supportsObjectInputFilter_returnsTrue_onJava8() {
    assumeThat(isJavaVersionAtMost(JAVA_1_8)).isTrue();

    boolean result = supportsObjectInputFilter();

    assertThat(result).isTrue();
  }

  @Test
  public void supportsObjectInputFilter_returnsTrue_onJava9orGreater() {
    assumeThat(isJavaVersionAtLeast(JAVA_9)).isTrue();

    boolean result = supportsObjectInputFilter();

    assertThat(result).isTrue();
  }
}
