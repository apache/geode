// Copyright (c) VMware, Inc. 2022. All rights reserved.
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
package org.apache.geode.internal.size;

import static org.apache.geode.internal.size.ReflectionSingleObjectSizer.REFERENCE_SIZE;
import static org.apache.geode.internal.size.ReflectionSingleObjectSizer.safeSizeof;
import static org.apache.geode.internal.size.ReflectionSingleObjectSizer.sizeof;
import static org.apache.geode.internal.size.ReflectionSingleObjectSizer.unsafeSizeof;
import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import org.junit.Test;

import org.apache.geode.unsafe.internal.sun.misc.Unsafe;

public class ReflectionSingleObjectSizerTest {

  @Test
  public void sizeofReturnsSafeSizeofGivenUnsafeFieldOffsetUnsupported() {
    Unsafe unsafe = mock(Unsafe.class);
    when(unsafe.objectFieldOffset(any())).thenThrow(UnsupportedOperationException.class);

    long result = sizeof(TestClass.class, false, unsafe);

    assertThat(result).isEqualTo(safeSizeof(TestClass.class));
  }

  @Test
  public void sizeofReturnsSafeSizeofGivenNullUnsafe() {
    long result = sizeof(TestClass.class, false, null);
    assertThat(result).isEqualTo(safeSizeof(TestClass.class));
  }

  @Test
  public void unsafeSizeofReturnsMinusOneGivenNullUnsafe() {
    long result = unsafeSizeof(TestClass.class, null);
    assertThat(result).isEqualTo(-1);
  }

  @Test
  public void unsafeSizeofReturnsFieldOffsetGivenMockedUnsafeFieldOffset() {
    Unsafe unsafe = mock(Unsafe.class);
    final long FIELD_OFFSET = 37;
    when(unsafe.objectFieldOffset(any())).thenReturn(FIELD_OFFSET);

    long result = unsafeSizeof(TestClass.class, unsafe);

    assertThat(result).isEqualTo(FIELD_OFFSET + REFERENCE_SIZE);
  }

  @Test
  public void unsafeSizeofReturnsMinusOneGivenUnsafeFieldOffsetUnsupported() {
    Unsafe unsafe = mock(Unsafe.class);
    when(unsafe.objectFieldOffset(any())).thenThrow(UnsupportedOperationException.class);

    long result = unsafeSizeof(TestClass.class, unsafe);

    assertThat(result).isEqualTo(-1);
  }

  private static class TestClass {
    public final Object reference = new Object();
  }
}
