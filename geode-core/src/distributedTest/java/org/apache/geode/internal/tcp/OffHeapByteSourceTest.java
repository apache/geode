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
package org.apache.geode.internal.tcp;

import static org.apache.geode.internal.tcp.ByteBufferInputStream.OffHeapByteSource.determineUnaligned;
import static org.assertj.core.api.Assertions.assertThat;

import org.junit.jupiter.api.Test;

public class OffHeapByteSourceTest {
  @Test
  public void nullIsNotUnaligned() {
    assertThat(determineUnaligned(null)).isFalse();
  }

  @Test
  public void bogusIsNotUnaligned() {
    assertThat(determineUnaligned("bogus")).isFalse();
  }

  @Test
  public void i386IsUnaligned() {
    assertThat(determineUnaligned("i386")).isTrue();
  }

  @Test
  public void x86IsUnaligned() {
    assertThat(determineUnaligned("x86")).isTrue();
  }

  @Test
  public void amd64IsUnaligned() {
    assertThat(determineUnaligned("amd64")).isTrue();
  }

  @Test
  public void x86_64IsUnaligned() {
    assertThat(determineUnaligned("x86_64")).isTrue();
  }

  @Test
  public void ppc64IsUnaligned() {
    assertThat(determineUnaligned("ppc64")).isTrue();
  }

  @Test
  public void ppc64leIsUnaligned() {
    assertThat(determineUnaligned("ppc64le")).isTrue();
  }
}
