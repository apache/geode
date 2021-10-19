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


package org.apache.geode.unsafe.internal.sun.nio.ch;

import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.parallel.ExecutionMode.CONCURRENT;

import java.nio.ByteBuffer;

import org.junit.jupiter.api.MethodOrderer;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestMethodOrder;
import org.junit.jupiter.api.parallel.Execution;

@Execution(CONCURRENT)
@TestMethodOrder(MethodOrderer.Random.class)
public class DirectBufferTest {

  @Test
  public void attachmentIsNullForNonDirectBuffer() {
    assertThat(DirectBuffer.attachment(null)).isNull();
    assertThat(DirectBuffer.attachment(new Object())).isNull();
    assertThat(DirectBuffer.attachment(ByteBuffer.allocate(1))).isNull();
  }

  @Test
  public void attachmentIsNullForUnslicedDirectBuffer() {
    assertThat(DirectBuffer.attachment(ByteBuffer.allocateDirect(1))).isNull();
  }

  @Test
  public void attachmentIsRootBufferForDirectBufferSlice() {
    final ByteBuffer root = ByteBuffer.allocateDirect(10);
    final ByteBuffer slice = root.slice();

    assertThat(DirectBuffer.attachment(slice)).isSameAs(root);
  }

}
