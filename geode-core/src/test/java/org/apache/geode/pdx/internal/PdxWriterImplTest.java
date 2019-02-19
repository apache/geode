/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.geode.pdx.internal;

import static org.assertj.core.api.Assertions.assertThat;

import java.nio.ByteBuffer;

import org.junit.Test;
import org.junit.experimental.categories.Category;

import org.apache.geode.test.junit.categories.SerializationTest;

@Category({SerializationTest.class})
public class PdxWriterImplTest {

  @Test
  public void copyingEmptySourceReturnsEmptyResult() {
    ByteBuffer source = ByteBuffer.allocate(10);
    source.limit(0);

    ByteBuffer result = PdxWriterImpl.copyRemainingBytes(source);

    assertThat(source.position()).isEqualTo(0);
    assertThat(source.limit()).isEqualTo(0);
    assertThat(result.capacity()).isEqualTo(0);
    assertThat(result.remaining()).isEqualTo(0);
  }

  @Test
  public void copyingFullSourceReturnsFullResult() {
    ByteBuffer source = ByteBuffer.allocate(10);
    source.put((byte) 1);
    source.put((byte) 2);
    source.put((byte) 3);
    source.flip();

    ByteBuffer result = PdxWriterImpl.copyRemainingBytes(source);

    assertThat(source.position()).isEqualTo(0);
    assertThat(source.limit()).isEqualTo(3);
    assertThat(result.remaining()).isEqualTo(3);
    assertThat(result.capacity()).isEqualTo(3);
    assertThat(result.get(0)).isEqualTo((byte) 1);
    assertThat(result.get(1)).isEqualTo((byte) 2);
    assertThat(result.get(2)).isEqualTo((byte) 3);
  }

  @Test
  public void copyingPartialSourceReturnsPartialResult() {
    ByteBuffer source = ByteBuffer.allocate(10);
    source.put((byte) 1);
    source.put((byte) 2);
    source.put((byte) 3);
    source.put((byte) 4);
    source.position(1);
    source.limit(3);

    ByteBuffer result = PdxWriterImpl.copyRemainingBytes(source);

    assertThat(source.position()).isEqualTo(1);
    assertThat(source.limit()).isEqualTo(3);
    assertThat(result.remaining()).isEqualTo(2);
    assertThat(result.capacity()).isEqualTo(2);
    assertThat(result.get(0)).isEqualTo((byte) 2);
    assertThat(result.get(1)).isEqualTo((byte) 3);
  }

}
