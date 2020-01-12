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
package org.apache.geode.internal.cache.tier.sockets;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;

import java.io.OutputStream;
import java.nio.ByteBuffer;

import org.junit.Test;
import org.junit.experimental.categories.Category;

import org.apache.geode.test.junit.categories.ClientServerTest;

@Category({ClientServerTest.class})
public class PartTest {

  @Test
  public void shouldBeMockable() throws Exception {
    Part mockPart = mock(Part.class);
    OutputStream mockOutputStream = mock(OutputStream.class);
    ByteBuffer mockByteBuffer = mock(ByteBuffer.class);

    mockPart.writeTo(mockOutputStream, mockByteBuffer);

    verify(mockPart, times(1)).writeTo(mockOutputStream, mockByteBuffer);
  }

  @Test
  public void getCacheStringReturnsCanonicalInstance() {
    String stringValue = "test string";
    Part part1 = new Part();
    byte[] stringBytes1 = CacheServerHelper.toUTF(stringValue);
    part1.setPartState(stringBytes1, false);
    Part part2 = new Part();
    byte[] stringBytes2 = CacheServerHelper.toUTF(stringValue);
    part2.setPartState(stringBytes2, false);

    String result1 = part1.getCachedString();
    String result2 = part2.getCachedString();

    assertThat(result1).isEqualTo(stringValue);
    assertThat(result1).isSameAs(result2);
  }

  @Test
  public void getCacheStringWithNullStateReturnsNull() {
    Part part = new Part();

    String result = part.getCachedString();

    assertThat(result).isNull();
  }

  @Test
  public void getCachedStringGivenPartThatIsNotBytesThrows() {
    Part part = new Part();
    part.setPartState(new byte[0], true);

    assertThatThrownBy(() -> part.getCachedString())
        .hasMessageContaining("expected String part to be of type BYTE, part =");
  }
}
