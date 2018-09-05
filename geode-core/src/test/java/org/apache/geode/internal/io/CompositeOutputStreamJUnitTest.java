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
package org.apache.geode.internal.io;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.inOrder;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyZeroInteractions;

import java.io.IOException;
import java.io.OutputStream;

import org.junit.Test;
import org.mockito.InOrder;


/**
 * Unit tests for CompositeOutputStream.
 *
 * @since GemFire 7.0
 */
public class CompositeOutputStreamJUnitTest {

  @Test
  public void testNewCompositeOutputStreamWithNoStreams() throws IOException {
    final CompositeOutputStream cos = new CompositeOutputStream();
    assertThat(cos.isEmpty()).isTrue();
    assertThat(cos.size()).isEqualTo(0);

    cos.write(new byte[] {0, 1, 2, 3, 4, 5, 6, 7, 8, 9}, 2, 3);
    cos.write(new byte[] {0, 1});
    cos.write(9);
    cos.flush();
    cos.close();
  }

  @Test
  public void testMockOutputStream() throws IOException {
    final OutputStream mockOutputStream = mock(OutputStream.class, "OutputStream");
    mockOutputStream.write(new byte[] {0, 1, 2, 3, 4, 5, 6, 7, 8, 9}, 2, 3);
    mockOutputStream.write(new byte[] {0, 1});
    mockOutputStream.write(9);
    mockOutputStream.flush();
    mockOutputStream.close();

    verify(mockOutputStream, times(1)).write(new byte[] {0, 1, 2, 3, 4, 5, 6, 7, 8, 9}, 2, 3);
    verify(mockOutputStream, times(1)).write(new byte[] {0, 1});
    verify(mockOutputStream, times(1)).write(9);
    verify(mockOutputStream, times(1)).flush();
    verify(mockOutputStream, times(1)).close();
  }

  @Test
  public void testNewCompositeOutputStreamWithOneStream() throws IOException {
    final OutputStream mockStreamOne = mock(OutputStream.class, "streamOne");
    final CompositeOutputStream compositeOutputStream = new CompositeOutputStream(mockStreamOne);
    assertThat(compositeOutputStream.isEmpty()).isFalse();
    assertThat(compositeOutputStream.size()).isEqualTo(1);
    compositeOutputStream.write(new byte[] {0, 1, 2, 3, 4, 5, 6, 7, 8, 9}, 2, 3);
    compositeOutputStream.write(new byte[] {0, 1});
    compositeOutputStream.write(9);
    compositeOutputStream.flush();
    compositeOutputStream.close();

    InOrder inOrder = inOrder(mockStreamOne);
    inOrder.verify(mockStreamOne, times(1)).write(2);
    inOrder.verify(mockStreamOne, times(1)).write(3);
    inOrder.verify(mockStreamOne, times(1)).write(4);
    inOrder.verify(mockStreamOne, times(1)).write(0);
    inOrder.verify(mockStreamOne, times(1)).write(1);
    inOrder.verify(mockStreamOne, times(1)).write(9);
    inOrder.verify(mockStreamOne, times(2)).flush();
    inOrder.verify(mockStreamOne, times(1)).close();
  }

  @Test
  public void testNewCompositeOutputStreamWithTwoStreams() throws IOException {
    final OutputStream streamOne = mock(OutputStream.class, "streamOne");
    final OutputStream streamTwo = mock(OutputStream.class, "streamTwo");
    final CompositeOutputStream cos = new CompositeOutputStream(streamOne, streamTwo);
    assertThat(cos.isEmpty()).isFalse();
    assertThat(cos.size()).isEqualTo(2);
    cos.write(new byte[] {0, 1, 2, 3, 4, 5, 6, 7, 8, 9}, 2, 3);
    cos.write(new byte[] {0, 1});
    cos.write(9);
    cos.flush();
    cos.close();

    InOrder inOrderStreams = inOrder(streamOne, streamTwo);
    inOrderStreams.verify(streamOne, times(1)).write(2);
    inOrderStreams.verify(streamOne, times(1)).write(3);
    inOrderStreams.verify(streamOne, times(1)).write(4);
    inOrderStreams.verify(streamOne, times(1)).write(0);
    inOrderStreams.verify(streamOne, times(1)).write(1);
    inOrderStreams.verify(streamOne, times(1)).write(9);
    inOrderStreams.verify(streamOne, times(2)).flush();
    inOrderStreams.verify(streamOne, times(1)).close();
  }

  @Test
  public void testAddOutputStreamWithTwoStreams() throws IOException {
    final OutputStream streamOne = mock(OutputStream.class, "streamOne");
    final OutputStream streamTwo = mock(OutputStream.class, "streamTwo");
    final OutputStream streamThree = mock(OutputStream.class, "streamThree");
    final CompositeOutputStream cos = new CompositeOutputStream(streamOne, streamTwo);
    assertThat(cos.isEmpty()).isFalse();
    assertThat(cos.size()).isEqualTo(2);
    cos.addOutputStream(streamThree);
    assertThat(cos.size()).isEqualTo(3);
    cos.write(new byte[] {0, 1, 2, 3, 4, 5, 6, 7, 8, 9}, 2, 3);
    cos.write(new byte[] {0, 1});
    cos.write(9);
    cos.flush();
    cos.close();

    InOrder inOrderStreams = inOrder(streamOne, streamTwo, streamThree);
    inOrderStreams.verify(streamOne, times(1)).write(2);
    inOrderStreams.verify(streamOne, times(1)).write(3);
    inOrderStreams.verify(streamOne, times(1)).write(4);
    inOrderStreams.verify(streamOne, times(1)).write(0);
    inOrderStreams.verify(streamOne, times(1)).write(1);
    inOrderStreams.verify(streamOne, times(1)).write(9);
    inOrderStreams.verify(streamOne, times(2)).flush();
    inOrderStreams.verify(streamOne, times(1)).close();
  }

  @Test
  public void testAddOutputStreamWithOneStream() throws IOException {
    final OutputStream streamOne = mock(OutputStream.class, "streamOne");
    final OutputStream streamTwo = mock(OutputStream.class, "streamTwo");
    final CompositeOutputStream cos = new CompositeOutputStream(streamOne);
    assertThat(cos.isEmpty()).isFalse();
    assertThat(cos.size()).isEqualTo(1);
    cos.addOutputStream(streamTwo);
    assertThat(cos.size()).isEqualTo(2);
    cos.write(new byte[] {0, 1, 2, 3, 4, 5, 6, 7, 8, 9}, 2, 3);
    cos.write(new byte[] {0, 1});
    cos.write(9);
    cos.flush();
    cos.close();

    InOrder inOrderStreams = inOrder(streamOne, streamTwo);
    inOrderStreams.verify(streamOne, times(1)).write(2);
    inOrderStreams.verify(streamOne, times(1)).write(3);
    inOrderStreams.verify(streamOne, times(1)).write(4);
    inOrderStreams.verify(streamOne, times(1)).write(0);
    inOrderStreams.verify(streamOne, times(1)).write(1);
    inOrderStreams.verify(streamOne, times(1)).write(9);
    inOrderStreams.verify(streamOne, times(2)).flush();
    inOrderStreams.verify(streamOne, times(1)).close();
  }

  @Test
  public void testAddOneOutputStreamWhenEmpty() throws IOException {
    final OutputStream streamOne = mock(OutputStream.class, "streamOne");
    final CompositeOutputStream cos = new CompositeOutputStream();
    assertThat(cos.isEmpty()).isTrue();
    assertThat(cos.size()).isEqualTo(0);
    cos.addOutputStream(streamOne);
    assertThat(cos.isEmpty()).isFalse();
    assertThat(cos.size()).isEqualTo(1);
    cos.write(new byte[] {0, 1, 2, 3, 4, 5, 6, 7, 8, 9}, 2, 3);
    cos.write(new byte[] {0, 1});
    cos.write(9);
    cos.flush();
    cos.close();

    InOrder inOrderStreams = inOrder(streamOne);
    inOrderStreams.verify(streamOne, times(1)).write(2);
    inOrderStreams.verify(streamOne, times(1)).write(3);
    inOrderStreams.verify(streamOne, times(1)).write(4);
    inOrderStreams.verify(streamOne, times(1)).write(0);
    inOrderStreams.verify(streamOne, times(1)).write(1);
    inOrderStreams.verify(streamOne, times(1)).write(9);
    inOrderStreams.verify(streamOne, times(2)).flush();
    inOrderStreams.verify(streamOne, times(1)).close();
  }

  @Test
  public void testAddTwoOutputStreamsWhenEmpty() throws IOException {
    final OutputStream streamOne = mock(OutputStream.class, "streamOne");
    final OutputStream streamTwo = mock(OutputStream.class, "streamTwo");
    final CompositeOutputStream cos = new CompositeOutputStream();
    assertThat(cos.isEmpty()).isTrue();
    assertThat(cos.size()).isEqualTo(0);
    cos.addOutputStream(streamOne);
    cos.addOutputStream(streamTwo);
    assertThat(cos.isEmpty()).isFalse();
    assertThat(cos.size()).isEqualTo(2);
    cos.write(new byte[] {0, 1, 2, 3, 4, 5, 6, 7, 8, 9}, 2, 3);
    cos.write(new byte[] {0, 1});
    cos.write(9);
    cos.flush();
    cos.close();

    InOrder inOrderStreams = inOrder(streamOne, streamTwo);
    inOrderStreams.verify(streamOne, times(1)).write(2);
    inOrderStreams.verify(streamOne, times(1)).write(3);
    inOrderStreams.verify(streamOne, times(1)).write(4);
    inOrderStreams.verify(streamOne, times(1)).write(0);
    inOrderStreams.verify(streamOne, times(1)).write(1);
    inOrderStreams.verify(streamOne, times(1)).write(9);
    inOrderStreams.verify(streamOne, times(2)).flush();
    inOrderStreams.verify(streamOne, times(1)).close();
  }

  @Test
  public void testRemoveOutputStreamWithTwoStreams() throws IOException {
    final OutputStream streamOne = mock(OutputStream.class, "streamOne");
    final OutputStream streamTwo = mock(OutputStream.class, "streamTwo");
    final CompositeOutputStream cos = new CompositeOutputStream(streamOne, streamTwo);
    assertThat(cos.isEmpty()).isFalse();
    assertThat(cos.size()).isEqualTo(2);
    cos.removeOutputStream(streamTwo);
    assertThat(cos.isEmpty()).isFalse();
    assertThat(cos.size()).isEqualTo(1);
    cos.write(new byte[] {0, 1, 2, 3, 4, 5, 6, 7, 8, 9}, 2, 3);
    cos.write(new byte[] {0, 1});
    cos.write(9);
    cos.flush();
    cos.close();

    verifyZeroInteractions(streamTwo);
    InOrder inOrderStreams = inOrder(streamOne);
    inOrderStreams.verify(streamOne, times(1)).write(2);
    inOrderStreams.verify(streamOne, times(1)).write(3);
    inOrderStreams.verify(streamOne, times(1)).write(4);
    inOrderStreams.verify(streamOne, times(1)).write(0);
    inOrderStreams.verify(streamOne, times(1)).write(1);
    inOrderStreams.verify(streamOne, times(1)).write(9);
    inOrderStreams.verify(streamOne, times(2)).flush();
    inOrderStreams.verify(streamOne, times(1)).close();
  }

  @Test
  public void testRemoveOutputStreamWithOneStream() throws IOException {
    final OutputStream streamOne = mock(OutputStream.class, "streamOne");
    final CompositeOutputStream cos = new CompositeOutputStream(streamOne);
    assertThat(cos.isEmpty()).isFalse();
    assertThat(cos.size()).isEqualTo(1);
    cos.removeOutputStream(streamOne);
    assertThat(cos.isEmpty()).isTrue();
    assertThat(cos.size()).isEqualTo(0);
    cos.write(new byte[] {0, 1, 2, 3, 4, 5, 6, 7, 8, 9}, 2, 3);
    cos.write(new byte[] {0, 1});
    cos.write(9);
    cos.flush();
    cos.close();

    verifyZeroInteractions(streamOne);
  }

  @Test
  public void testRemoveOutputStreamWhenEmpty() throws IOException {
    final OutputStream streamOne = mock(OutputStream.class, "streamOne");
    final CompositeOutputStream cos = new CompositeOutputStream();
    assertThat(cos.isEmpty()).isTrue();
    assertThat(cos.size()).isEqualTo(0);
    cos.removeOutputStream(streamOne);
    assertThat(cos.isEmpty()).isTrue();
    assertThat(cos.size()).isEqualTo(0);
    cos.write(new byte[] {0, 1, 2, 3, 4, 5, 6, 7, 8, 9}, 2, 3);
    cos.write(new byte[] {0, 1});
    cos.write(9);
    cos.flush();
    cos.close();

    verifyZeroInteractions(streamOne);
  }
}
