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

package org.apache.geode.modules.session.catalina;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.inOrder;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import java.io.IOException;
import java.nio.ByteBuffer;

import org.apache.coyote.OutputBuffer;
import org.junit.Test;
import org.mockito.InOrder;

public class Tomcat9CommitSessionOutputBufferTest {

  final SessionCommitter sessionCommitter = mock(SessionCommitter.class);
  final OutputBuffer delegate = mock(OutputBuffer.class);

  final Tomcat9CommitSessionOutputBuffer commitSesssionOutputBuffer =
      new Tomcat9CommitSessionOutputBuffer(sessionCommitter, delegate);

  @Test
  public void testDoWrite() throws IOException {
    final ByteBuffer byteBuffer = ByteBuffer.allocate(0);

    commitSesssionOutputBuffer.doWrite(byteBuffer);

    final InOrder inOrder = inOrder(sessionCommitter, delegate);
    inOrder.verify(sessionCommitter).commit();
    inOrder.verify(delegate).doWrite(byteBuffer);
    inOrder.verifyNoMoreInteractions();
  }

  @Test
  public void getBytesWritten() {
    when(delegate.getBytesWritten()).thenReturn(42L);

    assertThat(commitSesssionOutputBuffer.getBytesWritten()).isEqualTo(42L);

    final InOrder inOrder = inOrder(sessionCommitter, delegate);
    inOrder.verify(delegate).getBytesWritten();
    inOrder.verifyNoMoreInteractions();
  }
}
