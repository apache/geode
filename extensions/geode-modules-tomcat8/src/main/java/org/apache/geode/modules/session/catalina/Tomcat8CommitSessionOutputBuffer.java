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

import java.io.IOException;
import java.nio.ByteBuffer;

import org.apache.coyote.OutputBuffer;
import org.apache.tomcat.util.buf.ByteChunk;

/**
 * Delegating {@link OutputBuffer} that commits sessions on write through. Output data is buffered
 * ahead of this object and flushed through this interface when full or explicitly flushed.
 */
class Tomcat8CommitSessionOutputBuffer implements OutputBuffer {

  private final SessionCommitter sessionCommitter;
  private final OutputBuffer delegate;

  public Tomcat8CommitSessionOutputBuffer(final SessionCommitter sessionCommitter,
      final OutputBuffer delegate) {
    this.sessionCommitter = sessionCommitter;
    this.delegate = delegate;
  }

  @Deprecated
  @Override
  public int doWrite(final ByteChunk chunk) throws IOException {
    sessionCommitter.commit();
    return delegate.doWrite(chunk);
  }

  @Override
  public int doWrite(final ByteBuffer chunk) throws IOException {
    sessionCommitter.commit();
    return delegate.doWrite(chunk);
  }

  @Override
  public long getBytesWritten() {
    return delegate.getBytesWritten();
  }

  OutputBuffer getDelegate() {
    return delegate;
  }
}
