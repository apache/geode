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
package org.apache.geode.rest.internal.web.http.converter;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.OutputStream;
import java.util.concurrent.atomic.AtomicLong;

import org.springframework.http.HttpHeaders;
import org.springframework.http.HttpOutputMessage;
import org.springframework.http.converter.HttpMessageNotWritableException;
import org.springframework.http.converter.json.MappingJackson2HttpMessageConverter;
import org.springframework.util.Assert;

/**
 * The CustomMappingJackson2HttpMessageConverter class...
 *
 * @see org.springframework.http.converter.json.MappingJackson2HttpMessageConverter
 * @since GemFire 0.0.1
 */
@SuppressWarnings("unused")
public class CustomMappingJackson2HttpMessageConverter extends MappingJackson2HttpMessageConverter {

  protected static final int INITIAL_BYTE_ARRAY_BUFFER_SIZE = 8192;

  @Override
  protected void writeInternal(final Object object, final HttpOutputMessage outputMessage)
      throws IOException, HttpMessageNotWritableException {
    HttpOutputMessageWrapper outputMessageWrapper =
        new BufferingHttpOutputMessageWrapper(outputMessage);
    super.writeInternal(object, outputMessageWrapper);
    outputMessageWrapper.flush();
  }

  protected static class BufferingHttpOutputMessageWrapper implements HttpOutputMessageWrapper {

    private final ByteArrayOutputStream outputStream;

    private final HttpOutputMessage httpOutputMessage;

    protected BufferingHttpOutputMessageWrapper(final HttpOutputMessage httpOutputMessage) {
      Assert.notNull(httpOutputMessage, "The HttpOutputMessage instance to wrap must not be null!");
      this.httpOutputMessage = httpOutputMessage;
      outputStream = new ByteArrayOutputStream(INITIAL_BYTE_ARRAY_BUFFER_SIZE);
    }

    @Override
    public OutputStream getBody() throws IOException {
      return outputStream;
    }

    @Override
    public long getContentLength() {
      return outputStream.size();
    }

    @Override
    public HttpHeaders getHeaders() {
      return httpOutputMessage.getHeaders();
    }

    @Override
    public void flush() throws IOException {
      getHeaders().setContentLength(getContentLength());
      outputStream.writeTo(httpOutputMessage.getBody());
      outputStream.reset();
    }
  }

  /**
   * While sound idea in theory to "count the bytes as you stream/write", thus preserving memory,
   * this does not work in practice since the HTTP headers must be written to the HTTP output stream
   * response before the body!
   */
  protected static class ContentLengthAccessibleHttpOutputMessageWrapper
      implements HttpOutputMessageWrapper {

    private final ByteCountingOutputStream outputStream;

    private final HttpOutputMessage httpOutputMessage;

    protected ContentLengthAccessibleHttpOutputMessageWrapper(
        final HttpOutputMessage httpOutputMessage) throws IOException {
      Assert.notNull(httpOutputMessage, "The HttpOutputMessage instance to wrap must not be null!");
      this.httpOutputMessage = httpOutputMessage;
      outputStream = new ByteCountingOutputStream(this.httpOutputMessage.getBody());
    }

    @Override
    public OutputStream getBody() throws IOException {
      return outputStream;
    }

    @Override
    public long getContentLength() {
      return outputStream.getByteCount();
    }

    @Override
    public HttpHeaders getHeaders() {
      return httpOutputMessage.getHeaders();
    }

    @Override
    public void flush() throws IOException {
      getHeaders().setContentLength(getContentLength());
    }
  }

  protected interface HttpOutputMessageWrapper extends HttpOutputMessage {

    long getContentLength();

    void flush() throws IOException;

  }

  protected static class ByteCountingOutputStream extends OutputStream {

    private final AtomicLong byteCount = new AtomicLong(0l);

    private final OutputStream outputStream;

    protected ByteCountingOutputStream(final OutputStream outputStream) {
      Assert.notNull(outputStream, "The OutputStream to wrap must not be null!");
      this.outputStream = outputStream;
    }

    protected long getByteCount() {
      return byteCount.get();
    }

    @Override
    public void write(final int byteData) throws IOException {
      outputStream.write(byteData);
      byteCount.incrementAndGet();
    }

    @Override
    public void write(byte[] b, int off, int len) throws IOException {
      outputStream.write(b, off, len);
      byteCount.addAndGet(len);
    }
  }

}
