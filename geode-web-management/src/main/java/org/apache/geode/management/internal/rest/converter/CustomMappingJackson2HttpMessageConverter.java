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
package org.apache.geode.management.internal.rest.converter;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.OutputStream;

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
public class CustomMappingJackson2HttpMessageConverter extends MappingJackson2HttpMessageConverter {

  protected static final int INITIAL_BYTE_ARRAY_BUFFER_SIZE = 8192;

  @Override
  protected void writeInternal(final Object object, final HttpOutputMessage outputMessage)
      throws IOException, HttpMessageNotWritableException {
    BufferingHttpOutputMessageWrapper outputMessageWrapper =
        new BufferingHttpOutputMessageWrapper(outputMessage);
    super.writeInternal(object, outputMessageWrapper);
    outputMessageWrapper.flush();
  }

  protected static class BufferingHttpOutputMessageWrapper implements HttpOutputMessage {
    private final ByteArrayOutputStream outputStream;
    private final HttpOutputMessage httpOutputMessage;

    protected BufferingHttpOutputMessageWrapper(final HttpOutputMessage httpOutputMessage) {
      Assert.notNull(httpOutputMessage, "The HttpOutputMessage instance to wrap must not be null!");
      this.httpOutputMessage = httpOutputMessage;
      outputStream = new ByteArrayOutputStream(INITIAL_BYTE_ARRAY_BUFFER_SIZE);
    }

    @Override
    public OutputStream getBody() {
      return outputStream;
    }

    public long getContentLength() {
      return outputStream.size();
    }

    @Override
    public HttpHeaders getHeaders() {
      return httpOutputMessage.getHeaders();
    }

    public void flush() throws IOException {
      getHeaders().setContentLength(getContentLength());
      outputStream.writeTo(httpOutputMessage.getBody());
      outputStream.reset();
    }
  }
}
