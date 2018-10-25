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
package org.apache.geode.management.internal.web.http.converter;

import java.io.IOException;
import java.io.Serializable;

import org.springframework.http.HttpInputMessage;
import org.springframework.http.HttpMessage;
import org.springframework.http.HttpOutputMessage;
import org.springframework.http.MediaType;
import org.springframework.http.converter.AbstractHttpMessageConverter;
import org.springframework.http.converter.HttpMessageNotReadableException;
import org.springframework.http.converter.HttpMessageNotWritableException;
import org.springframework.util.StreamUtils;

import org.apache.geode.internal.util.IOUtils;

/**
 * The SerializableObjectHttpMessageConverter class is a Spring HttpMessageConverter for converting
 * bytes streams to/from Serializable Objects.
 * <p/>
 *
 * @see java.io.Serializable
 * @see org.springframework.http.HttpInputMessage
 * @see org.springframework.http.HttpMessage
 * @see org.springframework.http.HttpOutputMessage
 * @see org.springframework.http.MediaType
 * @see org.springframework.http.converter.AbstractHttpMessageConverter
 * @since GemFire 8.0
 */
@SuppressWarnings("unused")
public class SerializableObjectHttpMessageConverter
    extends AbstractHttpMessageConverter<Serializable> {

  public SerializableObjectHttpMessageConverter() {
    super(MediaType.APPLICATION_OCTET_STREAM, MediaType.ALL);
  }

  @Override
  protected boolean supports(final Class<?> type) {
    if (logger.isTraceEnabled()) {
      logger.trace(String.format("%1$s.supports(%2$s)", getClass().getName(), type.getName()),
          new Throwable());
    }

    return (type != null && Serializable.class.isAssignableFrom(type));
  }

  @Override
  protected Serializable readInternal(final Class<? extends Serializable> type,
      final HttpInputMessage inputMessage) throws IOException, HttpMessageNotReadableException {
    try {
      ClassLoader classLoader = type.getClassLoader();
      return type.cast(IOUtils.deserializeObject(IOUtils.toByteArray(inputMessage.getBody()),
          classLoader != null ? classLoader : getClass().getClassLoader()));
    } catch (ClassNotFoundException e) {
      throw new HttpMessageNotReadableException(
          String.format("Unable to convert the HTTP message body into an Object of type (%1$s)",
              type.getName()),
          e);
    }
  }

  protected void setContentLength(final HttpMessage message, final byte[] messageBody) {
    message.getHeaders().setContentLength(messageBody.length);
  }

  @Override
  protected void writeInternal(final Serializable serializableObject,
      final HttpOutputMessage outputMessage) throws IOException, HttpMessageNotWritableException {
    final byte[] messageBody = IOUtils.serializeObject(serializableObject);
    setContentLength(outputMessage, messageBody);
    StreamUtils.copy(messageBody, outputMessage.getBody());
  }

}
