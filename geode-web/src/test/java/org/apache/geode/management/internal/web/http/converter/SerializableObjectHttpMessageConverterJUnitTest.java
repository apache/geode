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

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.Serializable;
import java.util.Calendar;

import org.junit.Before;
import org.junit.Test;
import org.springframework.http.HttpHeaders;
import org.springframework.http.HttpInputMessage;
import org.springframework.http.HttpOutputMessage;
import org.springframework.http.MediaType;

import org.apache.geode.internal.util.IOUtils;

/**
 * The SerializableObjectHttpMessageConverterJUnitTest class is a test suite of test cases testing
 * the contract and functionality of the SerializableObjectHttpMessageConverter class.
 * <p/>
 *
 * @see org.apache.geode.management.internal.web.http.converter.SerializableObjectHttpMessageConverter
 * @see org.junit.Test
 * @since GemFire 8.0
 */
public class SerializableObjectHttpMessageConverterJUnitTest {
  private SerializableObjectHttpMessageConverter converter;

  @Before
  public void setUp() {
    converter = new SerializableObjectHttpMessageConverter();
  }

  @Test
  public void testCreateSerializableObjectHttpMessageConverter() {
    assertThat(converter).isNotNull();
    assertThat(converter.getSupportedMediaTypes().contains(MediaType.ALL)).isTrue();
    assertThat(converter.getSupportedMediaTypes().contains(MediaType.APPLICATION_OCTET_STREAM))
        .isTrue();
  }

  @Test
  public void testSupport() {
    assertThat(converter.supports(Boolean.class)).isTrue();
    assertThat(converter.supports(Calendar.class)).isTrue();
    assertThat(converter.supports(Character.class)).isTrue();
    assertThat(converter.supports(Integer.class)).isTrue();
    assertThat(converter.supports(Double.class)).isTrue();
    assertThat(converter.supports(String.class)).isTrue();
    assertThat(converter.supports(Serializable.class)).isTrue();
    assertThat(converter.supports(Object.class)).isFalse();
    assertThat(converter.supports(null)).isFalse();
  }

  @Test
  public void testReadInternal() throws IOException {
    final String expectedInputMessageBody = "Expected content of the HTTP input message body!";
    final HttpInputMessage mockInputMessage = mock(HttpInputMessage.class, "HttpInputMessage");
    when(mockInputMessage.getBody())
        .thenReturn(new ByteArrayInputStream(IOUtils.serializeObject(expectedInputMessageBody)));

    final Serializable obj = converter.readInternal(String.class, mockInputMessage);
    assertThat(obj).isInstanceOf(String.class);
    assertThat(obj).isEqualTo(expectedInputMessageBody);
  }

  @Test
  public void testSetContentLength() {
    final HttpHeaders headers = new HttpHeaders();
    final byte[] bytes = {(byte) 0xCA, (byte) 0xFE, (byte) 0xBA, (byte) 0xBE};
    final HttpOutputMessage mockOutputMessage = mock(HttpOutputMessage.class, "HttpOutputMessage");
    when(mockOutputMessage.getHeaders()).thenReturn(headers);

    converter.setContentLength(mockOutputMessage, bytes);
    assertThat(headers.getContentLength()).isEqualTo(bytes.length);
  }

  @Test
  public void testWriteInternal() throws IOException {
    final HttpHeaders headers = new HttpHeaders();
    final String expectedOutputMessageBody = "Expected media of the HTTP output message body!";
    final byte[] expectedOutputMessageBodyBytes =
        IOUtils.serializeObject(expectedOutputMessageBody);
    final ByteArrayOutputStream out =
        new ByteArrayOutputStream(expectedOutputMessageBodyBytes.length);
    final HttpOutputMessage mockOutputMessage = mock(HttpOutputMessage.class, "HttpOutputMessage");
    when(mockOutputMessage.getBody()).thenReturn(out);
    when(mockOutputMessage.getHeaders()).thenReturn(headers);

    converter.writeInternal(expectedOutputMessageBody, mockOutputMessage);
    final byte[] actualOutputMessageBodyBytes = out.toByteArray();
    assertThat(headers.getContentLength()).isEqualTo(expectedOutputMessageBodyBytes.length);
    assertThat(actualOutputMessageBodyBytes.length)
        .isEqualTo(expectedOutputMessageBodyBytes.length);
    assertThat(actualOutputMessageBodyBytes).isEqualTo(expectedOutputMessageBodyBytes);
  }
}
