/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.gemstone.gemfire.management.internal.web.http.converter;

import static org.junit.Assert.*;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.Serializable;
import java.util.Calendar;

import org.jmock.Expectations;
import org.jmock.Mockery;
import org.jmock.lib.legacy.ClassImposteriser;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.springframework.http.HttpHeaders;
import org.springframework.http.HttpInputMessage;
import org.springframework.http.HttpOutputMessage;
import org.springframework.http.MediaType;

import com.gemstone.gemfire.internal.util.IOUtils;
import com.gemstone.gemfire.test.junit.categories.UnitTest;

/**
 * The SerializableObjectHttpMessageConverterJUnitTest class is a test suite of test cases testing the contract
 * and functionality of the SerializableObjectHttpMessageConverter class.
 * <p/>
 * @see com.gemstone.gemfire.management.internal.web.http.converter.SerializableObjectHttpMessageConverter
 * @see org.jmock.Mockery
 * @see org.junit.Assert
 * @see org.junit.Test
 * @since GemFire 8.0
 */
@Category(UnitTest.class)
public class SerializableObjectHttpMessageConverterJUnitTest {

  private Mockery mockContext;

  @Before
  public void setUp() {
    mockContext = new Mockery();
    mockContext.setImposteriser(ClassImposteriser.INSTANCE);
  }

  @After
  public void tearDown() {
    mockContext.assertIsSatisfied();
    mockContext = null;
  }

  @Test
  public void testCreateSerializableObjectHttpMessageConverter() {
    final SerializableObjectHttpMessageConverter converter = new SerializableObjectHttpMessageConverter();

    assertNotNull(converter);
    assertTrue(converter.getSupportedMediaTypes().contains(MediaType.APPLICATION_OCTET_STREAM));
    assertTrue(converter.getSupportedMediaTypes().contains(MediaType.ALL));
  }

  @Test
  public void testSupport() {
    final SerializableObjectHttpMessageConverter converter = new SerializableObjectHttpMessageConverter();

    assertTrue(converter.supports(Boolean.class));
    assertTrue(converter.supports(Calendar.class));
    assertTrue(converter.supports(Character.class));
    assertTrue(converter.supports(Integer.class));
    assertTrue(converter.supports(Double.class));
    assertTrue(converter.supports(String.class));
    assertTrue(converter.supports(Serializable.class));
    assertFalse(converter.supports(Object.class));
    assertFalse(converter.supports(null));
  }

  @Test
  public void testReadInternal() throws IOException {
    final String expectedInputMessageBody = "Expected content of the HTTP input message body!";

    final HttpInputMessage mockInputMessage = mockContext.mock(HttpInputMessage.class, "HttpInputMessage");

    mockContext.checking(new Expectations() {{
      oneOf(mockInputMessage).getBody();
      will(returnValue(new ByteArrayInputStream(IOUtils.serializeObject(expectedInputMessageBody))));
    }});

    final SerializableObjectHttpMessageConverter converter = new SerializableObjectHttpMessageConverter();

    final Serializable obj = converter.readInternal(String.class, mockInputMessage);

    assertTrue(obj instanceof String);
    assertEquals(expectedInputMessageBody, obj);
  }

  @Test
  public void testSetContentLength() {
    final byte[] bytes = { (byte) 0xCA, (byte) 0xFE, (byte) 0xBA, (byte) 0xBE };

    final HttpHeaders headers = new HttpHeaders();

    final HttpOutputMessage mockOutputMessage = mockContext.mock(HttpOutputMessage.class, "HttpOutputMessage");

    mockContext.checking(new Expectations() {{
      oneOf(mockOutputMessage).getHeaders();
      will(returnValue(headers));
    }});

    final SerializableObjectHttpMessageConverter converter = new SerializableObjectHttpMessageConverter();

    converter.setContentLength(mockOutputMessage, bytes);

    assertEquals(bytes.length, headers.getContentLength());
  }

  @Test
  public void testWriteInternal() throws IOException {
    final String expectedOutputMessageBody = "Expected media of the HTTP output message body!";

    final byte[] expectedOutputMessageBodyBytes = IOUtils.serializeObject(expectedOutputMessageBody);

    final ByteArrayOutputStream out = new ByteArrayOutputStream(expectedOutputMessageBodyBytes.length);

    final HttpHeaders headers = new HttpHeaders();

    final HttpOutputMessage mockOutputMessage = mockContext.mock(HttpOutputMessage.class, "HttpOutputMessage");

    mockContext.checking(new Expectations() {{
      oneOf(mockOutputMessage).getHeaders();
      will(returnValue(headers));
      oneOf(mockOutputMessage).getBody();
      will(returnValue(out));
    }});

    final SerializableObjectHttpMessageConverter converter = new SerializableObjectHttpMessageConverter();

    converter.writeInternal(expectedOutputMessageBody, mockOutputMessage);

    final byte[] actualOutputMessageBodyBytes = out.toByteArray();

    assertEquals(expectedOutputMessageBodyBytes.length, headers.getContentLength());
    assertEquals(expectedOutputMessageBodyBytes.length, actualOutputMessageBodyBytes.length);

    for (int index = 0; index < actualOutputMessageBodyBytes.length; index++) {
      assertEquals(expectedOutputMessageBodyBytes[index], actualOutputMessageBodyBytes[index]);
    }
  }

}
