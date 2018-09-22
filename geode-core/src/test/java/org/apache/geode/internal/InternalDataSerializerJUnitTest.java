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
package org.apache.geode.internal;

import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.mock;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.net.SocketException;
import java.util.Properties;

import org.apache.logging.log4j.Level;
import org.apache.logging.log4j.simple.SimpleLogger;
import org.apache.logging.log4j.util.PropertiesUtil;
import org.junit.Test;
import org.junit.experimental.categories.Category;

import org.apache.geode.DataSerializable;
import org.apache.geode.InternalGemFireException;
import org.apache.geode.cache.execute.Function;
import org.apache.geode.cache.execute.FunctionContext;
import org.apache.geode.examples.security.ExampleSecurityManager;
import org.apache.geode.test.junit.categories.SerializationTest;

/**
 * Tests the functionality of the {@link InternalDataSerializer} class.
 */
@Category({SerializationTest.class})
public class InternalDataSerializerJUnitTest {
  @Test
  public void testIsGemfireObject() {
    assertTrue("Instances of Function are GemFire objects",
        InternalDataSerializer.isGemfireObject(new TestFunction()));
    assertFalse("Instances of PdxSerializaerObject are NOT GemFire objects",
        InternalDataSerializer.isGemfireObject(new TestPdxSerializerObject()));
    assertFalse("Instances of anything under org.apache. are GemFire objects",
        InternalDataSerializer.isGemfireObject(new SimpleLogger("", Level.OFF, false, false, false,
            false, "", null, new PropertiesUtil(new Properties()), null)));
    assertTrue("Instances of anything in org.apache.geode. are GemFire objects",
        InternalDataSerializer.isGemfireObject(new InternalGemFireException()));
    assertTrue("Instances of anything under org.apache.geode. are GemFire objects",
        InternalDataSerializer.isGemfireObject(new ExampleSecurityManager()));
  }

  @Test
  public void testInvokeFromData_SocketExceptionRethrown()
      throws IOException, ClassNotFoundException {
    DataInput in = mock(DataInput.class);
    DataSerializable ds = mock(DataSerializable.class);

    doThrow(SocketException.class).when(ds).fromData(in);

    assertThatThrownBy(() -> InternalDataSerializer.invokeFromData(ds, in))
        .isInstanceOf(SocketException.class);
  }

  @Test
  public void testBasicReadObject_SocketExceptionReThrown()
      throws IOException, ClassNotFoundException {
    DataInput in = mock(DataInput.class);
    doReturn(DSCODE.DS_NO_FIXED_ID.toByte()).doReturn(DSCODE.CLASS.toByte())
        .doReturn(DSCODE.STRING.toByte()).when(in).readByte();
    doReturn(
        "org.apache.geode.internal.InternalDataSerializerJUnitTest$SocketExceptionThrowingDataSerializable")
            .when(in).readUTF();

    assertThatThrownBy(() -> InternalDataSerializer.basicReadObject(in))
        .isInstanceOf(SocketException.class);
  }

  class TestFunction implements Function {
    @Override
    public void execute(FunctionContext context) {
      // NOP
    }
  }

  class TestPdxSerializerObject implements PdxSerializerObject {
  }

  // Class must be static in order to call the constructor via reflection in the serializer
  public static class SocketExceptionThrowingDataSerializable implements DataSerializable {
    public SocketExceptionThrowingDataSerializable() {}

    @Override
    public void toData(DataOutput out) throws IOException {
      // Not needed for test
    }

    @Override
    public void fromData(DataInput in) throws IOException, ClassNotFoundException {
      throw new SocketException();
    }
  }
}
