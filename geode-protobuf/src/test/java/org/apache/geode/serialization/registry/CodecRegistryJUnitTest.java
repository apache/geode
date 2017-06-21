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
package org.apache.geode.serialization.registry;

import org.apache.geode.serialization.SerializationType;
import org.apache.geode.serialization.TypeCodec;
import org.apache.geode.serialization.registry.exception.CodecAlreadyRegisteredForTypeException;
import org.apache.geode.serialization.registry.exception.CodecNotRegisteredForTypeException;
import org.apache.geode.test.junit.categories.UnitTest;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;
import org.powermock.api.mockito.PowerMockito;
import org.powermock.core.classloader.annotations.PowerMockIgnore;
import org.powermock.core.classloader.annotations.PrepareForTest;
import org.powermock.modules.junit4.PowerMockRunner;

@Category(UnitTest.class)
@RunWith(PowerMockRunner.class)
@PowerMockIgnore("*.UnitTest")
@PrepareForTest({SerializationType.class})
public class CodecRegistryJUnitTest {
  private SerializationCodecRegistry codecRegistry;

  @Before
  public void startup() throws CodecAlreadyRegisteredForTypeException {
    codecRegistry = new SerializationCodecRegistry();
  }

  @After
  public void tearDown() {
    codecRegistry.shutdown();
  }

  @Test
  public void testRegisterCodec() throws CodecAlreadyRegisteredForTypeException {
    Assert.assertEquals(10, codecRegistry.getRegisteredCodecCount());
    SerializationType mockSerializationType = PowerMockito.mock(SerializationType.class);
    codecRegistry.register(mockSerializationType, new DummyTypeCodec());
    Assert.assertEquals(11, codecRegistry.getRegisteredCodecCount());
  }

  @Test
  public void testRegisteringCodecForRegisteredType_throwsException()
      throws CodecAlreadyRegisteredForTypeException {
    SerializationType mockSerializationType = PowerMockito.mock(SerializationType.class);
    codecRegistry.register(mockSerializationType, new DummyTypeCodec());

    boolean caughtException = false;
    try {
      codecRegistry.register(mockSerializationType, new DummyTypeCodec());
    } catch (CodecAlreadyRegisteredForTypeException e) {
      caughtException = true;
    }
    Assert.assertTrue("This was supposed to have thrown a CodecAlreadyRegisteredException",
        caughtException);
  }

  @Test
  public void testGetRegisteredCodec()
      throws CodecAlreadyRegisteredForTypeException, CodecNotRegisteredForTypeException {
    TypeCodec expectedCodec = new DummyTypeCodec();
    SerializationType mockSerializationType = PowerMockito.mock(SerializationType.class);
    codecRegistry.register(mockSerializationType, expectedCodec);
    TypeCodec codec = codecRegistry.getCodecForType(mockSerializationType);
    Assert.assertSame(expectedCodec, codec);
  }

  @Test(expected = CodecNotRegisteredForTypeException.class)
  public void testGetCodecForUnregisteredType_throwsException()
      throws CodecNotRegisteredForTypeException {
    SerializationType mockSerializationType = PowerMockito.mock(SerializationType.class);
    codecRegistry.getCodecForType(mockSerializationType);
  }

  class DummyTypeCodec implements TypeCodec {
    @Override
    public Object decode(byte[] incoming) {
      return null;
    }

    @Override
    public byte[] encode(Object incoming) {
      return new byte[0];
    }

    @Override
    public SerializationType getSerializationType() {
      return PowerMockito.mock(SerializationType.class);
    }
  }
}
