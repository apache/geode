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

import static org.junit.Assert.assertEquals;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;

import com.pholser.junit.quickcheck.Property;
import com.pholser.junit.quickcheck.runner.JUnitQuickcheck;
import org.junit.Before;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import org.apache.geode.DataSerializer;
import org.apache.geode.test.junit.categories.SerializationTest;

/**
 * Tests the serialization and deserialization of randomly generated Strings.
 *
 * The current implementation (0.7 or 0.8alpha2) of junit-quickcheck-generators only generates valid
 * codepoints, and that it doesn't tend to test strings that are particularly long, though the more
 * trials you run, the longer they get.
 */
@Category({SerializationTest.class})
@RunWith(JUnitQuickcheck.class)
public class InternalDataSerializerQuickcheckStringTest {
  @Property(trials = 1000)
  public void StringSerializedDeserializesToSameValue(String originalString) throws IOException {
    ByteArrayOutputStream byteArrayOutputStream = new ByteArrayOutputStream();
    DataOutputStream dataOutputStream = new DataOutputStream(byteArrayOutputStream);

    DataSerializer.writeString(originalString, dataOutputStream);
    dataOutputStream.flush();

    byte[] stringBytes = byteArrayOutputStream.toByteArray();
    DataInputStream dataInputStream = new DataInputStream(new ByteArrayInputStream(stringBytes));
    String returnedString = DataSerializer.readString(dataInputStream);

    assertEquals("Deserialized string matches original", originalString, returnedString);
  }

  @Before
  public void setUp() {
    // this may be unnecessary, but who knows what tests run before us.
    InternalDataSerializer.reinitialize();
  }
}
