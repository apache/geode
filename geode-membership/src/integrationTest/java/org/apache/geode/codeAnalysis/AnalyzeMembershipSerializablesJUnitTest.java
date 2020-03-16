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
package org.apache.geode.codeAnalysis;

import java.io.ByteArrayInputStream;
import java.io.DataInputStream;
import java.io.IOException;

import org.junit.experimental.categories.Category;

import org.apache.geode.distributed.internal.membership.gms.Services;
import org.apache.geode.internal.serialization.BufferDataOutputStream;
import org.apache.geode.internal.serialization.DSFIDSerializer;
import org.apache.geode.internal.serialization.DSFIDSerializerFactory;
import org.apache.geode.test.junit.categories.MembershipTest;


/**
 * AnalyzeMembershipSerializablesJUnitTest analyzes the DataSerializableFixedID and
 * BasicSerializable implementations in the membership package. It does not test
 * java Serializable objects because the DSFIDSerializer that is used by default in the
 * membership module does not support java Serializables.
 */
@Category({MembershipTest.class})
public class AnalyzeMembershipSerializablesJUnitTest extends AnalyzeDataSerializablesJUnitTestBase {

  private final DSFIDSerializer dsfidSerializer = new DSFIDSerializerFactory().create();

  @Override
  protected String getModuleName() {
    return "geode-membership";
  }

  @Override
  protected Class getModuleClass() {
    return Services.class;
  }

  @Override
  protected void deserializeObject(BufferDataOutputStream outputStream)
      throws IOException, ClassNotFoundException {
    dsfidSerializer.getObjectDeserializer()
        .readObject(new DataInputStream(new ByteArrayInputStream(outputStream.toByteArray())));
  }

  @Override
  protected void serializeObject(Object object, BufferDataOutputStream outputStream)
      throws IOException {
    dsfidSerializer.getObjectSerializer().writeObject(object, outputStream);
  }

  @Override
  protected void initializeSerializationService() {
    Services.registerSerializables(dsfidSerializer);
  }
}
