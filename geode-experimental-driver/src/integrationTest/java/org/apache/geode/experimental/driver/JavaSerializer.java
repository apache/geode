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
package org.apache.geode.experimental.driver;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;

import com.google.protobuf.ByteString;

import org.apache.geode.cache.Cache;

public class JavaSerializer
    implements ValueSerializer, org.apache.geode.protocol.serialization.ValueSerializer {
  public JavaSerializer() {}

  @Override
  public ByteString serialize(Object object) throws IOException {
    ByteArrayOutputStream byteArrayOutputStream = new ByteArrayOutputStream();
    ObjectOutputStream objectStream = new ObjectOutputStream(byteArrayOutputStream);
    objectStream.writeObject(object);
    objectStream.flush();

    return ByteString.copyFrom(byteArrayOutputStream.toByteArray());
  }

  @Override
  public Object deserialize(ByteString bytes) throws IOException, ClassNotFoundException {
    return new ObjectInputStream(new ByteArrayInputStream(bytes.toByteArray())).readObject();
  }

  @Override
  public void init(Cache cache) {

  }

  @Override
  public String getID() {
    return "JAVA";
  }

  @Override
  public boolean supportsPrimitives() {
    return false;
  }

}
