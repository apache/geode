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
package org.apache.geode.internal.serialization;

import org.apache.geode.internal.serialization.internal.DSFIDSerializerImpl;

/**
 * DSFIDSerializerFactory can be used to create a serialization service.
 * You may establish an overriding ObjectSerializer and ObjectDeserializer
 * to handle types of serialization other than DataSerializableFixedID and
 * BasicSerializable. Geode-core does this and adds a large number of other
 * types of serialization including PDX and DataSerializable.
 * <p>
 * Once a DSFIDSerializer is created you should register your
 * DataSerializableFixedID classes with it using registerDSFID(). You can
 * find examples of doing this in other modules by looking for uses of this
 * method.
 */
public class DSFIDSerializerFactory {

  private ObjectSerializer serializer;
  private ObjectDeserializer deserializer;

  /**
   * Replaces the default serializer with the given serializer.
   * Typically the given ObjectSerializer will defer most serialization of
   * DataSerializableFixedID objects to the DSFIDSerializer but may handle serialization
   * of other types of objects in its readObject/writeObject methods.
   */
  public DSFIDSerializerFactory setObjectSerializer(ObjectSerializer serializer) {
    this.serializer = serializer;
    return this;
  }

  /**
   * Replaces the default deserializer with the given deserializer.
   * Typically the given ObjectDeserializer will defer most deserialization of
   * DataSerializableFixedID objects to the DSFIDSerializer but may handle deserialization
   * of other types of objects in its readObject/writeObject methods.
   */
  public DSFIDSerializerFactory setObjectDeserializer(ObjectDeserializer deserializer) {
    this.deserializer = deserializer;
    return this;
  }

  /** Create a DSFIDSerializer */
  public DSFIDSerializer create() {
    if (serializer == null && deserializer == null) {
      return new DSFIDSerializerImpl();
    } else {
      return new DSFIDSerializerImpl(serializer, deserializer);
    }
  }

}
