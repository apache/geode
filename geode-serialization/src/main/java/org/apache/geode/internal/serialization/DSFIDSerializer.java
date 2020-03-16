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

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

/**
 * A DSFIDSerializer is a serialization service that is able to serialize
 * and deserialize objects whose classes have been registered with it
 * using registerDSFID(). During serialization a DataSerializableFixedID
 * implementation will have its toData() method invoked and during deserialization
 * it will have its fromData() method invoked.
 * <p>
 * Use DSFIDSerializerFactory to construct a serialization service.
 */
public interface DSFIDSerializer {
  /**
   * Returns a plug-in object that can serialize Objects that are not handled by
   * DSFIDSerializer. The default implementation handles only DataSerializableFixedID.
   * In the context of Geode this will return a serializer that uses
   * InternalDataSerializer to write any object supported by Geode serialization
   * (PDX, DataSerializable, Java serializable, etc).
   */
  ObjectSerializer getObjectSerializer();

  /**
   * Returns a plug-in object that can deserialize Objects that are not handled by
   * DSFIDSerializer. The default implementation handles only DstaSerializableFixedID.
   * In the context of Geode this will return a deserializer that uses
   * InternalDataSerializer to read any object supported by Geode deserialization.
   * See DSFIDSerializerFactory#setObjectDeserializer
   */
  ObjectDeserializer getObjectDeserializer();

  /**
   * Register the constructor for a fixed ID class. Use this to register your
   * DataSerializableFixedID
   * classes so that deserialization knows how to instantiate them.
   */
  void registerDSFID(int dsfid, Class<?> dsfidClass);

  /**
   * Creates a DataSerializableFixedID or StreamableFixedID instance by deserializing it from the
   * data input. In general this is only used by deserialization internals since it doesn't
   * usually return a usable object.
   */
  Object create(int dsfid, DataInput in) throws IOException, ClassNotFoundException;



  /** create a context for serializaing an object */
  SerializationContext createSerializationContext(DataOutput dataOutput);

  /** create a context for deserializaing an object */
  DeserializationContext createDeserializationContext(DataInput dataInput);

  void write(BasicSerializable bs, DataOutput out) throws IOException;

  void writeDSFIDHeader(int dsfid, DataOutput out) throws IOException;

  void invokeToData(Object ds, DataOutput out) throws IOException;


  void invokeFromData(Object ds, DataInput in) throws IOException, ClassNotFoundException;

  int readDSFIDHeader(DataInput dis) throws IOException;

}
