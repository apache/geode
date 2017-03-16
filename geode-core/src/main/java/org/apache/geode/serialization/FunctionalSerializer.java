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

package org.apache.geode.serialization;

public class FunctionalSerializer<T> implements Serializer<T>, Deserializer<T> {
  public final Serializer<T> serializer;
  public final Deserializer<T> deserializer;

  public FunctionalSerializer(Serializer<T> serializer, Deserializer<T> deserializer) {
    this.serializer = serializer;
    this.deserializer = deserializer;
  }

  @Override
  public T deserialize(byte[] bytes) {
    return deserializer.deserialize(bytes);
  }

  @Override
  public byte[] serialize(T item) {
    return serializer.serialize(item);
  }
}
