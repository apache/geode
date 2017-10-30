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
package org.apache.geode.internal.protocol.serialization.registry;

import java.util.HashMap;
import java.util.ServiceLoader;

import org.apache.geode.GemFireConfigException;
import org.apache.geode.annotations.Experimental;
import org.apache.geode.internal.protocol.serialization.registry.exception.CodecAlreadyRegisteredForTypeException;
import org.apache.geode.internal.protocol.serialization.registry.exception.CodecNotRegisteredForTypeException;
import org.apache.geode.internal.protocol.serialization.SerializationType;
import org.apache.geode.internal.protocol.serialization.TypeCodec;

@Experimental
public class SerializationCodecRegistry {
  private final HashMap<SerializationType, TypeCodec> codecRegistry = new HashMap<>();

  public SerializationCodecRegistry() {
    ServiceLoader<TypeCodec> typeCodecs = ServiceLoader.load(TypeCodec.class);
    try {
      for (TypeCodec typeCodec : typeCodecs) {
        register(typeCodec.getSerializationType(), typeCodec);
      }
    } catch (CodecAlreadyRegisteredForTypeException ex) {
      throw new GemFireConfigException("Multiple implementations found for the same TypeCodec", ex);
    }
  }

  private void register(SerializationType serializationType, TypeCodec<?> typeCodec) {
    if (codecRegistry.containsKey(serializationType)) {
      throw new CodecAlreadyRegisteredForTypeException(
          "There is already a codec registered for type: " + serializationType);
    }
    codecRegistry.put(serializationType, typeCodec);
  }

  public int getRegisteredCodecCount() {
    return codecRegistry.size();
  }

  public TypeCodec getCodecForType(SerializationType serializationType)
      throws CodecNotRegisteredForTypeException {
    TypeCodec typeCodec = codecRegistry.get(serializationType);
    if (typeCodec == null) {
      throw new CodecNotRegisteredForTypeException(
          "There is no codec registered for type: " + serializationType);
    }
    return typeCodec;
  }

  public void shutdown() {
    codecRegistry.clear();
  }
}
