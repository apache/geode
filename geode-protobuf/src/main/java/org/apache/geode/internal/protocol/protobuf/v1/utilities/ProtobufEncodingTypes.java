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
package org.apache.geode.internal.protocol.protobuf.v1.utilities;

import org.apache.geode.annotations.Experimental;
import org.apache.geode.internal.protocol.protobuf.v1.utilities.exception.UnknownProtobufEncodingType;
import org.apache.geode.pdx.PdxInstance;

/**
 * Maps classes to encoding for protobuf.
 *
 * This currently conflates object type with serialization, which may be an issue if we add more
 * types of object serialization.
 */
@Experimental
public enum ProtobufEncodingTypes {

  STRING(String.class),
  INT(Integer.class),
  LONG(Long.class),
  SHORT(Short.class),
  BYTE(Byte.class),
  BOOLEAN(Boolean.class),
  DOUBLE(Double.class),
  FLOAT(Float.class),
  BINARY(byte[].class),

  // This will probably have to change once the protocol supports multiple object encodings.
  PDX_OBJECT(PdxInstance.class);

  private Class clazz;

  ProtobufEncodingTypes(Class clazz) {
    this.clazz = clazz;
  }

  public static ProtobufEncodingTypes valueOf(Class unencodedValueClass)
      throws UnknownProtobufEncodingType {
    for (ProtobufEncodingTypes protobufEncodingTypes : values()) {
      if (protobufEncodingTypes.clazz.equals(unencodedValueClass)) {
        return protobufEncodingTypes;
      }
    }
    throw new UnknownProtobufEncodingType(
        "There is no primitive protobuf type mapping for class:" + unencodedValueClass);
  }
}
