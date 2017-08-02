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
package org.apache.geode.protocol.protobuf.utilities;

import org.apache.geode.protocol.protobuf.utilities.exception.UnknownProtobufPrimitiveType;

public enum ProtobufPrimitiveTypes {

  STRING(String.class),
  INT(Integer.class),
  LONG(Long.class),
  SHORT(Short.class),
  BYTE(Byte.class),
  BOOLEAN(Boolean.class),
  DOUBLE(Double.class),
  FLOAT(Float.class),
  BINARY(byte[].class);

  private Class clazzType;

  ProtobufPrimitiveTypes(Class clazz) {
    this.clazzType = clazz;
  }

  public static ProtobufPrimitiveTypes valueOf(Class unencodedValueClass)
      throws UnknownProtobufPrimitiveType {
    for (ProtobufPrimitiveTypes protobufPrimitiveTypes : values()) {
      if (protobufPrimitiveTypes.clazzType.equals(unencodedValueClass)) {
        return protobufPrimitiveTypes;
      }
    }
    throw new UnknownProtobufPrimitiveType(
        "There is no primitive protobuf type mapping for class:" + unencodedValueClass);
  }
}
