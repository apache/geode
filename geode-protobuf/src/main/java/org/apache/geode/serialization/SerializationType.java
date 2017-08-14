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

import org.apache.geode.annotations.Experimental;
import org.apache.geode.pdx.PdxInstance;

/**
 * Enumerates the serialization types currently available to wire protocols.
 */
@Experimental
public enum SerializationType {
  STRING(String.class),
  BINARY(byte[].class),
  INT(int.class),
  BYTE(byte.class),
  SHORT(short.class),
  LONG(long.class),
  JSON(PdxInstance.class),
  BOOLEAN(boolean.class),
  FLOAT(float.class),
  DOUBLE(double.class);

  public final Class klass;

  SerializationType(Class klass) {
    this.klass = klass;
  }
}
