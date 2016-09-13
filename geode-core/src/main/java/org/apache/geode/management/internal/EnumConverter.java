/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.gemstone.gemfire.management.internal;

import java.io.InvalidObjectException;

import javax.management.openmbean.SimpleType;

/**
 * Open type converter for Enums.
 * 
 * 
 */
public final class EnumConverter<T extends Enum<T>> extends OpenTypeConverter {

  EnumConverter(Class<T> enumClass) {
    super(enumClass, SimpleType.STRING, String.class);
    this.enumClass = enumClass;
  }

  final Object toNonNullOpenValue(Object value) {
    return ((Enum) value).name();
  }

  public final Object fromNonNullOpenValue(Object value)
      throws InvalidObjectException {
    try {
      return Enum.valueOf(enumClass, (String) value);
    } catch (Exception e) {
      throw invalidObjectException("Cannot convert to enum: " + value, e);
    }
  }

  private final Class<T> enumClass;
}
