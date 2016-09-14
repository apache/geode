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
package org.apache.geode.management.internal;

import java.io.InvalidObjectException;
import java.lang.reflect.Array;
import java.lang.reflect.GenericArrayType;
import java.lang.reflect.Type;

import javax.management.openmbean.ArrayType;
import javax.management.openmbean.OpenDataException;

/**
 * Converting an Array of Composite types
 * 
 * 
 */
public final class ArrayConverter extends OpenTypeConverter {
  ArrayConverter(Type targetType, ArrayType openArrayType,
      Class openArrayClass, OpenTypeConverter elementConverter) {
    super(targetType, openArrayType, openArrayClass);
    this.elementConverter = elementConverter;
  }

  final Object toNonNullOpenValue(Object value) throws OpenDataException {
    Object[] valueArray = (Object[]) value;
    final int len = valueArray.length;
    final Object[] openArray = (Object[]) Array.newInstance(getOpenClass()
        .getComponentType(), len);
    for (int i = 0; i < len; i++) {
      openArray[i] = elementConverter.toOpenValue(valueArray[i]);
    }
    return openArray;
  }

  public final Object fromNonNullOpenValue(Object openValue)
      throws InvalidObjectException {
    final Object[] openArray = (Object[]) openValue;
    final Type targetType = getTargetType();
    final Object[] valueArray;
    final Type componentType;
    if (targetType instanceof GenericArrayType) {
      componentType = ((GenericArrayType) targetType).getGenericComponentType();
    } else if (targetType instanceof Class && ((Class<?>) targetType).isArray()) {
      componentType = ((Class<?>) targetType).getComponentType();
    } else {
      throw new IllegalArgumentException("Not an array: " + targetType);
    }
    valueArray = (Object[]) Array.newInstance((Class<?>) componentType,
        openArray.length);
    for (int i = 0; i < openArray.length; i++) {
      valueArray[i] = elementConverter.fromOpenValue(openArray[i]);
    }
    return valueArray;
  }

  void checkReconstructible() throws InvalidObjectException {
    elementConverter.checkReconstructible();
  }

  /**
   * OpenTypeConverter for the elements of this array. If this is an array of
   * arrays, the converter converts the second-level arrays, not the deepest
   * elements.
   */
  private final OpenTypeConverter elementConverter;
}
