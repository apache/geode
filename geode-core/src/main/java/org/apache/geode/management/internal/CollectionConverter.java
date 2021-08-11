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
package org.apache.geode.management.internal;

import java.io.InvalidObjectException;
import java.lang.reflect.Array;
import java.lang.reflect.ParameterizedType;
import java.lang.reflect.Type;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Comparator;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.SortedSet;
import java.util.TreeSet;

import javax.management.openmbean.ArrayType;
import javax.management.openmbean.OpenDataException;

/**
 * Open type converter for Collections
 *
 *
 */
public class CollectionConverter extends OpenTypeConverter {
  CollectionConverter(Type targetType, ArrayType openArrayType, Class openArrayClass,
      OpenTypeConverter elementConverter) {
    super(targetType, openArrayType, openArrayClass);
    this.elementConverter = elementConverter;

    /*
     * Determine the concrete class to be used when converting back to this Java type. We convert
     * all Lists to ArrayList and all Sets to TreeSet. (TreeSet because it is a SortedSet, so works
     * for both Set and SortedSet.)
     */
    Type raw = ((ParameterizedType) targetType).getRawType();
    Class c = (Class<?>) raw;
    if (c == List.class) {
      collectionClass = ArrayList.class;
    } else if (c == Set.class) {
      collectionClass = HashSet.class;
    } else if (c == SortedSet.class) {
      collectionClass = TreeSet.class;
    } else { // can't happen
      assert (false);
      collectionClass = null;
    }
  }

  @Override
  Object toNonNullOpenValue(Object value) throws OpenDataException {
    final Collection valueCollection = (Collection) value;
    if (valueCollection instanceof SortedSet) {
      Comparator comparator = ((SortedSet) valueCollection).comparator();
      if (comparator != null) {
        final String msg = "Cannot convert SortedSet with non-null comparator: " + comparator;
        throw openDataException(msg, new IllegalArgumentException(msg));
      }
    }
    final Object[] openArray =
        (Object[]) Array.newInstance(getOpenClass().getComponentType(), valueCollection.size());
    int i = 0;
    for (Object o : valueCollection) {
      openArray[i++] = elementConverter.toOpenValue(o);
    }
    return openArray;
  }

  @Override
  public Object fromNonNullOpenValue(Object openValue) throws InvalidObjectException {
    final Object[] openArray = (Object[]) openValue;
    final Collection<Object> valueCollection;
    try {
      valueCollection = collectionClass.newInstance();
    } catch (Exception e) {
      throw invalidObjectException("Cannot create collection", e);
    }
    for (Object o : openArray) {
      Object value = elementConverter.fromOpenValue(o);
      if (!valueCollection.add(value)) {
        final String msg =
            "Could not add " + o + " to " + collectionClass.getName() + " (duplicate set element?)";
        throw new InvalidObjectException(msg);
      }
    }
    return valueCollection;
  }

  @Override
  void checkReconstructible() throws InvalidObjectException {
    elementConverter.checkReconstructible();
  }

  private final Class<? extends Collection> collectionClass;
  private final OpenTypeConverter elementConverter;
}
