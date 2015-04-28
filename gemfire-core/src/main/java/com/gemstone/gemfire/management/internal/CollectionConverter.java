/*
 *  =========================================================================
 *  Copyright (c) 2002-2014 Pivotal Software, Inc. All Rights Reserved.
 * This product is protected by U.S. and international copyright
 * and intellectual property laws. Pivotal products are covered by
 * more patents listed at http://www.pivotal.io/patents.
 *  ========================================================================
 */
package com.gemstone.gemfire.management.internal;

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
 * @author rishim
 * 
 */
public final class CollectionConverter extends OpenTypeConverter {
  CollectionConverter(Type targetType, ArrayType openArrayType,
      Class openArrayClass, OpenTypeConverter elementConverter) {
    super(targetType, openArrayType, openArrayClass);
    this.elementConverter = elementConverter;

    /*
     * Determine the concrete class to be used when converting back to this Java
     * type. We convert all Lists to ArrayList and all Sets to TreeSet. (TreeSet
     * because it is a SortedSet, so works for both Set and SortedSet.)
     */
    Type raw = ((ParameterizedType) targetType).getRawType();
    Class c = (Class<?>) raw;
    if (c == List.class)
      collectionClass = ArrayList.class;
    else if (c == Set.class)
      collectionClass = HashSet.class;
    else if (c == SortedSet.class)
      collectionClass = TreeSet.class;
    else { // can't happen
      assert (false);
      collectionClass = null;
    }
  }

  final Object toNonNullOpenValue(Object value) throws OpenDataException {
    final Collection valueCollection = (Collection) value;
    if (valueCollection instanceof SortedSet) {
      Comparator comparator = ((SortedSet) valueCollection).comparator();
      if (comparator != null) {
        final String msg = "Cannot convert SortedSet with non-null comparator: "
            + comparator;
        throw openDataException(msg, new IllegalArgumentException(msg));
      }
    }
    final Object[] openArray = (Object[]) Array.newInstance(getOpenClass()
        .getComponentType(), valueCollection.size());
    int i = 0;
    for (Object o : valueCollection)
      openArray[i++] = elementConverter.toOpenValue(o);
    return openArray;
  }

  public final Object fromNonNullOpenValue(Object openValue)
      throws InvalidObjectException {
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
        final String msg = "Could not add " + o + " to "
            + collectionClass.getName() + " (duplicate set element?)";
        throw new InvalidObjectException(msg);
      }
    }
    return valueCollection;
  }

  void checkReconstructible() throws InvalidObjectException {
    elementConverter.checkReconstructible();
  }

  private final Class<? extends Collection> collectionClass;
  private final OpenTypeConverter elementConverter;
}