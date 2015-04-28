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
import java.lang.reflect.Type;
import java.util.Collection;
import java.util.Comparator;
import java.util.Map;
import java.util.SortedMap;

import javax.management.openmbean.CompositeData;
import javax.management.openmbean.CompositeDataSupport;
import javax.management.openmbean.CompositeType;
import javax.management.openmbean.OpenDataException;
import javax.management.openmbean.TabularData;
import javax.management.openmbean.TabularDataSupport;
import javax.management.openmbean.TabularType;

/**
 * Table type converter
 * 
 * @author rishim
 * 
 */
public final class TableConverter extends OpenTypeConverter {
  TableConverter(Type targetType, boolean sortedMap, TabularType tabularType,
      OpenTypeConverter keyConverter, OpenTypeConverter valueConverter) {
    super(targetType, tabularType, TabularData.class);
    this.sortedMap = sortedMap;
    this.keyConverter = keyConverter;
    this.valueConverter = valueConverter;
  }

  final Object toNonNullOpenValue(Object value) throws OpenDataException {
    final Map<Object, Object> valueMap = (Map<Object, Object>) value;
    if (valueMap instanceof SortedMap) {
      Comparator comparator = ((SortedMap) valueMap).comparator();
      if (comparator != null) {
        final String msg = "Cannot convert SortedMap with non-null comparator: "
            + comparator;
        IllegalArgumentException iae = new IllegalArgumentException(msg);
        OpenDataException ode = new OpenDataException(msg);
        ode.initCause(iae);
        throw ode;
      }
    }
    final TabularType tabularType = (TabularType) getOpenType();
    final TabularData table = new TabularDataSupport(tabularType);
    final CompositeType rowType = tabularType.getRowType();
    for (Map.Entry entry : valueMap.entrySet()) {
      final Object openKey = keyConverter.toOpenValue(entry.getKey());
      final Object openValue = valueConverter.toOpenValue(entry.getValue());
      final CompositeData row;
      row = new CompositeDataSupport(rowType, keyValueArray, new Object[] {
          openKey, openValue });
      table.put(row);
    }
    return table;
  }

  public final Object fromNonNullOpenValue(Object openValue)
      throws InvalidObjectException {
    final TabularData table = (TabularData) openValue;
    final Collection<CompositeData> rows = (Collection<CompositeData>) table
        .values();
    final Map<Object, Object> valueMap = sortedMap ? OpenTypeUtil
        .newSortedMap() : OpenTypeUtil.newMap();
    for (CompositeData row : rows) {
      final Object key = keyConverter.fromOpenValue(row.get("key"));
      final Object value = valueConverter.fromOpenValue(row.get("value"));
      if (valueMap.put(key, value) != null) {
        final String msg = "Duplicate entry in TabularData: key=" + key;
        throw new InvalidObjectException(msg);
      }
    }
    return valueMap;
  }

  void checkReconstructible() throws InvalidObjectException {
    keyConverter.checkReconstructible();
    valueConverter.checkReconstructible();
  }

  private final boolean sortedMap;
  private final OpenTypeConverter keyConverter;
  private final OpenTypeConverter valueConverter;
}