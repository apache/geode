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

import javax.management.openmbean.SimpleType;

/**
 * Open type converter for Enums.
 * 
 * @author rishim
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
