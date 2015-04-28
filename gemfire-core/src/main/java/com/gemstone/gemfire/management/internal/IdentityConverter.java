/*
 *  =========================================================================
 *  Copyright (c) 2002-2014 Pivotal Software, Inc. All Rights Reserved.
 * This product is protected by U.S. and international copyright
 * and intellectual property laws. Pivotal products are covered by
 * more patents listed at http://www.pivotal.io/patents.
 *  ========================================================================
 */
package com.gemstone.gemfire.management.internal;

import java.lang.reflect.Type;

import javax.management.openmbean.OpenType;

/**
 * Converter for classes where the open data is identical to the original
 * data. This is true for any of the SimpleType types, and for an
 * any-dimension array of those
 * 
 * @author rishim
 *
 */
public final class IdentityConverter extends OpenTypeConverter {
  IdentityConverter(Type targetType, OpenType openType, Class openClass) {
    super(targetType, openType, openClass);
  }

  boolean isIdentity() {
    return true;
  }

  final Object toNonNullOpenValue(Object value) {
    return value;
  }

  public final Object fromNonNullOpenValue(Object value) {
    return value;
  }
}
