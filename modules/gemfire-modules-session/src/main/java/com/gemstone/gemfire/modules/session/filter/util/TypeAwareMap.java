/*=========================================================================
 * Copyright (c) 2010-2014 Pivotal Software, Inc. All Rights Reserved.
 * This product is protected by U.S. and international copyright
 * and intellectual property laws. Pivotal products are covered by
 * one or more patents listed at http://www.pivotal.io/patents.
 *=========================================================================
 */
/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */

package com.gemstone.gemfire.modules.session.filter.util;

import com.gemstone.gemfire.modules.session.common.CacheProperty;

import java.util.HashMap;

/**
 *
 */
public class TypeAwareMap<K extends CacheProperty, Object> extends HashMap {

  private Class<K> keyType;

  public TypeAwareMap(Class<K> keyType) {
    super();
    this.keyType = keyType;
  }

  public Object put(K key, Object value) {
    if (!key.getClazz().isAssignableFrom(value.getClass())) {
      if (key.getClazz() == Boolean.class) {
        return (Object) super.put(key, Boolean.valueOf((String) value));
      } else if (key.getClazz() == Integer.class) {
        return (Object) super.put(key, Integer.valueOf((String) value));
      } else {
        throw new IllegalArgumentException("Value is not of type " +
            key.getClazz().getName());
      }
    }

    return (Object) super.put(key, value);
  }
}
