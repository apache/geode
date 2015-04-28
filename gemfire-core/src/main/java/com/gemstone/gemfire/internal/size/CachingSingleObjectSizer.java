/*=========================================================================
 * Copyright (c) 2002-2014 Pivotal Software, Inc. All Rights Reserved.
 * This product is protected by U.S. and international copyright
 * and intellectual property laws. Pivotal products are covered by
 * more patents listed at http://www.pivotal.io/patents.
 *=========================================================================
 */
package com.gemstone.gemfire.internal.size;

import java.util.HashMap;
import java.util.Map;

import com.gemstone.gemfire.internal.util.concurrent.CopyOnWriteWeakHashMap;

/**
 * @author dsmith
 *
 */
public class CachingSingleObjectSizer implements SingleObjectSizer {
  private final Map<Class, Long> sizeCache = new CopyOnWriteWeakHashMap<Class, Long>();
  private final SingleObjectSizer wrappedSizer;

  public CachingSingleObjectSizer(SingleObjectSizer sizer) {
    this.wrappedSizer = sizer;
  }
  public long sizeof(Object object) {
    Class clazz = object.getClass();
    if(clazz.isArray()) {
      return wrappedSizer.sizeof(object);
    }
    else {
      Long size = sizeCache.get(clazz);
      if(size != null) {
        return size.longValue();
      }
      size = Long.valueOf(wrappedSizer.sizeof(object));
      sizeCache.put(clazz, size);
      return size.longValue();
    }
  }

}
