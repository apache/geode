/*=========================================================================
 * Copyright (c) 2002-2014 Pivotal Software, Inc. All Rights Reserved.
 * This product is protected by U.S. and international copyright
 * and intellectual property laws. Pivotal products are covered by
 * more patents listed at http://www.pivotal.io/patents.
 *=========================================================================
 */
package com.gemstone.gemfire.internal.size;

import java.io.IOException;
import java.io.ObjectStreamException;
import java.io.Serializable;
import java.util.Map;
import java.util.Properties;

import com.gemstone.gemfire.cache.Declarable;
import com.gemstone.gemfire.cache.util.ObjectSizer;
import com.gemstone.gemfire.internal.util.concurrent.CopyOnWriteWeakHashMap;

/**
 * An implementation of {@link ObjectSizer} that calculates an accurate, in
 * memory size of for the first instance of each class that it sees. After the
 * first size calculation, it will return the same size for every instance of
 * that class.
 * 
 * This sizer is a compromise between generating accurate sizes for every object
 * and performance. It should work well for objects that are fairly constant
 * in size. For completely accurate sizing, use {@link ReflectionObjectSizer}
 * 
 * @author dsmith
 * 
 */
public class SizeClassOnceObjectSizer implements ObjectSizer, Serializable, Declarable {
  
  private static final SizeClassOnceObjectSizer INSTANCE = new SizeClassOnceObjectSizer();
  
  private transient final Map<Class, Integer> savedSizes = new CopyOnWriteWeakHashMap<Class, Integer>();
  
  private transient final ReflectionObjectSizer sizer = ReflectionObjectSizer.getInstance();
  
  public int sizeof(Object o) {
    if(o == null) {
      return 0;
    }
    int wellKnownObjectSize = WellKnownClassSizer.sizeof(o);
    if(wellKnownObjectSize != 0) {
      return wellKnownObjectSize;
    }
    
    //Now do the sizing
    Class clazz = o.getClass();
    Integer size = savedSizes.get(clazz);
    if(size == null) {
      size = Integer.valueOf(sizer.sizeof(o));
      savedSizes.put(clazz, size);
    }
    return size.intValue();
  }
  
  public static SizeClassOnceObjectSizer getInstance() {
    return INSTANCE;
  }
  
  //This object is serializable because EvictionAttributes is serializable
  //We want to resolve to the same singleton when deserializing
  private void writeObject(java.io.ObjectOutputStream out)
  throws IOException {
    
  }
  
  private void readObject(java.io.ObjectInputStream in)
    throws IOException, ClassNotFoundException {
  }
  
  private Object readResolve() throws ObjectStreamException {
    return INSTANCE;
  }

  private SizeClassOnceObjectSizer() {
    
  }

  public void init(Properties props) {
    // TODO Auto-generated method stub
    
  }
}
