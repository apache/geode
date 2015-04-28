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

import com.gemstone.gemfire.InternalGemFireError;
import com.gemstone.gemfire.cache.Cache;
import com.gemstone.gemfire.cache.Region;
import com.gemstone.gemfire.cache.util.ObjectSizer;
import com.gemstone.gemfire.internal.cache.PlaceHolderDiskRegion;
import com.gemstone.gemfire.internal.size.ObjectGraphSizer.ObjectFilter;

/**
 * An implementation of {@link ObjectSizer} that calculates an accurate, in
 * memory size of for each object that it sizes. This is the slowest method of
 * calculating sizes, but it should accurately reflect the amount of heap memory
 * used for objects.
 * 
 * This class will traverse all objects that are reachable from the passed in
 * object by instance fields. So use this class with caution if you have
 * instance fields that refer to shared objects.
 * 
 * For objects that are all approximately the same size, consider using
 * {@link SizeClassOnceObjectSizer}
 * 
 * @author dsmith
 * 
 */
public class ReflectionObjectSizer implements ObjectSizer, Serializable {
  
  private static final ReflectionObjectSizer INSTANCE = new ReflectionObjectSizer();
  
  private static final ObjectFilter FILTER = new ObjectFilter() {

    public boolean accept(Object parent, Object object) {
      //Protect the user from a couple of pitfalls. If their object
      //has a link to a region or cache, we don't want to size the whole thing.
      if (object instanceof Region || object instanceof Cache
          || object instanceof PlaceHolderDiskRegion) {
        return false;
      }
      return true;
    }
    
  };

  public int sizeof(Object o) {
    try {
      return (int) ObjectGraphSizer.size(o, FILTER, false);
    } catch (IllegalArgumentException e) {
      throw new InternalGemFireError(e);
    } catch (IllegalAccessException e) {
      throw new InternalGemFireError(e);
    }
  }
  
  public static ReflectionObjectSizer getInstance() {
    return INSTANCE;
  }
  
  private void writeObject(java.io.ObjectOutputStream out)
  throws IOException {
  }
  
  private void readObject(java.io.ObjectInputStream in)
       throws IOException, ClassNotFoundException {
  }
  
  private Object readResolve() throws ObjectStreamException {
    return INSTANCE;
  }
  
  private ReflectionObjectSizer() {
    
  }

}
