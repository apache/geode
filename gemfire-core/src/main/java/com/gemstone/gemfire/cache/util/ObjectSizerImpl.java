package com.gemstone.gemfire.cache.util;

/**
 * This class provides an implementation of the ObjectSizer interface. This
 * uses a helper class to provide the size of an object. The implementation uses
 * reflection to compute object size. This is to be used for testing purposes only
 * This implementation is slow and may cause throughput numbers to drop if used on
 * complex objects.
 *
 * @author Sudhir Menon
 *
 * @deprecated use {@link ObjectSizer#DEFAULT} instead.
 */
public class ObjectSizerImpl implements ObjectSizer {

  public int sizeof( Object o ) {
    return ObjectSizer.DEFAULT.sizeof(o);
  }
}
