package com.gemstone.gemfire.internal.size;

public interface SingleObjectSizer {
  
  /**
   * Returns the size of the object, WITHOUT descending into child objects
   * includes primatives and object references.
   * @param object
   */
  long sizeof(Object object);

}
