package com.gemstone.gemfire.internal.size;

public class SizeTestUtil {
  public static int OBJECT_SIZE=ReflectionSingleObjectSizer.OBJECT_SIZE;
  public static int REFERENCE_SIZE=ReflectionSingleObjectSizer.REFERENCE_SIZE;
  
  public static int roundup(int size) {
    //Rounds the size up to the next 8 byte boundary.
    return (int) (Math.ceil(size / 8.0) * 8);
  }
  
  private SizeTestUtil() {
    
  }

  
}
