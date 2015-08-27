package com.gemstone.gemfire.cache.lucene.internal.repository.serializer;

/**
 * A test type to get mapped to a lucene document
 */
public class Type2 extends Type1 {
  private String s2;

  public Type2(String s, int i, long l, double d, float f, String s2) {
    super(s, i, l, d, f);
    this.s2=s2;
  }
  
  
}