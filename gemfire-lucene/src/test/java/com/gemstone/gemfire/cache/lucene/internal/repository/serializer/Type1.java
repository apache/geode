package com.gemstone.gemfire.cache.lucene.internal.repository.serializer;

/**
 * A test type to get mapped to a lucene document
 */
public class Type1 {
  private String s;
  private int i;
  private long l;
  private double d;
  private float f;
  private Object o = new Object();
  
  public Type1(String s, int i, long l, double d, float f) {
    super();
    this.s = s;
    this.i = i;
    this.l = l;
    this.d = d;
    this.f = f;
  }
}