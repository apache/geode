package com.gemstone.gemfire.cache.lucene.internal.repository.serializer;

import java.io.Serializable;

/**
 * A test type to get mapped to a lucene document
 */
public class Type1 implements Serializable {
  private static final long serialVersionUID = 1L;

  public static final String[] fields = new String[] {"s", "i", "l", "d", "f"};
  
  String s;
  int i;
  long l;
  double d;
  float f;
  Serializable o = new Serializable() {
    private static final long serialVersionUID = 1L;
  };
  
  public Type1(String s, int i, long l, double d, float f) {
    this.s = s;
    this.i = i;
    this.l = l;
    this.d = d;
    this.f = f;
  }
}