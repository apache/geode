/*=========================================================================
 * Copyright (c) 2010-2014 Pivotal Software, Inc. All Rights Reserved.
 * This product is protected by U.S. and international copyright
 * and intellectual property laws. Pivotal products are covered by
 * one or more patents listed at http://www.pivotal.io/patents.
 *=========================================================================
 */
package com.gemstone.gemfire.internal.cache.persistence.soplog;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;

import junit.framework.TestCase;

import com.gemstone.gemfire.DataSerializer;
import com.gemstone.gemfire.internal.cache.persistence.soplog.SortedReader.SerializedComparator;

public abstract class ComparisonTestCase extends TestCase {
  public enum Comparison {
    EQ(0.0),
    LT(-1.0),
    GT(1.0);
    
    private final double sgn;
    
    private Comparison(double sgn) {
      this.sgn = sgn;
    }
    
    public double signum() {
      return sgn;
    }
    
    public Comparison opposite() {
      switch (this) {
      case LT: return GT;
      case GT : return LT;
      default : return EQ;
      }
    }
  }
  
  public void compare(SerializedComparator sc, Object o1, Object o2, Comparison result) throws IOException {
    double diff = sc.compare(convert(o1), convert(o2));
    assertEquals(String.format("%s ? %s", o1, o2), result.signum(), Math.signum(diff));
  }
  
  public void compareAsIs(SerializedComparator sc, byte[] b1, byte[] b2, Comparison result) throws IOException {
    double diff = sc.compare(b1, b2);
    assertEquals(result.signum(), Math.signum(diff));
  }
  
  public static byte[] convert(Object o) throws IOException {
    ByteArrayOutputStream b = new ByteArrayOutputStream();
    DataOutputStream d = new DataOutputStream(b);
    DataSerializer.writeObject(o, d);
    
    return b.toByteArray();
  }
  
  public static Object recover(byte[] obj, int off, int len) throws ClassNotFoundException, IOException {
    DataInputStream in = new DataInputStream(new ByteArrayInputStream(obj, off, len));
    return DataSerializer.readObject(in);
  }
}
