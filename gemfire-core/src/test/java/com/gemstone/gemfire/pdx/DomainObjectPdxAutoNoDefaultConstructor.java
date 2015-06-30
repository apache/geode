/*=========================================================================
 * Copyright (c) 2010-2014 Pivotal Software, Inc. All Rights Reserved.
 * This product is protected by U.S. and international copyright
 * and intellectual property laws. Pivotal products are covered by
 * one or more patents listed at http://www.pivotal.io/patents.
 *=========================================================================
 */
package com.gemstone.gemfire.pdx;

import java.util.Collection;
import java.util.Date;
import java.util.Map;

public class DomainObjectPdxAutoNoDefaultConstructor extends DomainObject {
  
  protected String string_immediate;
  
  public Integer anInteger;

  public enum Day {
    SUNDAY, MONDAY, TUESDAY, WEDNESDAY, THURSDAY, FRIDAY, SATURDAY 
  }

  public char aChar;
  public boolean aBoolean;
  public byte aByte;
  public short aShort;
  public int anInt;
  public long aLong;
  public float aFloat;
  public double aDouble;
  public Date aDate;
  public String aString;
  public Object anObject;
  public Map aMap;
  public Collection aCollection;
  public boolean[] aBooleanArray;
  public char[] aCharArray;
  public byte[] aByteArray;
  public short[] aShortArray;
  public int[] anIntArray;
  public long[] aLongArray;
  public float[] aFloatArray;
  public double[] aDoubleArray;
  public String[] aStringArray;
  public Object[] anObjectArray;
  public byte[][] anArrayOfByteArray;

  // No zero-arg constructor here...

  public DomainObjectPdxAutoNoDefaultConstructor(int size) {
    super(size);
  }
}
