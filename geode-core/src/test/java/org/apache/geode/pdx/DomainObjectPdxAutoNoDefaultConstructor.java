/*
 * Licensed to the Apache Software Foundation (ASF) under one or more contributor license
 * agreements. See the NOTICE file distributed with this work for additional information regarding
 * copyright ownership. The ASF licenses this file to You under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance with the License. You may obtain a
 * copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software distributed under the License
 * is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express
 * or implied. See the License for the specific language governing permissions and limitations under
 * the License.
 */
package org.apache.geode.pdx;

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
