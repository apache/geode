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

import java.util.Arrays;
import java.util.Collection;
import java.util.Date;
import java.util.Map;

public class DomainObjectPdxAuto extends DomainObject {

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
  public Day anEnum;
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

  public DomainObjectPdxAuto() {}

  public DomainObjectPdxAuto(int size) {
    super(size);
  }

  @Override
  public int hashCode() {
    final int prime = 31;
    int result = super.hashCode();
    result = prime * result + (aBoolean ? 1231 : 1237);
    result = prime * result + Arrays.hashCode(aBooleanArray);
    result = prime * result + aByte;
    result = prime * result + Arrays.hashCode(aByteArray);
    result = prime * result + aChar;
    result = prime * result + Arrays.hashCode(aCharArray);
    result = prime * result + ((aCollection == null) ? 0 : aCollection.hashCode());
    result = prime * result + ((aDate == null) ? 0 : aDate.hashCode());
    long temp;
    temp = Double.doubleToLongBits(aDouble);
    result = prime * result + (int) (temp ^ (temp >>> 32));
    result = prime * result + Arrays.hashCode(aDoubleArray);
    result = prime * result + Float.floatToIntBits(aFloat);
    result = prime * result + Arrays.hashCode(aFloatArray);
    result = prime * result + (int) (aLong ^ (aLong >>> 32));
    result = prime * result + Arrays.hashCode(aLongArray);
    result = prime * result + ((aMap == null) ? 0 : aMap.hashCode());
    result = prime * result + aShort;
    result = prime * result + Arrays.hashCode(aShortArray);
    result = prime * result + ((aString == null) ? 0 : aString.hashCode());
    result = prime * result + Arrays.hashCode(aStringArray);
    result = prime * result + Arrays.hashCode(anArrayOfByteArray);
    result = prime * result + ((anEnum == null) ? 0 : anEnum.hashCode());
    result = prime * result + anInt;
    result = prime * result + Arrays.hashCode(anIntArray);
    result = prime * result + ((anInteger == null) ? 0 : anInteger.hashCode());
    result = prime * result + ((anObject == null) ? 0 : anObject.hashCode());
    result = prime * result + Arrays.hashCode(anObjectArray);
    result = prime * result + ((string_immediate == null) ? 0 : string_immediate.hashCode());
    return result;
  }

  @Override
  public boolean equals(Object obj) {
    if (this == obj)
      return true;
    if (!super.equals(obj))
      return false;
    if (getClass() != obj.getClass())
      return false;
    DomainObjectPdxAuto other = (DomainObjectPdxAuto) obj;
    if (aBoolean != other.aBoolean)
      return false;
    if (!Arrays.equals(aBooleanArray, other.aBooleanArray))
      return false;
    if (aByte != other.aByte)
      return false;
    if (!Arrays.equals(aByteArray, other.aByteArray))
      return false;
    if (aChar != other.aChar)
      return false;
    if (!Arrays.equals(aCharArray, other.aCharArray))
      return false;
    if (aCollection == null) {
      if (other.aCollection != null)
        return false;
    } else if (!aCollection.equals(other.aCollection))
      return false;
    if (aDate == null) {
      if (other.aDate != null)
        return false;
    } else if (!aDate.equals(other.aDate))
      return false;
    if (Double.doubleToLongBits(aDouble) != Double.doubleToLongBits(other.aDouble))
      return false;
    if (!Arrays.equals(aDoubleArray, other.aDoubleArray))
      return false;
    if (Float.floatToIntBits(aFloat) != Float.floatToIntBits(other.aFloat))
      return false;
    if (!Arrays.equals(aFloatArray, other.aFloatArray))
      return false;
    if (aLong != other.aLong)
      return false;
    if (!Arrays.equals(aLongArray, other.aLongArray))
      return false;
    if (aMap == null) {
      if (other.aMap != null)
        return false;
    } else if (!aMap.equals(other.aMap))
      return false;
    if (aShort != other.aShort)
      return false;
    if (!Arrays.equals(aShortArray, other.aShortArray))
      return false;
    if (aString == null) {
      if (other.aString != null)
        return false;
    } else if (!aString.equals(other.aString))
      return false;
    if (!Arrays.equals(aStringArray, other.aStringArray))
      return false;
    if (!Arrays.equals(anArrayOfByteArray, other.anArrayOfByteArray))
      return false;
    if (anEnum != other.anEnum)
      return false;
    if (anInt != other.anInt)
      return false;
    if (!Arrays.equals(anIntArray, other.anIntArray))
      return false;
    if (anInteger == null) {
      if (other.anInteger != null)
        return false;
    } else if (!anInteger.equals(other.anInteger))
      return false;
    if (anObject == null) {
      if (other.anObject != null)
        return false;
    } else if (!anObject.equals(other.anObject))
      return false;
    if (!Arrays.equals(anObjectArray, other.anObjectArray))
      return false;
    if (string_immediate == null) {
      if (other.string_immediate != null)
        return false;
    } else if (!string_immediate.equals(other.string_immediate))
      return false;
    return true;
  }
}
