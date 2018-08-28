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

import java.util.HashMap;
import java.util.Map;


public class NestedPdx implements PdxSerializable {

  private String myString1;
  private long myLong;
  private Map<String, PdxSerializable> myHashMap = new HashMap<String, PdxSerializable>();
  private String myString2;
  private float myFloat;

  public NestedPdx() {}

  public NestedPdx(String str1, long myLong, HashMap<String, PdxSerializable> myMap, String str2,
      float myFloat) {
    this.myString1 = str1;
    this.myLong = myLong;
    this.myHashMap = myMap;
    this.myString2 = str2;
    this.myFloat = myFloat;
  }

  @Override
  public int hashCode() {
    final int prime = 31;
    int result = 1;
    result = prime * result + Float.floatToIntBits(myFloat);
    result = prime * result + ((myHashMap == null) ? 0 : myHashMap.hashCode());
    result = prime * result + (int) (myLong ^ (myLong >>> 32));
    result = prime * result + ((myString1 == null) ? 0 : myString1.hashCode());
    result = prime * result + ((myString2 == null) ? 0 : myString2.hashCode());
    return result;
  }

  @Override
  public boolean equals(Object obj) {
    if (this == obj)
      return true;
    if (obj == null)
      return false;
    if (getClass() != obj.getClass())
      return false;
    NestedPdx other = (NestedPdx) obj;
    if (Float.floatToIntBits(myFloat) != Float.floatToIntBits(other.myFloat))
      return false;
    if (myHashMap == null) {
      if (other.myHashMap != null)
        return false;
    } else if (!myHashMap.equals(other.myHashMap))
      return false;
    if (myLong != other.myLong)
      return false;
    if (myString1 == null) {
      if (other.myString1 != null)
        return false;
    } else if (!myString1.equals(other.myString1))
      return false;
    if (myString2 == null) {
      if (other.myString2 != null)
        return false;
    } else if (!myString2.equals(other.myString2))
      return false;
    return true;
  }

  public void toData(PdxWriter out) {
    out.writeString("myString1", this.myString1);
    out.writeLong("myLong", this.myLong);
    // out.writeObject("myHashMap", this.myHashMap);
    out.writeObject("myHashMap", this.myHashMap);
    out.writeString("myString2", this.myString2);
    out.writeFloat("myFloat", this.myFloat);
  }

  public void fromData(PdxReader in) {
    this.myString1 = in.readString("myString1");
    this.myLong = in.readLong("myLong");
    // this.myHashMap = (Map<String,
    // PdxSerializable>)in.readObject("myHashMap");
    this.myHashMap = (Map<String, PdxSerializable>) in.readObject("myHashMap");
    this.myString2 = in.readString("myString2");
    this.myFloat = in.readFloat("myFloat");
  }

  @Override
  public String toString() {
    return "NestedPdx [myString1=" + myString1 + ", myLong=" + myLong + ", myHashMap=" + myHashMap
        + ", myString2=" + myString2 + ", myFloat=" + myFloat + "]";
  }
}
