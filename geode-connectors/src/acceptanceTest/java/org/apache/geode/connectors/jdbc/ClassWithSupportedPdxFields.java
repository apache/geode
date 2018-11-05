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

package org.apache.geode.connectors.jdbc;

import java.io.Serializable;
import java.util.Arrays;
import java.util.Date;

import org.apache.geode.internal.PdxSerializerObject;

public class ClassWithSupportedPdxFields implements PdxSerializerObject, Serializable {
  private String id;
  private boolean aboolean;
  private byte aByte;
  private short ASHORT;
  private int anint;
  private long along;
  private float afloat;
  private double adouble;
  private String astring;
  private Date adate;
  private Object anobject;
  private byte[] abytearray;
  private char achar;

  public ClassWithSupportedPdxFields() {}

  public ClassWithSupportedPdxFields(String id, boolean aboolean, byte aByte, short ASHORT,
      int anint,
      long along, float afloat, double adouble, String astring, Date adate, Object anobject,
      byte[] abytearray, char achar) {
    this.id = id;
    this.aboolean = aboolean;
    this.aByte = aByte;
    this.ASHORT = ASHORT;
    this.anint = anint;
    this.along = along;
    this.afloat = afloat;
    this.adouble = adouble;
    this.astring = astring;
    this.adate = adate;
    this.anobject = anobject;
    this.abytearray = abytearray;
    this.achar = achar;
  }

  public ClassWithSupportedPdxFields(String id) {
    this.id = id;
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }
    if (o == null || getClass() != o.getClass()) {
      return false;
    }

    ClassWithSupportedPdxFields that = (ClassWithSupportedPdxFields) o;

    if (getId() != null ? !getId().equals(that.getId())
        : that.getId() != null) {
      return false;
    }
    if (isAboolean() != that.isAboolean()) {
      return false;
    }
    if (getAbyte() != that.getAbyte()) {
      return false;
    }
    if (getAshort() != that.getAshort()) {
      return false;
    }
    if (getAnint() != that.getAnint()) {
      return false;
    }
    if (getAlong() != that.getAlong()) {
      return false;
    }
    if (Float.compare(that.getAfloat(), getAfloat()) != 0) {
      return false;
    }
    if (Double.compare(that.getAdouble(), getAdouble()) != 0) {
      return false;
    }
    if (getAstring() != null ? !getAstring().equals(that.getAstring())
        : that.getAstring() != null) {
      return false;
    }
    if (getAdate() != null ? !getAdate().equals(that.getAdate()) : that.getAdate() != null) {
      return false;
    }
    if (getAnobject() != null ? !getAnobject().equals(that.getAnobject())
        : that.getAnobject() != null) {
      return false;
    }
    if (getAchar() != that.getAchar()) {
      return false;
    }
    return Arrays.equals(getAbytearray(), that.getAbytearray());
  }

  @Override
  public int hashCode() {
    int result;
    long temp;
    result = (isAboolean() ? 1 : 0);
    result = 31 * result + (int) getAbyte();
    result = 31 * result + (int) getAshort();
    result = 31 * result + getAnint();
    result = 31 * result + (int) (getAlong() ^ (getAlong() >>> 32));
    result = 31 * result + (getAfloat() != +0.0f ? Float.floatToIntBits(getAfloat()) : 0);
    temp = Double.doubleToLongBits(getAdouble());
    result = 31 * result + (int) (temp ^ (temp >>> 32));
    result = 31 * result + (getAstring() != null ? getAstring().hashCode() : 0);
    result = 31 * result + (getAdate() != null ? getAdate().hashCode() : 0);
    result = 31 * result + (getAnobject() != null ? getAnobject().hashCode() : 0);
    result = 31 * result + Arrays.hashCode(getAbytearray());
    return result;
  }

  @Override
  public String toString() {
    return "ClassWithSupportedPdxFields{" + "id=" + getId() + ", aboolean=" + isAboolean()
        + ", aByte=" + getAbyte()
        + ", achar=" + getAchar() + ", ASHORT=" + getAshort() + ", anint=" + getAnint() + ", along="
        + getAlong() + ", afloat=" + getAfloat() + ", adouble=" + getAdouble() + ", astring='"
        + getAstring() + '\'' + ", adate=" + getAdate() + ", anobject=" + getAnobject()
        + ", abytearray=" + Arrays.toString(getAbytearray()) + '}';
  }

  public String getId() {
    return id;
  }

  public boolean isAboolean() {
    return aboolean;
  }

  public byte getAbyte() {
    return aByte;
  }

  public char getAchar() {
    return achar;
  }

  public short getAshort() {
    return ASHORT;
  }

  public int getAnint() {
    return anint;
  }

  public long getAlong() {
    return along;
  }

  public float getAfloat() {
    return afloat;
  }

  public double getAdouble() {
    return adouble;
  }

  public String getAstring() {
    return astring;
  }

  public Date getAdate() {
    return adate;
  }

  public Object getAnobject() {
    return anobject;
  }

  public byte[] getAbytearray() {
    return abytearray;
  }
}
