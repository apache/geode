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
package org.apache.geode.cache.query.data;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.io.Serializable;
import java.util.Date;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;

import org.apache.geode.DataSerializable;
import org.apache.geode.DataSerializer;
import org.apache.geode.internal.Assert;

/**
 * THIS FILE IS ENCODED IN UTF-8 IN ORDER TO TEST UNICODE IN FIELD NAMES. THE ENCODING MUST BE
 * SPECIFIED AS UTF-8 WHEN COMPILED
 */
public class Portfolio implements Serializable, DataSerializable {

  public static AtomicInteger instanceCount = new AtomicInteger(0);
  public int ID;
  public int indexKey;
  public String pkid;
  public Short shortID;
  public Position position1;
  public Position position2;
  public Position[] position3;
  public String description;
  public long createTime;
  public HashMap positions = new HashMap();
  public HashMap collectionHolderMap = new HashMap();
  String type;
  public String status;
  public String[] names = {"aaa", "bbb", "ccc", "ddd"};
  public String unicodeṤtring;
  private final long longMinValue = Long.MIN_VALUE;
  private final float floatMinValue = Float.MIN_VALUE;
  private final double doubleMinValue = Double.MIN_VALUE;
  public Date createDate;

  /*
   * public String getStatus(){ return status;
   */
  public int getID() {
    return ID;
  }

  public long getCreateTime() {
    return this.createTime;
  }

  public void setCreateTime(long time) {
    this.createTime = time;
  }

  public String getPk() {
    return pkid;
  }

  public HashMap getPositions() {
    return positions;
  }

  public HashMap getPositions(String str) {
    return positions;
  }

  public HashMap getPositions(Integer i) {
    return positions;
  }

  public HashMap getPositions(int i) {
    return positions;
  }

  public Position getP1() {
    return position1;
  }

  public Position getP2() {
    return position2;
  }

  public HashMap getCollectionHolderMap() {
    return collectionHolderMap;
  }

  public ComparableWrapper getCW(int x) {
    return new ComparableWrapper(x);
  }

  public boolean testMethod(boolean booleanArg) {
    return true;
  }

  public boolean isActive() {
    return status.equals("active");
  }

  public static String secIds[] = {"SUN", "IBM", "YHOO", "GOOG", "MSFT", "AOL", "APPL", "ORCL",
      "SAP", "DELL", "RHAT", "NOVL", "HP"};

  /* public no-arg constructor required for Deserializable */
  public Portfolio() {
    instanceCount.addAndGet(1);

  }

  public Portfolio(int i) {
    instanceCount.addAndGet(1);
    ID = i;
    if (i % 2 == 0) {
      description = null;
    } else {
      description = "XXXX";
    }
    pkid = "" + i;
    status = i % 2 == 0 ? "active" : "inactive";
    type = "type" + (i % 3);
    position1 = new Position(secIds[Position.cnt % secIds.length], Position.cnt * 1000);
    if (i % 2 != 0) {
      position2 = new Position(secIds[Position.cnt % secIds.length], Position.cnt * 1000);
    } else {
      position2 = null;
    }
    positions.put(secIds[Position.cnt % secIds.length],
        new Position(secIds[Position.cnt % secIds.length], Position.cnt * 1000));
    positions.put(secIds[Position.cnt % secIds.length],
        new Position(secIds[Position.cnt % secIds.length], Position.cnt * 1000));
    collectionHolderMap.put("0", new CollectionHolder());
    collectionHolderMap.put("1", new CollectionHolder());
    collectionHolderMap.put("2", new CollectionHolder());
    collectionHolderMap.put("3", new CollectionHolder());

    unicodeṤtring = i % 2 == 0 ? "ṤṶẐ" : "ṤẐṶ";
    Assert.assertTrue(unicodeṤtring.length() == 3);
  }

  public Portfolio(int i, int j) {
    this(i);
    this.position1.portfolioId = j;
    this.position3 = new Position[3];
    for (int k = 0; k < position3.length; k++) {
      Position p = new Position(secIds[k], (k + 1) * 1000);
      p.portfolioId = (k + 1);
      this.position3[k] = p;
    }
  }

  private boolean eq(Object o1, Object o2) {
    return o1 == null ? o2 == null : o1.equals(o2);
  }

  public boolean equals(Object o) {
    if (!(o instanceof Portfolio)) {
      return false;
    }
    Portfolio p2 = (Portfolio) o;
    return this.ID == p2.ID;
  }

  public int hashCode() {
    return this.ID;
  }


  public String toString() {
    String out =
        "Portfolio [ID=" + ID + " status=" + status + " type=" + type + " pkid=" + pkid + "\n ";
    Iterator iter = positions.entrySet().iterator();
    while (iter.hasNext()) {
      Map.Entry entry = (Map.Entry) iter.next();
      out += entry.getKey() + ":" + entry.getValue() + ", ";
    }
    out += "\n P1:" + position1 + ", P2:" + position2;
    return out + "\n]";
  }

  /**
   * Getter for property type.S
   *
   * @return Value of property type.
   */
  public String getType() {
    return this.type;
  }

  public boolean boolFunction(String strArg) {
    return "active".equals(strArg);
  } // added by vikramj

  public int intFunction(int j) {
    return j;
  } // added by vikramj

  public String funcReturnSecId(Object o) {
    return ((Position) o).getSecId();
  }// added by vikramj

  public long longFunction(long j) {
    return j;
  }

  public float getFloatMinValue() {
    return this.floatMinValue;
  }

  public float getLongMinValue() {
    return this.longMinValue;
  }

  public double getDoubleMinValue() {
    return this.doubleMinValue;
  }

  public void throwExceptionMethod() {
    throw new IllegalStateException();
  }

  public void fromData(DataInput in) throws IOException, ClassNotFoundException {
    this.ID = in.readInt();
    boolean isNull = DataSerializer.readPrimitiveBoolean(in);
    if (!isNull) {
      this.shortID = DataSerializer.readShort(in);
    }
    this.pkid = DataSerializer.readString(in);

    this.position1 = (Position) DataSerializer.readObject(in);
    this.position2 = (Position) DataSerializer.readObject(in);
    this.positions = (HashMap) DataSerializer.readObject(in);
    this.collectionHolderMap = (HashMap) DataSerializer.readObject(in);
    this.type = DataSerializer.readString(in);
    this.status = DataSerializer.readString(in);
    this.names = DataSerializer.readStringArray(in);
    this.description = DataSerializer.readString(in);
    this.createTime = DataSerializer.readPrimitiveLong(in);
    this.createDate = DataSerializer.readDate(in);
    // Read Position3
    int position3Size = in.readInt();
    if (position3Size != 0) {
      this.position3 = new Position[position3Size];
      for (int i = 0; i < position3Size; i++) {
        this.position3[i] = (Position) DataSerializer.readObject(in);

      }
    }
    this.indexKey = in.readInt();
  }

  public void toData(DataOutput out) throws IOException {
    out.writeInt(this.ID);
    if (this.shortID == null) {
      DataSerializer.writePrimitiveBoolean(true, out);
    } else {
      DataSerializer.writePrimitiveBoolean(false, out);
      DataSerializer.writeShort(this.shortID, out);
    }

    DataSerializer.writeString(this.pkid, out);
    DataSerializer.writeObject(this.position1, out);
    DataSerializer.writeObject(this.position2, out);
    DataSerializer.writeObject(this.positions, out);
    DataSerializer.writeObject(this.collectionHolderMap, out);
    DataSerializer.writeString(this.type, out);
    DataSerializer.writeString(this.status, out);
    DataSerializer.writeStringArray(this.names, out);
    DataSerializer.writeString(this.description, out);
    DataSerializer.writePrimitiveLong(this.createTime, out);
    DataSerializer.writeDate(createDate, out);
    // Write Position3.
    if (this.position3 == null) {
      out.writeInt(0);
    } else {
      out.writeInt(this.position3.length);
      for (int i = 0; i < position3.length; i++) {
        DataSerializer.writeObject(this.position3[i], out);
      }
    }
    out.writeInt(this.indexKey);
  }

  public static void resetInstanceCount() {
    instanceCount.set(0);
  }

}
