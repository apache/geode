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

import org.apache.geode.internal.Assert;

public class PortfolioNoDS implements Serializable {

  public static AtomicInteger instanceCount = new AtomicInteger(0);
  public int ID;
  public int indexKey;
  public String pkid;
  public Short shortID;
  public PositionNoDS position1;
  public PositionNoDS position2;
  public PositionNoDS[] position3;
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

  public PositionNoDS getP1() {
    return position1;
  }

  public PositionNoDS getP2() {
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
  public PortfolioNoDS() {
    instanceCount.addAndGet(1);

  }

  public PortfolioNoDS(int i) {
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
    position1 = new PositionNoDS(secIds[PositionNoDS.cnt % secIds.length], PositionNoDS.cnt * 1000);
    if (i % 2 != 0) {
      position2 =
          new PositionNoDS(secIds[PositionNoDS.cnt % secIds.length], PositionNoDS.cnt * 1000);
    } else {
      position2 = null;
    }
    positions.put(secIds[PositionNoDS.cnt % secIds.length],
        new PositionNoDS(secIds[PositionNoDS.cnt % secIds.length], PositionNoDS.cnt * 1000));
    positions.put(secIds[PositionNoDS.cnt % secIds.length],
        new PositionNoDS(secIds[PositionNoDS.cnt % secIds.length], PositionNoDS.cnt * 1000));
    collectionHolderMap.put("0", new CollectionHolder());
    collectionHolderMap.put("1", new CollectionHolder());
    collectionHolderMap.put("2", new CollectionHolder());
    collectionHolderMap.put("3", new CollectionHolder());

    unicodeṤtring = i % 2 == 0 ? "ṤṶẐ" : "ṤẐṶ";
    Assert.assertTrue(unicodeṤtring.length() == 3);
  }

  public PortfolioNoDS(int i, int j) {
    this(i);
    this.position1.portfolioId = j;
    this.position3 = new PositionNoDS[3];
    for (int k = 0; k < position3.length; k++) {
      PositionNoDS p = new PositionNoDS(secIds[k], (k + 1) * 1000);
      p.portfolioId = (k + 1);
      this.position3[k] = p;
    }
  }

  private boolean eq(Object o1, Object o2) {
    return o1 == null ? o2 == null : o1.equals(o2);
  }

  public boolean equals(Object o) {
    if (!(o instanceof PortfolioNoDS)) {
      return false;
    }
    PortfolioNoDS p2 = (PortfolioNoDS) o;
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
    return ((PositionNoDS) o).getSecId();
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

  public static void resetInstanceCount() {
    instanceCount.set(0);
  }

}
