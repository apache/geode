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
import java.util.HashSet;
import java.util.Set;

import org.apache.geode.DataSerializable;
import org.apache.geode.DataSerializer;


public class Position implements Serializable, DataSerializable, Comparable {
  private long avg20DaysVol = 0;
  private String bondRating;
  private double convRatio;
  private String country;
  private double delta;
  private long industry;
  private long issuer;
  public double mktValue;
  private double qty;
  public String secId;
  public String secIdIndexed;
  private String secLinks;
  public String secType;
  private double sharesOutstanding;
  public String underlyer;
  private long volatility;
  private int pid;
  public static int cnt = 0;
  public int portfolioId = 0;

  /* public no-arg constructor required for DataSerializable */
  public Position() {}

  public Position(String id, double out) {
    secId = id;
    secIdIndexed = secId;
    sharesOutstanding = out;
    secType = "a";
    pid = cnt++;
    mktValue = cnt;
  }

  public boolean equals(Object o) {
    if (!(o instanceof Position)) {
      return false;
    }
    return secId.equals(((Position) o).secId);
  }

  public int hashCode() {
    return secId.hashCode();
  }


  public static void resetCounter() {
    cnt = 0;
  }

  public double getMktValue() {
    return mktValue;
  }

  public String getSecId() {
    return secId;
  }

  public int getId() {
    return pid;
  }

  public double getSharesOutstanding() {
    return sharesOutstanding;
  }

  public String toString() {
    return "Position [secId=" + secId + " out=" + sharesOutstanding + " type="
        + secType + " id=" + pid + " mktValue=" + mktValue + "]";
  }

  public Set getSet(int size) {
    Set set = new HashSet();
    for (int i = 0; i < size; i++) {
      set.add("" + i);
    }
    return set;
  }

  public Set getCol() {
    Set set = new HashSet();
    for (int i = 0; i < 2; i++) {
      set.add("" + i);
    }
    return set;
  }

  public int getPid() {
    return pid;
  }

  @Override
  public void fromData(DataInput in) throws IOException, ClassNotFoundException {
    avg20DaysVol = in.readLong();
    bondRating = DataSerializer.readString(in);
    convRatio = in.readDouble();
    country = DataSerializer.readString(in);
    delta = in.readDouble();
    industry = in.readLong();
    issuer = in.readLong();
    mktValue = in.readDouble();
    qty = in.readDouble();
    secId = DataSerializer.readString(in);
    secIdIndexed = DataSerializer.readString(in);
    secLinks = DataSerializer.readString(in);
    sharesOutstanding = in.readDouble();
    underlyer = DataSerializer.readString(in);
    volatility = in.readLong();
    pid = in.readInt();
    portfolioId = in.readInt();
  }

  @Override
  public void toData(DataOutput out) throws IOException {
    out.writeLong(avg20DaysVol);
    DataSerializer.writeString(bondRating, out);
    out.writeDouble(convRatio);
    DataSerializer.writeString(country, out);
    out.writeDouble(delta);
    out.writeLong(industry);
    out.writeLong(issuer);
    out.writeDouble(mktValue);
    out.writeDouble(qty);
    DataSerializer.writeString(secId, out);
    DataSerializer.writeString(secIdIndexed, out);
    DataSerializer.writeString(secLinks, out);
    out.writeDouble(sharesOutstanding);
    DataSerializer.writeString(underlyer, out);
    out.writeLong(volatility);
    out.writeInt(pid);
    out.writeInt(portfolioId);
  }


  @Override
  public int compareTo(Object o) {
    if (o == this) {
      return 0;
    } else {
      if (pid == ((Position) o).pid) {
        return 0;
      } else {
        return pid < ((Position) o).pid ? -1 : 1;
      }
    }

  }

}
