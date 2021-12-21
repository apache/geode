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
package org.apache.geode.cache.query.dunit;


import java.io.Serializable;
import java.util.HashSet;
import java.util.Set;

import org.apache.geode.pdx.PdxReader;
import org.apache.geode.pdx.PdxSerializable;
import org.apache.geode.pdx.PdxWriter;


public class PositionPdxVersion implements Serializable, PdxSerializable, Comparable {
  private long avg20DaysVol = 0;
  private String bondRating;
  private double convRatio;
  private String country;
  private double delta;
  private long industry;
  private long issuer;
  private double mktValue;
  private double qty;
  public String secId;
  private String secLinks;
  public String secType;
  private double sharesOutstanding;
  public String underlyer;
  private long volatility;
  private int pid;
  public static int cnt = 0;
  public int portfolioId = 0;


  /* public no-arg constructor required for DataSerializable */
  public PositionPdxVersion() {}

  public PositionPdxVersion(String id, double out) {
    secId = id;
    sharesOutstanding = out;
    secType = "a";
    pid = cnt++;
    mktValue = cnt;
  }

  @Override
  public boolean equals(Object o) {
    if (!(o instanceof PositionPdxVersion)) {
      return false;
    }
    return secId.equals(((PositionPdxVersion) o).secId);
  }

  @Override
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
    return getClass().getName() + "[secId=" + secId + " out=" + sharesOutstanding
        + " type=" + secType + " id=" + pid + " mktValue=" + mktValue + "]";
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

  @Override
  public void fromData(PdxReader in) {
    avg20DaysVol = in.readLong("avg20DaysVol");
    bondRating = in.readString("bondRating");
    convRatio = in.readDouble("convRatio");
    country = in.readString("country");
    delta = in.readDouble("delta");
    industry = in.readLong("industry");
    issuer = in.readLong("issuer");
    mktValue = in.readDouble("mktValue");
    qty = in.readDouble("qty");
    secId = in.readString("secId");
    secLinks = in.readString("secLinks");
    sharesOutstanding = in.readDouble("sharesOutstanding");
    underlyer = in.readString("underlyer");
    volatility = in.readLong("volatility");
    pid = in.readInt("pid");
    portfolioId = in.readInt("portfolioId");
  }

  @Override
  public void toData(PdxWriter out) {
    out.writeLong("avg20DaysVol", avg20DaysVol);
    out.writeString("bondRating", bondRating);
    out.writeDouble("convRatio", convRatio);
    out.writeString("country", country);
    out.writeDouble("delta", delta);
    out.writeLong("industry", industry);
    out.writeLong("issuer", issuer);
    out.writeDouble("mktValue", mktValue);
    out.writeDouble("qty", qty);
    out.writeString("secId", secId);
    out.writeString("secLinks", secLinks);
    out.writeDouble("outStanding", sharesOutstanding);
    out.writeString("underLyer", underlyer);
    out.writeLong("volatility", volatility);
    out.writeInt("pid", pid);
    out.writeInt("portfolioId", portfolioId);
  }


  @Override
  public int compareTo(Object o) {
    if (o == this) {
      return 0;
    } else {
      return pid < ((PositionPdxVersion) o).pid ? -1 : 1;
    }

  }

}
