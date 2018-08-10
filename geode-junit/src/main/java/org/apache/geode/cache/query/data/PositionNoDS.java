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

import java.io.Serializable;
import java.util.HashSet;
import java.util.Set;


public class PositionNoDS implements Serializable, Comparable {
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
  public PositionNoDS() {}

  public PositionNoDS(String id, double out) {
    secId = id;
    secIdIndexed = secId;
    sharesOutstanding = out;
    secType = "a";
    pid = cnt++;
    this.mktValue = cnt;
  }

  public boolean equals(Object o) {
    if (!(o instanceof PositionNoDS))
      return false;
    return this.secId.equals(((PositionNoDS) o).secId);
  }

  public int hashCode() {
    return this.secId.hashCode();
  }


  public static void resetCounter() {
    cnt = 0;
  }

  public double getMktValue() {
    return this.mktValue;
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
    return "Position [secId=" + this.secId + " out=" + this.sharesOutstanding + " type="
        + this.secType + " id=" + this.pid + " mktValue=" + this.mktValue + "]";
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



  public int compareTo(Object o) {
    if (o == this) {
      return 0;
    } else {
      if (this.pid == ((PositionNoDS) o).pid)
        return 0;
      else
        return this.pid < ((PositionNoDS) o).pid ? -1 : 1;
    }

  }

}
