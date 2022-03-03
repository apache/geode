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
package parReg.query.unittest;

import java.io.Serializable;
import java.util.List;
import java.util.Properties;
import java.util.Random;

import org.apache.geode.cache.Cache;
import org.apache.geode.cache.Declarable;

/**
 * Represents a number of shares of a stock ("security") held in a {@link NewPortfolio}.
 *
 * <P>
 *
 * This class is <code>Serializable</code> because we want it to be distributed to multiple members
 * of a distributed system. Because this class is <code>Declarable</code>, we can describe instances
 * of it in a GemFire <code>cache.xml</code> file.
 *
 * @since GemFire 4.0
 */
public class Position implements Declarable, Serializable, Comparable {

  private static final Random rng = new Random();

  protected String secId;
  protected double qty;
  protected double mktValue;
  private final int NUM_OF_SECURITIES = 200;
  private final int MAX_PRICE = 100;

  @Override
  public void initialize(Cache cache, Properties props) {
    secId = props.getProperty("secId");
    qty = Double.parseDouble(props.getProperty("qty"));
    mktValue = Double.parseDouble(props.getProperty("mktValue"));
  }

  /**
   * Returns the id of the security held in this position.
   */
  public String getSecId() {
    return secId;
  }

  /**
   * Returns the number of shares held in this position.
   */
  public double getQty() {
    return qty;
  }

  /**
   * Returns the value of this position.
   */
  public double getMktValue() {
    return mktValue;
  }

  public String toString() {
    return "Position [secId=" + secId + " qty=" + qty + " mktValue=" + mktValue + "]";
  }

  public static String toString(List aList) {
    StringBuilder aStr = new StringBuilder();
    aStr.append("List of size " + aList.size() + "\n");
    for (Object anObj : aList) {
      if (anObj instanceof Position) {
        Position p = (Position) (anObj);
        aStr.append(p);
      }
      aStr.append("\n");
    }
    return aStr.toString();
  }

  /**
   * To enable the comparison.
   */
  public boolean equals(Object anObj) {
    if (anObj == null) {
      return false;
    }
    if (anObj.getClass() == getClass()) {
      Position pos = (Position) anObj;
      if ((pos.mktValue != mktValue) || (pos.qty != qty)) {
        return false;
      }

      if (pos.secId == null) {
        return secId == null;
      } else {
        return pos.secId.equals(secId);
      }
    } else {

      return false;
    }
  }

  public int hashCode() {
    int result = 17;
    result = 37 * result
        + (int) (Double.doubleToLongBits(mktValue) ^ (Double.doubleToLongBits(mktValue) >>> 32));
    result =
        37 * result + (int) (Double.doubleToLongBits(qty) ^ (Double.doubleToLongBits(qty) >>> 32));
    result = 37 * result + secId.hashCode();

    return result;
  }

  /**
   * to configure position using index, set quantity equal to the index
   */
  public void init(int i) {
    secId = new Integer(rng.nextInt(NUM_OF_SECURITIES)).toString();
    qty = (double) i;
    mktValue = rng.nextDouble() * MAX_PRICE;
  }

  public int getIndex() {
    return (int) qty;
  }

  public void validate(int index) {}

  @Override
  public int compareTo(Object o) {
    if (o == this) {
      return 0;
    } else {
      if (o instanceof Position) {
        return Integer.valueOf(hashCode())
            .compareTo(o.hashCode());
      } else {
        return -1;
      }
    }
  }
}
