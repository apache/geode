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
import java.util.HashMap;
import java.util.Properties;

import org.apache.geode.cache.Declarable;

public class PortfolioData implements Declarable, Serializable {

  int ID;

  public String pkid;

  public HashMap collectionHolderMap = new HashMap();

  String type;

  public String status;

  public String[] names = {"aaa", "bbb", "ccc", "ddd"};

  /*
   * public String getStatus(){ return status;
   */
  public int getID() {
    return ID;
  }

  public String getPk() {
    return pkid;
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

  public PortfolioData() {

  }

  public PortfolioData(int i) {
    ID = i;
    pkid = "" + i;
    status = i % 2 == 0 ? "active" : "inactive";
    type = "type" + (i % 3);

    collectionHolderMap.put("0", new CollectionHolder());
    collectionHolderMap.put("1", new CollectionHolder());
    collectionHolderMap.put("2", new CollectionHolder());
    collectionHolderMap.put("3", new CollectionHolder());
  }

  public String toString() {
    String out =
        "Portfolio [ID=" + ID + " status=" + status + " type=" + type + " pkid=" + pkid + "\n ";

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

  public int hashCode() {
    return this.ID | this.pkid.hashCode() | this.type.hashCode();
  }

  public boolean equals(Object p) {
    if (this == p) {
      return true;
    }
    if (p == null || !(p instanceof PortfolioData)) {
      return false;
    }
    PortfolioData pd = (PortfolioData) p;
    if ((this.ID == pd.ID) && this.pkid.equals(pd.pkid) && (this.type.equals(pd.type))) {
      return true;
    } else {
      return false;
    }
  }

  public void init(Properties props) {
    this.ID = Integer.parseInt(props.getProperty("id"));
    this.type = props.getProperty("type", "type1");
    this.status = props.getProperty("status", "active");

  }

}
