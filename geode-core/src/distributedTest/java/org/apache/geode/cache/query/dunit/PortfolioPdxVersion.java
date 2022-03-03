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

import java.util.HashMap;

import org.apache.geode.cache.query.data.CollectionHolder;
import org.apache.geode.internal.Assert;
import org.apache.geode.pdx.PdxInstance;
import org.apache.geode.pdx.PdxInstanceFactory;


public class PortfolioPdxVersion {

  private final int ID;
  public String pkid;
  public PositionPdxVersion position1;
  public PositionPdxVersion position2;
  public Object[] position3;
  public String description;
  public long createTime;
  public HashMap positions = new HashMap();
  public HashMap collectionHolderMap = new HashMap();
  String type;
  public String status;
  public String[] names = {"aaa", "bbb", "ccc", "ddd"};
  public String unicodeṤtring;

  public static String[] secIds = {"SUN", "IBM", "YHOO", "GOOG", "MSFT", "AOL", "APPL", "ORCL",
      "SAP", "DELL", "RHAT", "NOVL", "HP"};

  public PortfolioPdxVersion(int i) {
    ID = i;
    if (i % 2 == 0) {
      description = null;
    } else {
      description = "XXXX";
    }
    pkid = "" + i;
    status = i % 2 == 0 ? "active" : "inactive";
    type = "type" + (i % 3);
    position1 = new PositionPdxVersion(secIds[PositionPdxVersion.cnt % secIds.length],
        PositionPdxVersion.cnt * 1000L);
    if (i % 2 != 0) {
      position2 = new PositionPdxVersion(secIds[PositionPdxVersion.cnt % secIds.length],
          PositionPdxVersion.cnt * 1000L);
    } else {
      position2 = null;
    }

    positions.put(secIds[PositionPdxVersion.cnt % secIds.length], new PositionPdxVersion(
        secIds[PositionPdxVersion.cnt % secIds.length], PositionPdxVersion.cnt * 1000L));
    positions.put(secIds[PositionPdxVersion.cnt % secIds.length], new PositionPdxVersion(
        secIds[PositionPdxVersion.cnt % secIds.length], PositionPdxVersion.cnt * 1000L));

    collectionHolderMap.put("0", new CollectionHolder());
    collectionHolderMap.put("1", new CollectionHolder());

    unicodeṤtring = i % 2 == 0 ? "ṤṶẐ" : "ṤẐṶ";
    Assert.assertTrue(unicodeṤtring.length() == 3);
  }

  public PortfolioPdxVersion(int i, int j) {
    this(i);
    position1.portfolioId = j;
    position3 = new Object[3];
    for (int k = 0; k < position3.length; k++) {
      PositionPdxVersion p = new PositionPdxVersion(secIds[k], (k + 1) * 1000L);
      p.portfolioId = (k + 1);
      position3[k] = p;
    }
  }

  public PdxInstance createPdxInstance(PdxInstanceFactory pdxFactory) {
    pdxFactory.writeInt("ID", ID);
    pdxFactory.writeString("pkid", pkid);
    pdxFactory.writeObject("position1", position1);
    pdxFactory.writeObject("position2", position2);
    pdxFactory.writeObject("positions", positions);
    pdxFactory.writeObject("collectionHolderMap", collectionHolderMap);
    pdxFactory.writeString("type", type);
    pdxFactory.writeString("status", status);
    pdxFactory.writeStringArray("names", names);
    pdxFactory.writeString("description", description);
    pdxFactory.writeLong("createTime", createTime);
    pdxFactory.writeObjectArray("position3", position3);

    return pdxFactory.create();

  }
}
