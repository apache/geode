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
package org.apache.geode.security.query.data;

import java.io.Serializable;

import org.apache.geode.pdx.PdxReader;
import org.apache.geode.pdx.PdxSerializable;
import org.apache.geode.pdx.PdxWriter;

public class PdxTrade implements PdxSerializable, Serializable {

  private String id;

  private String cusip;

  private int shares;

  private int price;

  public PdxTrade() {}

  public String getCusip() {
    return this.cusip;
  }

  public PdxTrade(String id, String cusip, int shares, int price) {
    this.id = id;
    this.cusip = cusip;
    this.shares = shares;
    this.price = price;
  }

  @Override
  public void toData(PdxWriter writer) {
    writer.writeString("id", id);
    writer.writeString("cusip", cusip);
    writer.writeInt("shares", shares);
    writer.writeInt("price", price);
  }

  @Override
  public void fromData(PdxReader reader) {
    id = reader.readString("id");
    cusip = reader.readString("cusip");
    shares = reader.readInt("shares");
    price = reader.readInt("price");
  }
}
