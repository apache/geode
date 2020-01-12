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
import java.util.ArrayList;
import java.util.List;

public class Instrument implements Serializable {

  private String id;

  private List<TradingLine> tradingLines;

  public Instrument(String id) {
    this.id = id;
    this.tradingLines = new ArrayList<>();
  }

  public String getId() {
    return this.id;
  }

  public void addTradingLine(TradingLine tl) {
    this.tradingLines.add(tl);
  }

  // This method is needed for the query
  public List<TradingLine> getTradingLines() {
    return this.tradingLines;
  }

  public String toString() {
    return new StringBuilder().append(getClass().getSimpleName()).append("[").append("id=")
        .append(this.id).append("; tradingLines=").append(this.tradingLines).append("]").toString();
  }

  public static Instrument getInstrument(String id) {
    Instrument inst = new Instrument(id);
    for (int i = 0; i < 5; i++) {
      TradingLine tl = new TradingLine();
      tl.addAlternateReference("SOME_KEY", "SOME_VALUE");
      tl.addAlternateReference("SOME_OTHER_KEY", "SOME_OTHER_VALUE");
      inst.addTradingLine(tl);
    }
    return inst;
  }
}
