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
/*
 * Quote.java
 *
 * Created on October 4, 2005, 1:57 PM
 */

package org.apache.geode.cache.query.data;

import java.io.Serializable;
import java.util.HashSet;
import java.util.Set;

public class Quote implements Serializable {
  public int cusip;
  public int quoteId;
  public String quoteIdStr;
  public String quoteType;
  public String uniqueQuoteType;
  public String dealerPortfolio;
  public int dealerCode;
  public String channelName;
  public String priceType;
  public double price;
  public int lowerQty;
  public int upperQty;
  public int ytm;


  // later added by Prafulla
  public Set restrict = new HashSet();

  /** Creates a new instance of Quote */
  public Quote(int i) {
    cusip = 1000000000 - i;
    quoteId = i;
    quoteIdStr = Integer.toString(quoteId);
    dealerCode = cusip;
    String[] arr1 = {"moving", "binding", "non binding", "not to exceed", "storage",
        "auto transport", "mortgage"};
    quoteType = arr1[i % 7];
    uniqueQuoteType = "quoteType" + Integer.toString(i);
    String[] arr2 = {"dealer1", "dealer2", "dealer3", "dealer4", "dealer5", "dealer6", "dealer7"};
    dealerPortfolio = arr2[i % 7];
    String[] arr3 =
        {"channel1", "channel2", "channel3", "channel4", "channel5", "channel6", "channel7",};
    channelName = arr3[i % 7];
    String[] arr4 = {"priceType1", "priceType2", "priceType3", "priceType4", "priceType5",
        "priceType6", "priceType7"};
    priceType = arr4[i % 7];
    price = (i / 10) * 8;
    lowerQty = i + 100;
    upperQty = i + 1000;
    if ((i % 12) == 0) {
      ytm = upperQty - lowerQty;
    } else {
      ytm = ((upperQty - lowerQty) / 12) * (i % 12);
    }

    restrict.add(new Restricted(i));
  }// end of constructor

  public int getCusip() {
    return cusip;
  }

  public String getQuoteIdStr() {
    return quoteIdStr;
  }

  public String getQuoteType() {
    return quoteType;
  }

  public String getUniqueQuoteType() {
    return uniqueQuoteType;
  }

  public String getDealerPortfolio() {
    return dealerPortfolio;
  }

  public String getChannelName() {
    return channelName;
  }

  public int getLowerQty() {
    return lowerQty;
  }

  public int getUpperQty() {
    return upperQty;
  }

  public double getPrice() {
    return price;
  }

}// end of Quote
