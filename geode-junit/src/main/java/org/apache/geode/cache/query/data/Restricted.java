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
 * Restricted.java
 *
 * Created on October 4, 2005, 2:25 PM
 */

package org.apache.geode.cache.query.data;

import java.io.Serializable;


public class Restricted implements Serializable {
  public int cusip;
  public String quoteType;
  public String uniqueQuoteType;
  public double price;
  public int minQty;
  public int maxQty;
  public int incQty;

  /** Creates a new instance of Restricted */
  public Restricted(int i) {
    cusip = 1000000000 - i;
    String[] arr1 = {"moving", "binding", "non binding", "not to exceed", "storage",
        "auto transport", "mortgage"};
    quoteType = arr1[i % 7];
    uniqueQuoteType = "quoteType" + Integer.toString(i);
    price = (i / 10) * 8;
    minQty = i + 100;
    maxQty = i + 1000;
    if ((i % 12) == 0) {
      incQty = maxQty - minQty;
    } else {
      incQty = ((maxQty - minQty) / 12) * (i % 12);
    }
  }// end of constructor

  public int getCusip() {
    return cusip;
  }

  public String getQuoteType() {
    return quoteType;
  }

  public String getUniqueQuoteType() {
    return quoteType;
  }

  public int getMinQty() {
    return minQty;
  }

  public int getMaxQty() {
    return maxQty;
  }

  public double getPrice() {
    return price;
  }
}// end of Restricted
