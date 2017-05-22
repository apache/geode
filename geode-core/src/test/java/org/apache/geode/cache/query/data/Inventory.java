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
import java.sql.Timestamp;

/*
 * author: Prafulla Chaudhari
 */

public class Inventory implements Serializable {

  public String cusip;// CHAR(9)
  public String dealer_code;// VARCHAR(10)
  public String price_type;// CHAR(3)
  public double quote_price;// DOUBLE
  public Timestamp quote_timestamp;// TIMESTAMP
  public int min_order_qty;// INTEGER
  public int max_order_qty;// INTEGER
  public int lower_qty;// INTEGER
  public int upper_qty;// INTEGER
  public int inc_order_qty;// INTEGER
  public int retail_price;// INTEGER
  public String is_benchmark_flag;// CHAR(1)
  public double yield_spread;// DOUBLE
  public String treasury_cusip;// VARCHAR(9)


  //////// constructor of class Inventory

  protected String[] tempArr;
  protected int i = 0;
  protected String tempStr;
  protected int tempInt;
  protected double tempDouble;

  public Inventory(String inputStr) {
    tempArr = inputStr.split(",");

    cusip = tempArr[i++].replaceAll("\"", " ").trim();// CHAR(9)
    dealer_code = tempArr[i++].replaceAll("\"", " ").trim();// VARCHAR(10)
    price_type = tempArr[i++].replaceAll("\"", " ").trim();// CHAR(3)
    quote_price = (Double.valueOf(tempArr[i++].replaceAll("\"", " ").trim())).doubleValue();// DOUBLE

    tempStr = tempArr[i++];
    if (!tempStr.equalsIgnoreCase("NULL")) {
      quote_timestamp = Timestamp.valueOf(tempStr.replaceAll("\"", " ").trim());// TIMESTAMP
    }

    tempStr = tempArr[i++];
    if (tempStr.equalsIgnoreCase("NULL")) {
      tempInt = 0;
    } else {
      tempInt = (Integer.valueOf(tempStr.replaceAll("\"", " ").trim())).intValue();
    }
    min_order_qty = tempInt;// INTEGER

    tempStr = tempArr[i++];
    if (tempStr.equalsIgnoreCase("NULL")) {
      tempInt = 0;
    } else {
      tempInt = (Integer.valueOf(tempStr.replaceAll("\"", " ").trim())).intValue();
    }
    max_order_qty = tempInt;// INTEGER

    tempStr = tempArr[i++];
    if (tempStr.equalsIgnoreCase("NULL")) {
      tempInt = 0;
    } else {
      tempInt = (Integer.valueOf(tempStr.replaceAll("\"", " ").trim())).intValue();
    }
    lower_qty = tempInt;// INTEGER

    tempStr = tempArr[i++];
    if (tempStr.equalsIgnoreCase("NULL")) {
      tempInt = 0;
    } else {
      tempInt = (Integer.valueOf(tempStr.replaceAll("\"", " ").trim())).intValue();
    }
    upper_qty = tempInt;// INTEGER

    tempStr = tempArr[i++];
    if (tempStr.equalsIgnoreCase("NULL")) {
      tempInt = 0;
    } else {
      tempInt = (Integer.valueOf(tempStr.replaceAll("\"", " ").trim())).intValue();
    }
    inc_order_qty = tempInt;// INTEGER

    tempStr = tempArr[i++];
    if (tempStr.equalsIgnoreCase("NULL")) {
      tempInt = 0;
    } else {
      tempInt = (Integer.valueOf(tempStr.replaceAll("\"", " ").trim())).intValue();
    }
    retail_price = tempInt;// INTEGER

    is_benchmark_flag = tempArr[i++].replaceAll("\"", " ").trim();;// CHAR(1)

    tempStr = tempArr[i++];
    if (tempStr.equalsIgnoreCase("NULL")) {
      tempDouble = 0;
    } else {
      tempDouble = (Double.valueOf(tempStr.replaceAll("\"", " ").trim())).doubleValue();
    }
    yield_spread = tempDouble;// DOUBLE

    treasury_cusip = tempArr[i++].replaceAll("\"", " ").trim();// VARCHAR(9)

  }// end of Inventory constructor

}// end of class
