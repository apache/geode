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

public class ProhibitedSecurityQuote implements Serializable {

  public int prohibited_security_quote_id;// INTEGER
  public String cusip;// VARCHAR(9)
  public String price_type;// VARCHAR(3)
  public String distribution_channel;// VARCHAR(10)
  public String user_role;// VARCHAR(10)
  public int lower_qty;// INTEGER
  public int upper_qty;// INTEGER
  public String dealer_code;// VARCHAR(10)
  public String status;// VARCHAR(20)
  public String block_reason;// VARCHAR(200)
  public Timestamp sbs_timestamp;// TIMESTAMP
  public String user_id;// VARCHAR(30)
  public String ycf_filter_name;// VARCHAR(100)

  //////// constructor of class ProhibitedSecurityQuote

  protected String[] tempArr;
  protected int i = 0;

  public ProhibitedSecurityQuote(String inputStr) {
    tempArr = inputStr.split(",");

    prohibited_security_quote_id =
        (Integer.valueOf(tempArr[i++].replaceAll("\"", " ").trim())).intValue();// INTEGER
    cusip = tempArr[i++].replaceAll("\"", " ").trim();// VARCHAR(9)
    price_type = tempArr[i++].replaceAll("\"", " ").trim();// VARCHAR(3)
    distribution_channel = tempArr[i++].replaceAll("\"", " ").trim();// VARCHAR(10)
    user_role = tempArr[i++].replaceAll("\"", " ").trim();// VARCHAR(10)
    lower_qty = (Integer.valueOf(tempArr[i++].replaceAll("\"", " ").trim())).intValue();// INTEGER
    upper_qty = (Integer.valueOf(tempArr[i++].replaceAll("\"", " ").trim())).intValue();// INTEGER
    dealer_code = tempArr[i++].replaceAll("\"", " ").trim();// VARCHAR(10)
    status = tempArr[i++].replaceAll("\"", " ").trim();// VARCHAR(20)
    block_reason = tempArr[i++].replaceAll("\"", " ").trim();// VARCHAR(200)
    sbs_timestamp = Timestamp.valueOf(tempArr[i++].replaceAll("\"", " ").trim());// TIMESTAMP
    user_id = tempArr[i++].replaceAll("\"", " ").trim();// VARCHAR(30)
    ycf_filter_name = tempArr[i++].replaceAll("\"", " ").trim();// VARCHAR(100)

  }// end of ProhibitedSecurityQuote constructor

}// end of class
