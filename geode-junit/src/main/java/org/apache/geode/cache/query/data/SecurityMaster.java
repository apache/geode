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

public class SecurityMaster implements Serializable {

  public String cusip; // VARCHAR(9)
  public String cusip_prohibited;// VARCHAR(1)
  public String security_number;// VARCHAR(7)
  public String security_type;// VARCHAR(1)
  public String sbs_product_code;// VARCHAR(10)
  public String sbs_product_group;// _CODE VARCHAR(20)
  public String is_callable;// VARCHAR(1)
  public Timestamp dated_date;// TIMESTAMP
  public Timestamp maturity_date;// TIMESTAMP
  public Timestamp coupon_date;// TIMESTAMP
  public double coupon_rate;// DOUBLE
  public Timestamp issue_date;// TIMESTAMP
  public double issuer_price;// DOUBLE
  public String moody_rating;// VARCHAR(10)
  public String snp_rating;// VARCHAR(10)
  public String security_description;// VARCHAR(200)
  public String state_code;// VARCHAR(2)
  public String country_code;// VARCHAR(3)
  public Timestamp first_settlement_date;// TIMESTAMP
  public Timestamp next_settlement_date; // TIMESTAMP
  public Timestamp first_coupon_date;// TIMESTAMP
  public Timestamp next_last_coupon_date; // TIMESTAMP
  public Timestamp last_coupon_date; // TIMESTAMP
  public String call_type;// VARCHAR(10)
  public Timestamp pre_refunded_date; // TIMESTAMP
  public double pre_refunded_price;// DOUBLE
  public String put_type;// VARCHAR(5)
  public String coupon_frequency;// VARCHAR(15)
  public String sinking_fund_type;// VARCHAR(5)
  public String escrow_type;// VARCHAR(5)
  public String pre_refund_type;// VARCHAR(5)
  public String insurance_type;// VARCHAR(5)
  public String insurer_name;// VARCHAR(60)
  public String exchange;// VARCHAR(4)
  public String is_escrow;// VARCHAR(1)
  public String is_prerefunded;// VARCHAR(1)
  public String is_insured;// VARCHAR(1)
  public String is_taxable;// VARCHAR(1)
  public String is_subj_amt;// VARCHAR(1)
  public String is_exmt_amt;// VARCHAR(1)
  public String is_bank_qualified;// VARCHAR(1)
  public String is_general_oblig;// VARCHAR(1)
  public String is_revenue;// VARCHAR(1)
  public String legal_defeasance_status;// VARCHAR(1)
  public String alternative_minimum_tax_status;// VARCHAR(1)
  public String tax_indicator;// VARCHAR(1)
  public String bank_qualified_status;// VARCHAR(1)
  public String taxable_status;// VARCHAR(1)
  public String advalorem_tax_status;// VARCHAR(1)
  public String source;// VARCHAR(8)
  public String accrual_daycount_code;// VARCHAR(22)
  public String is_endof_monthpayment;// VARCHAR(10)
  public String symbol;// VARCHAR(9)
  public String short_description;// VARCHAR(48)
  public String moody_watch;// VARCHAR(10)
  public String snp_watch;// VARCHAR(10)
  public double article_counter;// DOUBLE
  public String coupon_type;// VARCHAR(10)
  public Timestamp update_date;// TIMESTAMP
  public String is_perpetual;// VARCHAR(1)
  public String tba_cusip;// VARCHAR(7)
  public String coupon_class;// VARCHAR(10)
  public int amortization_term;// INTEGER
  public String mbs_class_code;// VARCHAR(1)
  public String mbscc_eligibility_flag;// VARCHAR(1)
  public String additional_sec_desc;// VARCHAR(256)
  public String hand_figure_ind;// VARCHAR(1)
  public String trading_flat_ind;// VARCHAR(1)
  public String monetary_default_ind;// VARCHAR(1)
  public String investment_grade;// VARCHAR(1)


  //////// constructor of class SecurityMaster

  protected String[] tempArr;
  protected int i = 0;
  protected String tempStr;
  protected int tempInt;
  protected double tempDouble;

  public SecurityMaster(String inputStr) {
    tempArr = inputStr.split(",");

    cusip = tempArr[i++].replaceAll("\"", " ").trim(); // VARCHAR(9)

    cusip_prohibited = tempArr[i++].replaceAll("\"", " ").trim();// VARCHAR(1)
    security_number = tempArr[i++].replaceAll("\"", " ").trim();// VARCHAR(7)
    security_type = tempArr[i++].replaceAll("\"", " ").trim();// VARCHAR(1)
    sbs_product_code = tempArr[i++].replaceAll("\"", " ").trim();// VARCHAR(10)
    sbs_product_group = tempArr[i++].replaceAll("\"", " ").trim();// _CODE VARCHAR(20)
    is_callable = tempArr[i++].replaceAll("\"", " ").trim();// VARCHAR(1)

    tempStr = tempArr[i++];
    if (!tempStr.equalsIgnoreCase("NULL")) {
      dated_date = Timestamp.valueOf(tempStr.replaceAll("\"", " ").trim());// TIMESTAMP
    }

    tempStr = tempArr[i++];
    if (!tempStr.equalsIgnoreCase("NULL")) {
      maturity_date = Timestamp.valueOf(tempStr.replaceAll("\"", " ").trim());// TIMESTAMP
    }

    tempStr = tempArr[i++];
    if (!tempStr.equalsIgnoreCase("NULL")) {
      coupon_date = Timestamp.valueOf(tempStr.replaceAll("\"", " ").trim());// TIMESTAMP
    }

    tempStr = tempArr[i++];
    if (!tempStr.equalsIgnoreCase("NULL")) {
      coupon_rate = (Double.valueOf(tempStr.replaceAll("\"", " ").trim())).doubleValue();// DOUBLE
    }

    tempStr = tempArr[i++];
    if (!tempStr.equalsIgnoreCase("NULL")) {
      issue_date = Timestamp.valueOf(tempStr.replaceAll("\"", " ").trim());// TIMESTAMP
    }

    tempStr = tempArr[i++];
    if (!tempStr.equalsIgnoreCase("NULL")) {
      issuer_price = (Double.valueOf(tempStr.replaceAll("\"", " ").trim())).doubleValue();// DOUBLE
    }

    moody_rating = tempArr[i++].replaceAll("\"", " ").trim();// VARCHAR(10)
    snp_rating = tempArr[i++].replaceAll("\"", " ").trim();// VARCHAR(10)

    tempStr = tempArr[i++];
    tempStr = tempStr.replaceFirst("\"", " ").trim();
    if (tempStr.lastIndexOf("\"") == -1) {
      tempStr = tempStr + tempArr[i++];
    }
    security_description = tempStr.replaceAll("\"", " ").trim();// VARCHAR(200)

    state_code = tempArr[i++].replaceAll("\"", " ").trim();// VARCHAR(2)
    country_code = tempArr[i++].replaceAll("\"", " ").trim();// VARCHAR(3)

    tempStr = tempArr[i++];
    if (!tempStr.equalsIgnoreCase("NULL")) {
      first_settlement_date = Timestamp.valueOf(tempStr.replaceAll("\"", " ").trim());// TIMESTAMP
    }

    tempStr = tempArr[i++];
    if (!tempStr.equalsIgnoreCase("NULL")) {
      next_settlement_date = Timestamp.valueOf(tempStr.replaceAll("\"", " ").trim());// TIMESTAMP
    }

    tempStr = tempArr[i++];
    if (!tempStr.equalsIgnoreCase("NULL")) {
      first_coupon_date = Timestamp.valueOf(tempStr.replaceAll("\"", " ").trim());// TIMESTAMP
    }

    tempStr = tempArr[i++];
    if (!tempStr.equalsIgnoreCase("NULL")) {
      next_last_coupon_date = Timestamp.valueOf(tempStr.replaceAll("\"", " ").trim());// TIMESTAMP
    }

    tempStr = tempArr[i++];
    if (!tempStr.equalsIgnoreCase("NULL")) {
      last_coupon_date = Timestamp.valueOf(tempStr.replaceAll("\"", " ").trim());// TIMESTAMP
    }

    call_type = tempArr[i++].replaceAll("\"", " ").trim();// VARCHAR(10)

    tempStr = tempArr[i++];
    if (!tempStr.equalsIgnoreCase("NULL")) {
      pre_refunded_date = Timestamp.valueOf(tempStr.replaceAll("\"", " ").trim());// TIMESTAMP
    }

    tempStr = tempArr[i++];
    if (!tempStr.equalsIgnoreCase("NULL")) {
      pre_refunded_price = (Double.valueOf(tempStr.replaceAll("\"", " ").trim())).doubleValue();// DOUBLE
    }

    put_type = tempArr[i++].replaceAll("\"", " ").trim();// VARCHAR(5)
    coupon_frequency = tempArr[i++].replaceAll("\"", " ").trim();// VARCHAR(15)
    sinking_fund_type = tempArr[i++].replaceAll("\"", " ").trim();// VARCHAR(5)
    escrow_type = tempArr[i++].replaceAll("\"", " ").trim();// VARCHAR(5)
    pre_refund_type = tempArr[i++].replaceAll("\"", " ").trim();// VARCHAR(5)
    insurance_type = tempArr[i++].replaceAll("\"", " ").trim();// VARCHAR(5)
    insurer_name = tempArr[i++].replaceAll("\"", " ").trim();// VARCHAR(60)
    exchange = tempArr[i++].replaceAll("\"", " ").trim();// VARCHAR(4)
    is_escrow = tempArr[i++].replaceAll("\"", " ").trim();// VARCHAR(1)
    is_prerefunded = tempArr[i++].replaceAll("\"", " ").trim();// VARCHAR(1)
    is_insured = tempArr[i++].replaceAll("\"", " ").trim();// VARCHAR(1)
    is_taxable = tempArr[i++].replaceAll("\"", " ").trim();// VARCHAR(1)
    is_subj_amt = tempArr[i++].replaceAll("\"", " ").trim();// VARCHAR(1)
    is_exmt_amt = tempArr[i++].replaceAll("\"", " ").trim();// VARCHAR(1)
    is_bank_qualified = tempArr[i++].replaceAll("\"", " ").trim();// VARCHAR(1)
    is_general_oblig = tempArr[i++].replaceAll("\"", " ").trim();// VARCHAR(1)
    is_revenue = tempArr[i++].replaceAll("\"", " ").trim();// VARCHAR(1)
    legal_defeasance_status = tempArr[i++].replaceAll("\"", " ").trim();// VARCHAR(1)
    alternative_minimum_tax_status = tempArr[i++].replaceAll("\"", " ").trim();// VARCHAR(1)
    tax_indicator = tempArr[i++].replaceAll("\"", " ").trim();// VARCHAR(1)
    bank_qualified_status = tempArr[i++].replaceAll("\"", " ").trim();// VARCHAR(1)
    taxable_status = tempArr[i++].replaceAll("\"", " ").trim();// VARCHAR(1)
    advalorem_tax_status = tempArr[i++].replaceAll("\"", " ").trim();// VARCHAR(1)
    source = tempArr[i++].replaceAll("\"", " ").trim();// VARCHAR(8)
    accrual_daycount_code = tempArr[i++].replaceAll("\"", " ").trim();// VARCHAR(22)
    is_endof_monthpayment = tempArr[i++].replaceAll("\"", " ").trim();// VARCHAR(10)
    symbol = tempArr[i++].replaceAll("\"", " ").trim();// VARCHAR(9)
    short_description = tempArr[i++].replaceAll("\"", " ").trim();// VARCHAR(48)
    moody_watch = tempArr[i++].replaceAll("\"", " ").trim();// VARCHAR(10)
    snp_watch = tempArr[i++].replaceAll("\"", " ").trim();// VARCHAR(10)

    tempStr = tempArr[i++];
    if (!tempStr.equalsIgnoreCase("NULL")) {
      article_counter = (Double.valueOf(tempStr.replaceAll("\"", " ").trim())).doubleValue();// DOUBLE
    }

    coupon_type = tempArr[i++].replaceAll("\"", " ").trim();// VARCHAR(10)

    tempStr = tempArr[i++];
    if (!tempStr.equalsIgnoreCase("NULL")) {
      update_date = Timestamp.valueOf(tempStr.replaceAll("\"", " ").trim());// TIMESTAMP
    }

    is_perpetual = tempArr[i++].replaceAll("\"", " ").trim();// VARCHAR(1)
    tba_cusip = tempArr[i++].replaceAll("\"", " ").trim();// VARCHAR(7)
    coupon_class = tempArr[i++].replaceAll("\"", " ").trim();// VARCHAR(10)

    tempStr = tempArr[i++];
    if (!tempStr.equalsIgnoreCase("NULL")) {
      amortization_term = (Integer.valueOf(tempStr.replaceAll("\"", " ").trim())).intValue();// INTEGER
    }

    mbs_class_code = tempArr[i++].replaceAll("\"", " ").trim();// VARCHAR(1)
    mbscc_eligibility_flag = tempArr[i++].replaceAll("\"", " ").trim();// VARCHAR(1)
    additional_sec_desc = tempArr[i++].replaceAll("\"", " ").trim();// VARCHAR(256)
    hand_figure_ind = tempArr[i++].replaceAll("\"", " ").trim();// VARCHAR(1)
    trading_flat_ind = tempArr[i++].replaceAll("\"", " ").trim();// VARCHAR(1)
    monetary_default_ind = tempArr[i++].replaceAll("\"", " ").trim();// VARCHAR(1)
    investment_grade = tempArr[i++].replaceAll("\"", " ").trim();// VARCHAR(1)

  }// end of SecurityMaster constructor

}// end of class
