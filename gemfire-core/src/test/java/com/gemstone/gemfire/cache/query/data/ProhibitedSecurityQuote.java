/*=========================================================================
 * Copyright (c) 2010-2014 Pivotal Software, Inc. All Rights Reserved.
 * This product is protected by U.S. and international copyright
 * and intellectual property laws. Pivotal products are covered by
 * one or more patents listed at http://www.pivotal.io/patents.
 *=========================================================================
 */
package com.gemstone.gemfire.cache.query.data;

import java.sql.*;
import java.io.*;

/*
 * author: Prafulla Chaudhari
 */

public class ProhibitedSecurityQuote implements Serializable{
	
	public int prohibited_security_quote_id;// INTEGER
	public String cusip;//        VARCHAR(9)
	public String price_type;//   VARCHAR(3)
	public String distribution_channel;// VARCHAR(10)
	public String user_role;//    VARCHAR(10)
	public int lower_qty;//    INTEGER
	public int upper_qty;//    INTEGER
	public String dealer_code;//  VARCHAR(10)
	public String status;//       VARCHAR(20)
	public String block_reason;// VARCHAR(200)
	public Timestamp sbs_timestamp;// TIMESTAMP
	public String user_id;//      VARCHAR(30)
	public String ycf_filter_name;// VARCHAR(100)
	
	////////constructor of class ProhibitedSecurityQuote
	
	protected String  [] tempArr;
	protected int i=0;
	
	public ProhibitedSecurityQuote(String inputStr){
		tempArr = inputStr.split(",");
		
		prohibited_security_quote_id = (Integer.valueOf(tempArr[i++].replaceAll("\"", " ").trim())).intValue();// INTEGER
		cusip = tempArr[i++].replaceAll("\"", " ").trim();//        VARCHAR(9)
		price_type = tempArr[i++].replaceAll("\"", " ").trim();//   VARCHAR(3)
		distribution_channel = tempArr[i++].replaceAll("\"", " ").trim();// VARCHAR(10)
		user_role = tempArr[i++].replaceAll("\"", " ").trim();//    VARCHAR(10)
		lower_qty = (Integer.valueOf(tempArr[i++].replaceAll("\"", " ").trim())).intValue();//    INTEGER
		upper_qty = (Integer.valueOf(tempArr[i++].replaceAll("\"", " ").trim())).intValue();//    INTEGER
		dealer_code = tempArr[i++].replaceAll("\"", " ").trim();//  VARCHAR(10)
		status = tempArr[i++].replaceAll("\"", " ").trim();//       VARCHAR(20)
		block_reason = tempArr[i++].replaceAll("\"", " ").trim();// VARCHAR(200)
		sbs_timestamp = Timestamp.valueOf(tempArr[i++].replaceAll("\"", " ").trim());// TIMESTAMP
		user_id = tempArr[i++].replaceAll("\"", " ").trim();//      VARCHAR(30)
		ycf_filter_name = tempArr[i++].replaceAll("\"", " ").trim();// VARCHAR(100)
		
	}//end of ProhibitedSecurityQuote constructor

}//end of class
