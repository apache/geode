/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.gemstone.gemfire.rest.internal.web.controllers;

import static com.gemstone.gemfire.distributed.ConfigurationProperties.*;
import static org.junit.Assert.*;

import java.net.InetAddress;
import java.net.UnknownHostException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;

import org.json.JSONArray;
import org.json.JSONException;
import org.json.JSONObject;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.springframework.http.HttpEntity;
import org.springframework.http.HttpHeaders;
import org.springframework.http.HttpMethod;
import org.springframework.http.HttpStatus;
import org.springframework.http.MediaType;
import org.springframework.http.ResponseEntity;
import org.springframework.util.StringUtils;
import org.springframework.web.client.HttpClientErrorException;
import org.springframework.web.client.HttpServerErrorException;

import com.gemstone.gemfire.cache.AttributesFactory;
import com.gemstone.gemfire.cache.Cache;
import com.gemstone.gemfire.cache.CacheFactory;
import com.gemstone.gemfire.cache.CacheLoader;
import com.gemstone.gemfire.cache.CacheWriter;
import com.gemstone.gemfire.cache.CacheWriterException;
import com.gemstone.gemfire.cache.DataPolicy;
import com.gemstone.gemfire.cache.Declarable;
import com.gemstone.gemfire.cache.EntryEvent;
import com.gemstone.gemfire.cache.LoaderHelper;
import com.gemstone.gemfire.cache.RegionAttributes;
import com.gemstone.gemfire.cache.RegionDestroyedException;
import com.gemstone.gemfire.cache.RegionEvent;
import com.gemstone.gemfire.cache.RegionFactory;
import com.gemstone.gemfire.cache.RegionShortcut;
import com.gemstone.gemfire.cache.TimeoutException;
import com.gemstone.gemfire.cache.execute.FunctionService;
import com.gemstone.gemfire.distributed.ServerLauncher;
import com.gemstone.gemfire.internal.AvailablePortHelper;
import com.gemstone.gemfire.internal.GemFireVersion;
import com.gemstone.gemfire.internal.net.SocketCreator;
import com.gemstone.gemfire.management.internal.AgentUtil;
import com.gemstone.gemfire.management.internal.ManagementConstants;
import com.gemstone.gemfire.test.junit.categories.IntegrationTest;

@Category(IntegrationTest.class)
public class RestAPIsQueryAndFEJUnitTest {

  private Cache c;

  private String hostName;
  
  private String baseURL;
  
  private int restServicePort;
  
  private final String CUSTOMER_REGION = "customers";
  
  private final String ITEM_REGION = "items";
  private final String ORDER_REGION = "orders";
  private final String PRIMITIVE_KV_STORE_REGION = "primitiveKVStore";
  private final String UNKNOWN_REGION = "unknown_region";
  private final String EMPTY_REGION = "empty_region";
  
  private Map<Integer, QueryResultData> queryResultByIndex;
 
  private final String ORDER1_AS_JSON = "{"
      + "\"@type\": \"com.gemstone.gemfire.rest.internal.web.controllers.Order\","
      + "\"purchaseOrderNo\": 111," + "\"customerId\": 101,"
      + "\"description\": \"Purchase order for company - A\"," + "\"orderDate\": \"01/10/2014\"," + "\"deliveryDate\": \"01/20/2014\","
      + "\"contact\": \"Nilkanthkumar N Patel\","
      + "\"email\": \"npatel@pivotal.io\"," + "\"phone\": \"020-2048096\"," + "\"totalPrice\": 205,"
      + "\"items\":" + "[" + "{" + "\"itemNo\": 1,"
      + "\"description\": \"Product-1\"," + "\"quantity\": 5,"
      + "\"unitPrice\": 10," + "\"totalPrice\": 50" + "}," + "{"
      + "\"itemNo\": 1," + "\"description\": \"Product-2\","
      + "\"quantity\": 10," + "\"unitPrice\": 15.5," + "\"totalPrice\": 155"
      + "}" + "]" + "}";
  
  private final String MALFORMED_JSON = "{"
      + "\"@type\" \"com.gemstone.gemfire.rest.internal.web.controllers.Order\","
      + "\"purchaseOrderNo\": 111," + "\"customerId\": 101,"
      + "\"description\": \"Purchase order for company - A\"," + "\"orderDate\": \"01/10/2014\"," + "\"deliveryDate\": \"01/20/2014\","
      + "\"contact\": \"Nilkanthkumar N Patel\","
      + "\"email\": \"npatel@pivotal.io\"," + "\"phone\": \"020-2048096\"," + "\"totalPrice\": 205,"
      + "\"items\":" + "[" + "{" + "\"itemNo\": 1,"
      + "\"description\": \"Product-1\"," + "\"quantity\": 5,"
      + "\"unitPrice\": 10," + "\"totalPrice\": 50" + "}," + "{"
      + "\"itemNo\": 1," + "\"description\": \"Product-2\","
      + "\"quantity\": 10," + "\"unitPrice\": 15.5," + "\"totalPrice\": 155"
      + "}" + "]" + "}";
  
  private final String ORDER2_AS_JSON = "{"
      + "\"@type\": \"com.gemstone.gemfire.rest.internal.web.controllers.Order\","
      + "\"purchaseOrderNo\": 112," + "\"customerId\": 102,"
      + "\"description\": \"Purchase order for company - B\"," + "\"orderDate\": \"02/10/2014\"," + "\"deliveryDate\": \"02/20/2014\","
      + "\"contact\": \"John Blum\","
      + "\"email\": \"jblum@pivotal.io\"," + "\"phone\": \"01-2048096\"," + "\"totalPrice\": 225,"
      + "\"items\":" + "[" + "{" + "\"itemNo\": 1,"
      + "\"description\": \"Product-3\"," + "\"quantity\": 6,"
      + "\"unitPrice\": 20," + "\"totalPrice\": 120" + "}," + "{"
      + "\"itemNo\": 2," + "\"description\": \"Product-4\","
      + "\"quantity\": 10," + "\"unitPrice\": 10.5," + "\"totalPrice\": 105"
      + "}" + "]" + "}";
  
  private final String ORDER2_UPDATED_AS_JSON = "{"
      + "\"@type\": \"com.gemstone.gemfire.rest.internal.web.controllers.Order\","
      + "\"purchaseOrderNo\": 1112," + "\"customerId\": 102,"
      + "\"description\": \"Purchase order for company - B\","  + "\"orderDate\": \"02/10/2014\"," + "\"deliveryDate\": \"02/20/2014\","
      + "\"contact\": \"John Blum\","
      + "\"email\": \"jblum@pivotal.io\"," + "\"phone\": \"01-2048096\"," + "\"totalPrice\": 350,"
      + "\"items\":" + "[" + "{" + "\"itemNo\": 1,"
      + "\"description\": \"Product-AAAA\"," + "\"quantity\": 10,"
      + "\"unitPrice\": 20," + "\"totalPrice\": 200" + "}," + "{"
      + "\"itemNo\": 2," + "\"description\": \"Product-BBB\","
      + "\"quantity\": 15," + "\"unitPrice\": 10," + "\"totalPrice\": 150"
      + "}" + "]" + "}";
  
  final String CUSTOMER_LIST1_AS_JSON = "[" 
      + "{"
      + "\"@type\": \"com.gemstone.gemfire.rest.internal.web.controllers.Customer\","
      +"\"customerId\": 1,"
      + " \"firstName\": \"Vishal\","
      + " \"lastName\": \"Roa\"" 
      + "},"
      +"{"
      + "\"@type\": \"com.gemstone.gemfire.rest.internal.web.controllers.Customer\","
      +"\"customerId\": 2,"
      + " \"firstName\": \"Nilkanth\","
      + " \"lastName\": \"Patel\"" 
      + "},"
      + "{"
      + "\"@type\": \"com.gemstone.gemfire.rest.internal.web.controllers.Customer\","
      +"\"customerId\": 3,"
      + " \"firstName\": \"Avinash Dongre\","
      + " \"lastName\": \"Roa\"" 
      + "},"
      + "{"
      + "\"@type\": \"com.gemstone.gemfire.rest.internal.web.controllers.Customer\","
      +"\"customerId\": 4,"
      + " \"firstName\": \"Avinash Dongre\","
      + " \"lastName\": \"Roa\"" 
      + "}"
      + "]";
      
  final String CUSTOMER_LIST_AS_JSON = "[" 
      + "{"
      + "\"@type\": \"com.gemstone.gemfire.rest.internal.web.controllers.Customer\","
      +"\"customerId\": 101,"
      + " \"firstName\": \"Vishal\","
      + " \"lastName\": \"Roa\"" 
      + "},"
      +"{"
      + "\"@type\": \"com.gemstone.gemfire.rest.internal.web.controllers.Customer\","
      +"\"customerId\": 102,"
      + " \"firstName\": \"Nilkanth\","
      + " \"lastName\": \"Patel\"" 
      + "},"
      + "{"
      + "\"@type\": \"com.gemstone.gemfire.rest.internal.web.controllers.Customer\","
      +"\"customerId\": 103,"
      + " \"firstName\": \"Avinash Dongre\","
      + " \"lastName\": \"Roa\"" 
      + "},"
      +"{"
      + "\"@type\": \"com.gemstone.gemfire.rest.internal.web.controllers.Customer\","
      +"\"customerId\": 104,"
      + " \"firstName\": \"John\","
      + " \"lastName\": \"Blum\"" 
      + "},"
      + "{"
      + "\"@type\": \"com.gemstone.gemfire.rest.internal.web.controllers.Customer\","
      +"\"customerId\": 105,"
      + " \"firstName\": \"Shankar\","
      + " \"lastName\": \"Hundekar\"" 
      + "},"
      + "{"
      + "\"@type\": \"com.gemstone.gemfire.rest.internal.web.controllers.Customer\","
      +"\"customerId\": 106,"
      + " \"firstName\": \"Amey\","
      + " \"lastName\": \"Barve\"" 
      + "},"
      + "{"
      + "\"@type\": \"com.gemstone.gemfire.rest.internal.web.controllers.Customer\","
      +"\"customerId\": 107,"
      + " \"firstName\": \"Vishal\","
      + " \"lastName\": \"Roa\"" 
      + "},"
      + "{"
      + "\"@type\": \"com.gemstone.gemfire.rest.internal.web.controllers.Customer\","
      +"\"customerId\": 108,"
      + " \"firstName\": \"Supriya\","
      + " \"lastName\": \"Pillai\"" 
      + "},"
      + "{"
      + "\"@type\": \"com.gemstone.gemfire.rest.internal.web.controllers.Customer\","
      +"\"customerId\": 109,"
      + " \"firstName\": \"Tushar\","
      + " \"lastName\": \"khairnar\"" 
      + "},"
      + "{"
      + "\"@type\": \"com.gemstone.gemfire.rest.internal.web.controllers.Customer\","
      +"\"customerId\": 110,"
      + " \"firstName\": \"Rishitesh\","
      + " \"lastName\": \"Mishra\"" 
      + "},"
      + "{"
      + "\"@type\": \"com.gemstone.gemfire.rest.internal.web.controllers.Customer\","
      +"\"customerId\": 111,"
      + " \"firstName\": \"Ajay\","
      + " \"lastName\": \"Pandey\"" 
      + "},"
      + "{"
      + "\"@type\": \"com.gemstone.gemfire.rest.internal.web.controllers.Customer\","
      +"\"customerId\": 112,"
      + " \"firstName\": \"Suyog\","
      + " \"lastName\": \"Bokare\"" 
      + "},"
      + "{"
      + "\"@type\": \"com.gemstone.gemfire.rest.internal.web.controllers.Customer\","
      +"\"customerId\": 113,"
      + " \"firstName\": \"Rajesh\","
      + " \"lastName\": \"kumar\"" 
      + "},"
      + "{"
      + "\"@type\": \"com.gemstone.gemfire.rest.internal.web.controllers.Customer\","
      +"\"customerId\": 114,"
      + " \"firstName\": \"swati\","
      + " \"lastName\": \"sawant\"" 
      + "},"
      + "{"
      + "\"@type\": \"com.gemstone.gemfire.rest.internal.web.controllers.Customer\","
      +"\"customerId\": 115,"
      + " \"firstName\": \"sonal\","
      + " \"lastName\": \"Agrawal\"" 
      + "},"
      + "{"
      + "\"@type\": \"com.gemstone.gemfire.rest.internal.web.controllers.Customer\","
      +"\"customerId\": 116,"
      + " \"firstName\": \"Amogh\","
      + " \"lastName\": \"Shetkar\"" 
      + "},"
      + "{"
      + "\"@type\": \"com.gemstone.gemfire.rest.internal.web.controllers.Customer\","
      +"\"customerId\": 117,"
      + " \"firstName\": \"Viren\","
      + " \"lastName\": \"Balaut\"" 
      + "},"
      + "{"
      + "\"@type\": \"com.gemstone.gemfire.rest.internal.web.controllers.Customer\","
      +"\"customerId\": 118,"
      + " \"firstName\": \"Namrata\","
      + " \"lastName\": \"Tanvi\"" 
      + "},"
      + "{"
      + "\"@type\": \"com.gemstone.gemfire.rest.internal.web.controllers.Customer\","
      +"\"customerId\": 119,"
      + " \"firstName\": \"Rahul\","
      + " \"lastName\": \"Diyekar\"" 
      + "},"
      + "{"
      + "\"@type\": \"com.gemstone.gemfire.rest.internal.web.controllers.Customer\","
      +"\"customerId\": 120,"
      + " \"firstName\": \"Varun\","
      + " \"lastName\": \"Agrawal\"" 
      + "},"
      + "{"
      + "\"@type\": \"com.gemstone.gemfire.rest.internal.web.controllers.Customer\","
      +"\"customerId\": 121,"
      + " \"firstName\": \"Hemant\","
      + " \"lastName\": \"Bhanavat\"" 
      + "},"
      + "{"
      + "\"@type\": \"com.gemstone.gemfire.rest.internal.web.controllers.Customer\","
      +"\"customerId\": 122,"
      + " \"firstName\": \"Sunil\","
      + " \"lastName\": \"jigyasu\"" 
      + "},"
      + "{"
      + "\"@type\": \"com.gemstone.gemfire.rest.internal.web.controllers.Customer\","
      +"\"customerId\": 123,"
      + " \"firstName\": \"Sumedh\","
      + " \"lastName\": \"wale\"" 
      + "},"
      + "{"
      + "\"@type\": \"com.gemstone.gemfire.rest.internal.web.controllers.Customer\","
      +"\"customerId\": 124,"
      + " \"firstName\": \"saobhik\","
      + " \"lastName\": \"chaudhari\"" 
      + "},"
      + "{"
      + "\"@type\": \"com.gemstone.gemfire.rest.internal.web.controllers.Customer\","
      +"\"customerId\": 125,"
      + " \"firstName\": \"Ketki\","
      + " \"lastName\": \"Naidu\"" 
      + "},"
      + "{"
      + "\"@type\": \"com.gemstone.gemfire.rest.internal.web.controllers.Customer\","
      +"\"customerId\": 126,"
      + " \"firstName\": \"YOgesh\","
      + " \"lastName\": \"Mahajan\"" 
      + "},"
      + "{"
      + "\"@type\": \"com.gemstone.gemfire.rest.internal.web.controllers.Customer\","
      +"\"customerId\": 127,"
      + " \"firstName\": \"Surinder\","
      + " \"lastName\": \"Bindra\"" 
      + "},"
      + "{"
      + "\"@type\": \"com.gemstone.gemfire.rest.internal.web.controllers.Customer\","
      +"\"customerId\": 128,"
      + " \"firstName\": \"sandip\","
      + " \"lastName\": \"kasbe\"" 
      + "},"
      + "{"
      + "\"@type\": \"com.gemstone.gemfire.rest.internal.web.controllers.Customer\","
      +"\"customerId\": 129,"
      + " \"firstName\": \"shivam\","
      + " \"lastName\": \"Panada\"" 
      + "},"
      + "{"
      + "\"@type\": \"com.gemstone.gemfire.rest.internal.web.controllers.Customer\","
      +"\"customerId\": 130,"
      + " \"firstName\": \"Preeti\","
      + " \"lastName\": \"Kumari\"" 
      + "},"
      + "{"
      +"\"customerId\": 131,"
      + " \"firstName\": \"Vishal31\","
      + " \"lastName\": \"Roa31\"" 
      + "},"
      +"{"
      +"\"customerId\": 132,"
      + " \"firstName\": \"Nilkanth32\","
      + " \"lastName\": \"Patel32\"" 
      + "},"
      + "{"
      +"\"customerId\": 133,"
      + " \"firstName\": \"Avinash33\","
      + " \"lastName\": \"Dongre33\"" 
      + "},"
      +"{"
      +"\"customerId\": 134,"
      + " \"firstName\": \"John34\","
      + " \"lastName\": \"Blum34\"" 
      + "},"
      + "{"
      +"\"customerId\": 135,"
      + " \"firstName\": \"Shankar35\","
      + " \"lastName\": \"Hundekar35\"" 
      + "},"
      + "{"
      +"\"customerId\": 136,"
      + " \"firstName\": \"Amey36\","
      + " \"lastName\": \"Barve36\"" 
      + "},"
      + "{"
      +"\"customerId\": 137,"
      + " \"firstName\": \"Vishal37\","
      + " \"lastName\": \"Roa37\"" 
      + "},"
      + "{"
      +"\"customerId\": 138,"
      + " \"firstName\": \"Supriya38\","
      + " \"lastName\": \"Pillai38\"" 
      + "},"
      + "{"
      +"\"customerId\": 139,"
      + " \"firstName\": \"Tushar39\","
      + " \"lastName\": \"khairnar39\"" 
      + "},"
      + "{"
      +"\"customerId\": 140,"
      + " \"firstName\": \"Rishitesh40\","
      + " \"lastName\": \"Mishra40\"" 
      + "},"
      + "{"
      +"\"customerId\": 141,"
      + " \"firstName\": \"Ajay41\","
      + " \"lastName\": \"Pandey41\"" 
      + "},"
      + "{"
      +"\"customerId\": 142,"
      + " \"firstName\": \"Suyog42\","
      + " \"lastName\": \"Bokare42\"" 
      + "},"
      + "{"
      +"\"customerId\": 143,"
      + " \"firstName\": \"Rajesh43\","
      + " \"lastName\": \"kumar43\"" 
      + "},"
      + "{"
      +"\"customerId\": 144,"
      + " \"firstName\": \"swati44\","
      + " \"lastName\": \"sawant44\"" 
      + "},"
      + "{"
      +"\"customerId\": 145,"
      + " \"firstName\": \"sonal45\","
      + " \"lastName\": \"Agrawal45\"" 
      + "},"
      + "{"
      +"\"customerId\": 146,"
      + " \"firstName\": \"Amogh46\","
      + " \"lastName\": \"Shetkar46\"" 
      + "},"
      + "{"
      +"\"customerId\": 147,"
      + " \"firstName\": \"Viren47\","
      + " \"lastName\": \"Balaut47\"" 
      + "},"
      + "{"
      +"\"customerId\": 148,"
      + " \"firstName\": \"Namrata48\","
      + " \"lastName\": \"Tanvi48\"" 
      + "},"
      + "{"
      +"\"customerId\": 149,"
      + " \"firstName\": \"Rahul49\","
      + " \"lastName\": \"Diyekar49\"" 
      + "},"
      + "{"
      +"\"customerId\": 150,"
      + " \"firstName\": \"Varun50\","
      + " \"lastName\": \"Agrawal50\"" 
      + "},"
      + "{"
      +"\"customerId\": 151,"
      + " \"firstName\": \"Hemant50\","
      + " \"lastName\": \"Bhanavat50\"" 
      + "},"
      + "{"
      +"\"customerId\": 152,"
      + " \"firstName\": \"Sunil52\","
      + " \"lastName\": \"jigyasu52\"" 
      + "},"
      + "{"
      +"\"customerId\": 153,"
      + " \"firstName\": \"Sumedh53\","
      + " \"lastName\": \"wale53\"" 
      + "},"
      + "{"
      +"\"customerId\": 154,"
      + " \"firstName\": \"saobhik54\","
      + " \"lastName\": \"chaudhari54\"" 
      + "},"
      + "{"
      +"\"customerId\": 155,"
      + " \"firstName\": \"Ketki55\","
      + " \"lastName\": \"Naidu55\"" 
      + "},"
      + "{"
      +"\"customerId\": 156,"
      + " \"firstName\": \"YOgesh56\","
      + " \"lastName\": \"Mahajan56\"" 
      + "},"
      + "{"
      +"\"customerId\": 157,"
      + " \"firstName\": \"Surinder57\","
      + " \"lastName\": \"Bindra57\"" 
      + "},"
      + "{"
      +"\"customerId\": 158,"
      + " \"firstName\": \"sandip58\","
      + " \"lastName\": \"kasbe58\"" 
      + "},"
      + "{"
      +"\"customerId\": 159,"
      + " \"firstName\": \"shivam59\","
      + " \"lastName\": \"Panada59\"" 
      + "},"
      + "{"
      +"\"customerId\": 160,"
      + " \"firstName\": \"Preeti60\","
      + " \"lastName\": \"Kumari60\"" 
      + "}"
      + "]";
  
  private final String ORDER_AS_CASJSON = "{"
      + "\"@old\" :" 
      + "{"
      + "\"@type\": \"com.gemstone.gemfire.rest.internal.web.controllers.Order\","
      + "\"purchaseOrderNo\": 111," + "\"customerId\": 101,"
      + "\"description\": \"Purchase order for company - A\"," + "\"orderDate\": \"01/10/2014\"," + "\"deliveryDate\": \"01/20/2014\","
      + "\"contact\": \"Nilkanthkumar N Patel\","
      + "\"email\": \"npatel@pivotal.io\"," + "\"phone\": \"020-2048096\"," + "\"totalPrice\": 205,"
      + "\"items\":" + "[" + "{" + "\"itemNo\": 1,"
      + "\"description\": \"Product-1\"," + "\"quantity\": 5,"
      + "\"unitPrice\": 10," + "\"totalPrice\": 50" + "}," + "{"
      + "\"itemNo\": 1," + "\"description\": \"Product-2\","
      + "\"quantity\": 10," + "\"unitPrice\": 15.5," + "\"totalPrice\": 155"
      + "}" + "]" 
      + "},"
      + "\"@new\" :" 
      + "{"
      + "\"@type\": \"com.gemstone.gemfire.rest.internal.web.controllers.Order\","
      + "\"purchaseOrderNo\": 11101," + "\"customerId\": 101,"
      + "\"description\": \"Purchase order for company - A\"," + "\"orderDate\": \"01/10/2014\"," + "\"deliveryDate\": \"01/20/2014\","
      + "\"contact\": \"Nilkanthkumar N Patel\","
      + "\"email\": \"npatel@pivotal.io\"," + "\"phone\": \"020-2048096\"," + "\"totalPrice\": 205,"
      + "\"items\":" 
      + "[" 
        + "{" 
          + "\"itemNo\": 1,"
          +  "\"description\": \"Product-1\","
          + "\"quantity\": 5,"
          + "\"unitPrice\": 10,"
          + "\"totalPrice\": 50" 
        + "}," 
          + "{" 
          + "\"itemNo\": 3,"
          +  "\"description\": \"Product-3\","
          + "\"quantity\": 10,"
          + "\"unitPrice\": 100,"
          + "\"totalPrice\": 1000" 
          + "}," 
        + "{"
          + "\"itemNo\": 1,"
          + "\"description\": \"Product-2\","
          + "\"quantity\": 10,"
          + "\"unitPrice\": 15.5,"
          + "\"totalPrice\": 155"
        + "}"
      + "]" 
      + "}"
      + "}";
      
  private final String MALFORMED_CAS_JSON = "{"
      + "\"@old\" :" 
      + "{"
      + "\"@type\": \"com.gemstone.gemfire.rest.internal.web.controllers.Order\","
      + "\"purchaseOrderNo\": 111," + "\"customerId\": 101,"
      + "\"description\": \"Purchase order for company - A\"," + "\"orderDate\": \"01/10/2014\"," + "\"deliveryDate\": \"01/20/2014\","
      + "\"contact\": \"Nilkanthkumar N Patel\","
      + "\"email\": \"npatel@pivotal.io\"," + "\"phone\": \"020-2048096\"," + "\"totalPrice\": 205,"
      + "\"items\":" + "[" + "{" + "\"itemNo\": 1,"
      + "\"description\": \"Product-1\"," + "\"quantity\": 5,"
      + "\"unitPrice\": 10," + "\"totalPrice\": 50" + "}," + "{"
      + "\"itemNo\": 1," + "\"description\": \"Product-2\","
      + "\"quantity\": 10," + "\"unitPrice\": 15.5," + "\"totalPrice\": 155"
      + "}" + "]" 
      + "},"
       
      + "{"
      + "\"@type\": \"com.gemstone.gemfire.rest.internal.web.controllers.Order\","
      + "\"purchaseOrderNo\": 11101," + "\"customerId\": 101,"
      + "\"description\": \"Purchase order for company - A\"," + "\"orderDate\": \"01/10/2014\"," + "\"deliveryDate\": \"01/20/2014\","
      + "\"contact\": \"Nilkanthkumar N Patel\","
      + "\"email\": \"npatel@pivotal.io\"," + "\"phone\": \"020-2048096\"," + "\"totalPrice\": 205,"
      + "\"items\":" 
      + "[" 
        + "{" 
          + "\"itemNo\": 1,"
          +  "\"description\": \"Product-1\","
          + "\"quantity\": 5,"
          + "\"unitPrice\": 10,"
          + "\"totalPrice\": 50" 
        + "}," 
          + "{" 
          + "\"itemNo\": 3,"
          +  "\"description\": \"Product-3\","
          + "\"quantity\": 10,"
          + "\"unitPrice\": 100,"
          + "\"totalPrice\": 1000" 
          + "}," 
        + "{"
          + "\"itemNo\": 1,"
          + "\"description\": \"Product-2\","
          + "\"quantity\": 10,"
          + "\"unitPrice\": 15.5,"
          + "\"totalPrice\": 155"
        + "}"
      + "]" 
      + "}"
      + "}";
  final String[][] PARAMETERIZED_QUERIES = new String[][] {
      {
        "selectOrders",
        "SELECT DISTINCT o FROM /orders o, o.items item WHERE item.quantity > $1 AND item.totalPrice > $2" },
      { 
        "selectCustomer",
        "SELECT c FROM /customers c WHERE c.customerId = $1" },
      {
        "selectHighRollers",
        "SELECT DISTINCT c FROM /customers c, /orders o, o.items item WHERE item.totalprice > $1 AND c.customerId = o.customerId" 
      },
      {
        "testQuery",
        "SELECT DISTINCT c from /customers c where lastName=$1"
      },
      {
        "findSelectedCustomers",
        "SELECT * from /customers where customerId  IN SET ($1, $2, $3)"
      },
      {
        "invalidQuery",
        "This is invalid string"
      }
  };
  
  final String QUERY_ARGS2 = "{"
      + "\"@type\": \"int\","
      + "\"@value\": 101"
      + "}";
  
  final String QUERY_ARGS1 = "["
      +"{"
      + "\"@type\": \"int\","
      + "\"@value\": 2"
      + "},"
      +"{"
      + "\"@type\": \"double\","
      + "\"@value\": 110.00"
      + "}"
      + "]";

  @SuppressWarnings("unused")
  final String QUERY_ARGS3 = "["
      +"{"
      + "\"@type\": \"String\","
      + "\"@value\": \"Agrawal\""
      + "}"
      + "]";

  @SuppressWarnings("unused")
  final String QUERY_ARGS4 = "["
      +"{"
      + "\"@type\": \"int\","
      + "\"@value\": 20"
      + "},"
      +"{"
      + "\"@type\": \"int\","
      + "\"@value\": 120"
      + "},"
      +"{"
      + "\"@type\": \"int\","
      + "\"@value\": 130"
      + "}"
      + "]";
  
 
  final String FUNCTION_ARGS1 = "["
    +    "{"
    +        "\"@type\": \"double\","
    +        "\"@value\": 210"
    +    "},"
    +    "{"
    +        "\"@type\": \"com.gemstone.gemfire.rest.internal.web.controllers.Item\","
    +        "\"itemNo\": \"599\","
    +        "\"description\": \"Part X Free on Bumper Offer\","
    +        "\"quantity\": \"2\","
    +        "\"unitprice\": \"5\","
    +        "\"totalprice\": \"10.00\""
    +    "}"
    +"]";
  
  public final int METHOD_INDEX = 0;
  public final int URL_INDEX = 1;
  public final int REQUEST_BODY_INDEX = 2;
  public final int STATUS_CODE_INDEX = 3;
  public final int LOCATION_HEADER_INDEX = 4;
  public final int RESPONSE_HAS_BODY_INDEX = 5;
  public final int RESPONSE_HAS_EXCEPTION_INDEX = 6;
  
  final Object TEST_DATA[][]={ 
    { //0. create - 200 ok
      HttpMethod.POST,  
      "/orders?key=1",  
      ORDER1_AS_JSON, 
      HttpStatus.CREATED,               
      "/orders/1",
      false, 
      false
    },
    { //1. create - 409 conflict
      HttpMethod.POST,  
      "/orders?key=1", 
      ORDER1_AS_JSON,  
      HttpStatus.CONFLICT,              
      "/orders/1",
      true,
      true
    },
    { //2. create - 400 bad Req for malformed Json
      HttpMethod.POST,  
      "/orders?key=k1", 
      MALFORMED_JSON,   
      HttpStatus.BAD_REQUEST,           
      null,
      true,
      true
    },    
    { //3. create - 404, Not Found, for Region not exist
      HttpMethod.POST,  
      "/"+ UNKNOWN_REGION + "?key=k1",
      ORDER1_AS_JSON,
      HttpStatus.NOT_FOUND,             
      null,
      true,
      true
    },    
    { //4. create - 500 creating entry on region having DataPolicy=Empty
      HttpMethod.POST,  
      "/"+ EMPTY_REGION + "?key=k1", 
      ORDER1_AS_JSON, 
      HttpStatus.INTERNAL_SERVER_ERROR, 
      null,
      true,
      true
    },    
    { //5. Get data for key - 200 ok
      HttpMethod.GET,   
      "/orders/1", 
      null,
      HttpStatus.OK,                    
      "/orders/1",
      true,
      false
    },
    { //6. Get data for key - 404 region not exist
      HttpMethod.GET,   
      "/"+ UNKNOWN_REGION + "/1",
      null,
      HttpStatus.NOT_FOUND,             
      null,
      true,
      true
    },
    { //7. Get data for Non-existing key - 404, Resource NOT FOUND.
      HttpMethod.GET,   
      "/"+ EMPTY_REGION + "/unknown",
      null,
      HttpStatus.INTERNAL_SERVER_ERROR, 
      null,
      true,
      true
    },    
    { //8.  Put - 200 Ok, successful
      HttpMethod.PUT,   
      "/orders/2", 
      ORDER2_AS_JSON,
      HttpStatus.OK,                    
      "/orders/2",  
      false,
      false
    },
    { //9.  Put - 400 Bad Request, Malformed JSOn
      HttpMethod.PUT,   
      "/orders/3",
      MALFORMED_JSON, 
      HttpStatus.BAD_REQUEST,          
      null,
      true,
      true
    },
    { //10. Put - 404 Not Found, Region does not exist
      HttpMethod.PUT,   
      "/"+ UNKNOWN_REGION + "/k1",
      ORDER2_AS_JSON, 
      HttpStatus.NOT_FOUND,        
      null,
      true,
      true
    },
    { //11. Put - 500, Gemfire throws exception
      HttpMethod.PUT,   
      "/"+ EMPTY_REGION + "/k1",
      ORDER2_AS_JSON,
      HttpStatus.INTERNAL_SERVER_ERROR,
      null,
      true,
      true
    },    
    { //12. putAll - 200 Ok
      HttpMethod.PUT,   
      "/customers/1,2,3,4,5,6,7,8,9,10,11,12,13,14,15,16,17,18,19,20,21,22,23,24,25,26,27,28,29,30,31,32,33,34,35,36,37,38,39,40,41,42,43,44,45,46,47,48,49,50,51,52,53,54,55,56,57,58,59,60",
      CUSTOMER_LIST_AS_JSON,
      HttpStatus.OK,   
      "/customers/3,4,5,6",
      false,
      false
    },
    { //13. putAll - 400 bad Request, amlformed Json
      HttpMethod.PUT,   
      "/customers/3,4,5,6",
      MALFORMED_JSON,
      HttpStatus.BAD_REQUEST,  
      null,
      true,
      true
    },
    { //14. putAll - 404 Not Found, Region Does not exist
      HttpMethod.PUT,   
      "/"+ UNKNOWN_REGION + "/3,4,5,6",
      CUSTOMER_LIST1_AS_JSON,
      HttpStatus.NOT_FOUND,     
      null,
      true,
      true
    },
    { //15. putAll - 500, Gemfire throws exception
      HttpMethod.PUT,   
      "/"+ EMPTY_REGION + "/3,4,5,6",
      CUSTOMER_LIST1_AS_JSON,
      HttpStatus.INTERNAL_SERVER_ERROR, 
      null,
      true,
      true   
    },
    { //16. PUT?op=REPLACE, 200 Ok test case
      HttpMethod.PUT,
      "/orders/2?op=REPLACE",
      ORDER2_UPDATED_AS_JSON,
      HttpStatus.OK,
      "/orders/2",
      false,
      false		
    },
    { //17. Put?op=REPLACE, 400 Bad Request, Malformed JSOn
      HttpMethod.PUT,   
      "/orders/2?op=REPLACE",
      MALFORMED_JSON, 
      HttpStatus.BAD_REQUEST,          
      null,
      true,
      true
    },
    { //18. Put?op=REPLACE, 404 Not Found, Region does not exist
      HttpMethod.PUT,   
      "/"+ UNKNOWN_REGION + "/k1?op=rePlace",
      ORDER2_AS_JSON, 
      HttpStatus.NOT_FOUND,        
      null,
      true,
      true
    },
    { //19. Put?op=REPLACE, 500 testcase, Gemfire exception
      HttpMethod.PUT,   
      "/"+ EMPTY_REGION + "/k1?op=REPLACE",
      ORDER2_AS_JSON,
      HttpStatus.INTERNAL_SERVER_ERROR,
      null,
      true,
      true
    },
    { //20. Put?op=CAS, 200 OK testcase.
      HttpMethod.PUT,   
      "/orders/1?op=CAS",
      ORDER_AS_CASJSON,
      HttpStatus.OK,
      "/orders/1",
      false,
      false
    },
    { //21. Put?op=CAS, 409 OK testcase.
      HttpMethod.PUT,   
      "/orders/2?op=CAS",
      ORDER_AS_CASJSON,
      HttpStatus.CONFLICT,
      "/orders/2",
      true,
      true
    },
    { //22. Put?op=CAS, 400 Bad Request, Malformed JSOn
      HttpMethod.PUT,   
      "/orders/2?op=cas",
      MALFORMED_CAS_JSON, 
      HttpStatus.BAD_REQUEST,          
      null,
      true,
      true
    },
    { //23. Put?op=CAS, 404 Not Found, Region does not exist
      HttpMethod.PUT,   
      "/"+ UNKNOWN_REGION + "/k1?op=CAS",
      ORDER_AS_CASJSON, 
      HttpStatus.NOT_FOUND,        
      null,
      true,
      true
    },
    { //24. Put?op=cAs, 500 testcase, Gemfire exception
      HttpMethod.PUT,   
      "/"+ EMPTY_REGION + "/k1?op=cAs",
      ORDER_AS_CASJSON,
      HttpStatus.INTERNAL_SERVER_ERROR,
      null,
      true,
      true
    },
    { //25. Get - List all regions/resources - 200 ok testcase
      HttpMethod.GET,   
      "",
      null,
      HttpStatus.OK,
      RestTestUtils.GEMFIRE_REST_API_WEB_SERVICE_URL,
      true,
      true
    },
    { //26. List all regions/resources - 405 testcase.
      HttpMethod.POST,   
      "",
      null,
      HttpStatus.METHOD_NOT_ALLOWED,
      null,
      true,
      true
    },   
    { //27. GetAll - read all data for region - 200 ok, Default test case [No limit param specified].
      HttpMethod.GET,   
      "/customers",
      null,
      HttpStatus.OK,
      "/customers",
      true,
      false
    },
    { //28 GetAll - read all data for region - 404 NOT FOUND testcase.
      HttpMethod.GET,   
      "/" + UNKNOWN_REGION,
      null,
      HttpStatus.NOT_FOUND,
      null,
      true,
      true
    },
    { //29 GetAll - read all data for region - limit=ALL testcase.
      HttpMethod.GET,   
      "/customers?limit=ALL",
      null,
      HttpStatus.OK,
      null,
      true,
      false
    },
    { //30 GetAll - read data for fixed number of keys - limit=<NUMBER> testcase.
      HttpMethod.GET,   
      "/customers?limit=10",
      null,
      HttpStatus.OK,
      null,
      true,
      false
    },
    { //31. Get keys - List all keys in region - 200 ok testcase
      HttpMethod.GET,   
      "/customers/keys",
      null,
      HttpStatus.OK,
      "/customers/keys",
      true,
      false
    },
    { //32. Get keys - List all keys for region which does not exist - 404 NOt Found testcase
      HttpMethod.GET,   
      "/"+ UNKNOWN_REGION +"/keys",
      null,
      HttpStatus.NOT_FOUND,
      null,
      true,
      true
    },
    { //33. Get keys - 405 testcase, if any HTTP request method other than GET (e.g. only POST, NOT PUT, DELETE, as for them its a valid op) is used
      HttpMethod.POST,   
      "/customers/keys",
      null,
      HttpStatus.METHOD_NOT_ALLOWED,
      null,
      true,
      true
    },
    { //34. Read data for the specific keys. 200 Ok testcase.
      HttpMethod.GET,   
      "/customers/1,2,3,4,5,6,7,8,9,10",
      null,
      HttpStatus.OK,
      null,
      true,
      false
    },
    {
    //35. Read data for the specific keys. 404 Ok testcase.
      HttpMethod.GET,   
      "/" + UNKNOWN_REGION + "/1,2,3,4,5,6,7,8,9,10",
      null,
      HttpStatus.NOT_FOUND,
      null,
      true,
      true
    },
    { //36. delete data for key in region. 200 Ok testcase
      HttpMethod.DELETE,   
      "/customers/1",
      null,
      HttpStatus.OK,
      "/customers/1",
      false,
      false
    },
    { //37. delete data for key with non-existing region. 404 Not Found, testcase.
      HttpMethod.DELETE,   
      "/" + UNKNOWN_REGION + "/1",
      null,
      HttpStatus.NOT_FOUND,
      null,
      true,
      true
    },
    { //38. delete data for key, 500 - Gemfire throws exception testcase.
      HttpMethod.DELETE,   
      "/" + EMPTY_REGION + "/1",
      null,
      HttpStatus.NOT_FOUND,
      null,
      true,
      true
    },
    { //39. delete data for set of keys, 200 Ok, testcase.
      HttpMethod.DELETE,   
      "/customers/2,3,4,5",
      null,
      HttpStatus.OK,
      null,
      false,
      false
    },
    { //40. delete data for set of keys, 404 Region NOT Found, testcase.
      HttpMethod.DELETE,   
      "/" + UNKNOWN_REGION + "/2,3,4,5",
      null,
      HttpStatus.NOT_FOUND,
      null,
      true,
      true
    },
    { //41. delete data for set of keys, 500 Gemfire throws exception testcase.
      HttpMethod.DELETE,   
      "/" + EMPTY_REGION + "/2,3,4,5",
      null,
      HttpStatus.NOT_FOUND,
      null,
      true,
      true
    },
    { //42. create parameterized named query
      HttpMethod.POST,
      "/queries?id=" +  PARAMETERIZED_QUERIES[0][0] + "&q=" + PARAMETERIZED_QUERIES[0][1],
      null,
      HttpStatus.CREATED,
      "/queries/" + PARAMETERIZED_QUERIES[0][0],
      false,
      false
    },
    { //43. create parameterized named query 
      HttpMethod.POST,
      "/queries?id=" +  PARAMETERIZED_QUERIES[1][0] + "&q=" + PARAMETERIZED_QUERIES[1][1],
      null,
      HttpStatus.CREATED,
      "/queries/" + PARAMETERIZED_QUERIES[1][0],
      false,
      false
    },
    { //44. create parameterized named query
      HttpMethod.POST,
      "/queries?id=" +  PARAMETERIZED_QUERIES[2][0] + "&q=" + PARAMETERIZED_QUERIES[2][1],
      null,
      HttpStatus.CREATED,
      "/queries/" + PARAMETERIZED_QUERIES[2][0],
      false,
      false
    },
    { //45. list all named/parameterized queries
      //NOTE: query result = 3. old index=8.
      HttpMethod.GET,
      "/queries",
      null,
      HttpStatus.OK,
      "/queries",
      true,
      false
    },
    { //46. Run the specified named query passing in args for query parameters in request body
      //Note: Query Result = 2, Old index=9
      HttpMethod.POST,
      "/queries/" + PARAMETERIZED_QUERIES[0][0],
      QUERY_ARGS1,
      HttpStatus.OK,
      "/queries/" + PARAMETERIZED_QUERIES[0][0],
      true,
      false
    },
    { //47. Run the specified named query passing in args for query parameters in request body
      //Note: Query size = 1, old index = 10
      HttpMethod.POST,
      "/queries/" + PARAMETERIZED_QUERIES[1][0],
      QUERY_ARGS2,
      HttpStatus.OK,
      "/queries/" + PARAMETERIZED_QUERIES[1][0],
      true,
      false
    },
    { //48. Run an unnamed (unidentified), ad-hoc query passed as a URL parameter
      HttpMethod.GET,
      "/queries/adhoc?q=SELECT * FROM /customers", 
      null,
      HttpStatus.OK,
      null,
      true,
      false
    },
    { //49. list all functions available in the GemFire cluster
      HttpMethod.GET,
      "/functions",
      null,
      HttpStatus.OK,
      "/functions",
      true,
      false
    },
    { //50. Execute function with args on availabl nodes in the GemFire cluster
      HttpMethod.POST,
      "/functions/AddFreeItemToOrders?onRegion=orders",
      FUNCTION_ARGS1,
      HttpStatus.OK,
      "/functions/AddFreeItemToOrders",
      true,
      false
    },
    { //51. create parameterized named query "testQuery"
      HttpMethod.POST,
      "/queries?id=" +  PARAMETERIZED_QUERIES[3][0] + "&q=" + PARAMETERIZED_QUERIES[3][1],
      null,
      HttpStatus.CREATED,
      "/queries/" + PARAMETERIZED_QUERIES[3][0],
      false,
      false
    },
    { //52. update parameterized named query "testQuery"
      HttpMethod.PUT,
      "/queries/" +  PARAMETERIZED_QUERIES[0][0] + "?q=" + PARAMETERIZED_QUERIES[4][1],
      null,
      HttpStatus.OK,
      null,
      false,
      false
    },
    { //53. Run the updated named query passing in args for query parameters in request body
      HttpMethod.POST,
      "/queries/" + PARAMETERIZED_QUERIES[0][0],
      QUERY_ARGS1,
      HttpStatus.INTERNAL_SERVER_ERROR,
      null,
      true,
      false
    },
    { //54. update unknown parameterized named query 
      HttpMethod.PUT,
      "/queries/" +  "invalidQuery" + "?q=" + PARAMETERIZED_QUERIES[4][1],
      null,
      HttpStatus.NOT_FOUND,
      null,
      true,
      true
    },
    { //55. DELETE parameterized named query with invalid queryString
      HttpMethod.DELETE,
      "/queries/" + PARAMETERIZED_QUERIES[3][0] ,
      null,
      HttpStatus.OK,
      null,
      false,
      false
    },
    { //56. DELETE Non-existing parameterized named
      HttpMethod.DELETE,
      "/queries/" + PARAMETERIZED_QUERIES[3][0] ,
      null,
      HttpStatus.NOT_FOUND,
      null,
      true,
      true
    },
    { //57. Ping the REST service using HTTP HEAD
      HttpMethod.HEAD,
      "/ping",
      null,
      HttpStatus.OK,
      null,
      false,
      false
    },
    { //58. Ping the REST service using HTTP GET
      HttpMethod.GET,
      "/ping",
      null,
      HttpStatus.OK,
      null,
      false,
      false
    },
    { //59. Get the total number of entries in region
      HttpMethod.HEAD,
      "/customers",
      null,
      HttpStatus.OK,
      null,
      false,
      false
    },
    { //60. create parameterized named query "testQuery", passing it in request-body 
      HttpMethod.POST,
      "/queries?id=" +  PARAMETERIZED_QUERIES[3][0],
      PARAMETERIZED_QUERIES[3][1],
      HttpStatus.CREATED,
      "/queries/" + PARAMETERIZED_QUERIES[3][0],
      false,
      false
    },
    { //61. update parameterized named query, passing it in request-body 
      HttpMethod.PUT,
      "/queries/" +  PARAMETERIZED_QUERIES[3][0],
      PARAMETERIZED_QUERIES[4][1],
      HttpStatus.OK,
      null,
      false,
      false
    },
    { // 52.5. update parameterized named query "testQuery"
      HttpMethod.GET,
      "/queries",
      null,
      HttpStatus.OK,
      null,
      true,
      false
    },
  };
  //TEST_DATA_END
  
  final int LIST_ALL_NAMED_QUERIES_INDEX = 45;
  final List<Integer> VALID_400_URL_INDEXS = Arrays.asList(2, 9, 13, 17,22);
  final List<Integer> VALID_404_URL_INDEXS = Arrays.asList(3, 6, 7, 10, 14, 18, 23, 28, 32, 35, 37, 38, 40, 41, 54, 56);
  final List<Integer> VALID_409_URL_INDEXS = Arrays.asList(1, 21);
  final List<Integer> VALID_405_URL_INDEXS = Arrays.asList(26, 33);
  final List<Integer> Query_URL_INDEXS = Arrays.asList(LIST_ALL_NAMED_QUERIES_INDEX, 46, 47, 48);
  
  public String createRestURL(String baseURL, Object requestPart) {
    if(StringUtils.isEmpty(requestPart)) {
      return baseURL + RestTestUtils.GEMFIRE_REST_API_CONTEXT + RestTestUtils.GEMFIRE_REST_API_VERSION;
    }else {
      return baseURL + RestTestUtils.GEMFIRE_REST_API_CONTEXT + RestTestUtils.GEMFIRE_REST_API_VERSION + requestPart;
    }
  }

  public void initializeQueryTestData() {
    //LIST_ALL_NAMED_QUERY
    int size = PARAMETERIZED_QUERIES.length;
    List<String> queryIds = new ArrayList<>();
    for (int i=0; i < size; i++ ){
      queryIds.add(PARAMETERIZED_QUERIES[i][0]) ;
    }
      
    QueryResultData qIndex45_resultData = new QueryResultData();
    qIndex45_resultData.setQueryIndex(45);
    qIndex45_resultData.setType(QueryType.LIST_ALL_NAMED_QUERY);
    qIndex45_resultData.setResultSize(2);
    qIndex45_resultData.setResult(queryIds);
    queryResultByIndex.put(45, qIndex45_resultData);
    
    //index=46
    QueryResultData qIndex46_resultData = new QueryResultData();
    qIndex46_resultData.setQueryIndex(46);
    qIndex46_resultData.setType(QueryType.EXECUTE_NAMED_QUERY);
    qIndex46_resultData.setResultSize(2);
    qIndex46_resultData.setResult(null);
    queryResultByIndex.put(46, qIndex46_resultData);
    
    //index=47
    QueryResultData qIndex47_resultData = new QueryResultData();
    qIndex47_resultData.setQueryIndex(47);
    qIndex47_resultData.setType(QueryType.EXECUTE_NAMED_QUERY);
    qIndex47_resultData.setResultSize(0);
    qIndex47_resultData.setResult(null);
    queryResultByIndex.put(47, qIndex47_resultData);
    
    //index=48
    QueryResultData qIndex48_resultData = new QueryResultData();
    qIndex48_resultData.setQueryIndex(48);
    qIndex48_resultData.setType(QueryType.EXECUTE_ADHOC_QUERY);
    qIndex48_resultData.setResultSize(55);
    qIndex48_resultData.setResult(null);
    queryResultByIndex.put(48, qIndex48_resultData);
  }
  
  @Before
  public void setUp() throws Exception {
    AgentUtil agentUtil = new AgentUtil(GemFireVersion.getGemFireVersion());
    if (agentUtil.findWarLocation("geode-web-api") == null) {
      fail("unable to locate geode-web-api WAR file");
    }

    this.restServicePort = AvailablePortHelper.getRandomAvailableTCPPort();
    
    try {
      InetAddress addr = SocketCreator.getLocalHost();
      this.hostName = addr.getHostName();
    } catch (UnknownHostException ex) {
      this.hostName = ManagementConstants.DEFAULT_HOST_NAME;
    }
    
    String workingDirectory = System.getProperty("geode.build.dir", System.getProperty("user.dir"));
    
    ServerLauncher serverLauncher = new ServerLauncher.Builder()
        .set(MCAST_PORT, "0")
    .setServerBindAddress(this.hostName)
    .setServerPort(0)
        .set(START_DEV_REST_API, "true")
        .set(HTTP_SERVICE_PORT, String.valueOf(this.restServicePort))
        .set(HTTP_SERVICE_BIND_ADDRESS, this.hostName)
    .setPdxReadSerialized(true)
    .setWorkingDirectory(workingDirectory)
    .build();
    
    serverLauncher.start();
    
    this.baseURL = "http://" + this.hostName + ":" + this.restServicePort;
    this.c = CacheFactory.getAnyInstance();
    
    final AttributesFactory<String, String> attributesFactory = new AttributesFactory<>();
    attributesFactory.setDataPolicy(DataPolicy.REPLICATE);

    // Create region, customers
    final RegionAttributes<String, String> regionAttributes = attributesFactory
        .create();
    c.createRegion(CUSTOMER_REGION, regionAttributes);
    
    // Create region, items
    attributesFactory.setDataPolicy(DataPolicy.PARTITION);
    c.createRegion(ITEM_REGION, regionAttributes);
     
    // Create region, /orders
    final AttributesFactory<Object, Object> af2 = new AttributesFactory<>();
    af2.setDataPolicy(DataPolicy.PARTITION);
    final RegionAttributes<Object, Object> rAttributes2 = af2.create();
    
    c.createRegion(ORDER_REGION, rAttributes2);
   
    // Create region, primitiveKVStore
    final AttributesFactory<Object, Object> af1 = new AttributesFactory<>();
    af1.setDataPolicy(DataPolicy.PARTITION);
    final RegionAttributes<Object, Object> rAttributes = af1.create();
    
    c.createRegion(PRIMITIVE_KV_STORE_REGION, rAttributes);
   
    RegionFactory<String,Object> rf = c.createRegionFactory(RegionShortcut.REPLICATE);
    rf.setDataPolicy(DataPolicy.EMPTY);
    rf.setCacheLoader(new SimpleCacheLoader());
    rf.setCacheWriter(new SampleCacheWriter());
    rf.create(EMPTY_REGION);
    
    // Register functions here
    FunctionService.registerFunction(new GetAllEntries());
    FunctionService.registerFunction(new GetRegions());
    FunctionService.registerFunction(new PutKeyFunction());
    FunctionService.registerFunction(new GetDeliveredOrders());
    FunctionService.registerFunction(new AddFreeItemToOrders());
  }

  @After
  public void tearDown() {
    // shutdown and clean up the manager node.
    ServerLauncher.getInstance().stop();
  }
  
  private HttpHeaders setAcceptAndContentTypeHeaders() {
    List<MediaType> acceptableMediaTypes = new ArrayList<>();
    acceptableMediaTypes.add(MediaType.APPLICATION_JSON);

    HttpHeaders headers = new HttpHeaders();
    headers.setAccept(acceptableMediaTypes);
    headers.setContentType(MediaType.APPLICATION_JSON);
    return headers;
  }

  @Test
  public void testCreateAsJson() { 
    executeQueryTestCases();
  }

  private void caught(String message, Throwable cause) {
    throw new AssertionError(message, cause);
  }

  private void validateGetAllResult(int index, ResponseEntity<String> result){
    if (index == 27  || index == 29  || index == 30) {
      try {
        new JSONObject(result.getBody()).getJSONArray("customers");
      } catch (JSONException e) {
        caught("Caught JSONException in validateGetAllResult :: " + e.getMessage(), e);
      }
    }
  }
  
  private void verifyRegionSize(int index, ResponseEntity<String> result) {
    if (index == 59 ) {
      HttpHeaders headers = result.getHeaders();
      String value = headers.getFirst("Resource-Count");
      assertEquals(Integer.parseInt(value), 55);
    }
  }
  
  private void validateQueryResult(int index, ResponseEntity<String> result){
    if (Query_URL_INDEXS.contains(index)) {
      queryResultByIndex = new HashMap<>();
      initializeQueryTestData();  
      QueryResultData queryResult =  queryResultByIndex.get(index);   

      //Check whether received response contains expected query IDs.
      if(index == 45 ) { 
        
        try {
          JSONObject jsonObject = new JSONObject(result.getBody());
          JSONArray jsonArray = new JSONArray(jsonObject.get("queries").toString());
          
          for (int i=0; i< jsonArray.length(); i++) {  
            assertTrue("PREPARE_PARAMETERIZED_QUERY: function IDs are not matched", queryResult.getResult().contains(jsonArray.getJSONObject(i).getString("id")));
          }
        } catch (JSONException e) {
          caught("Caught JSONException in validateQueryResult :: " + e.getMessage(), e);
        }
      }
      else if (index == 46 || index == 47 || index == 48) {
        
        JSONArray jsonArray;
        try {
          jsonArray = new JSONArray(result.getBody());
          //verify query result size
          assertEquals(queryResult.getResultSize(), jsonArray.length());
        } catch (JSONException e) {
          caught("Caught JSONException in validateQueryResult :: " + e.getMessage(), e);
        }
      }
    }
  }
  
  private String addExpectedException (int index) {
    String expectedEx =  "appears to have started a thread named";
    if (index == 4 || index == 5 || index == 24) {
      expectedEx = "java.lang.UnsupportedOperationException";
      c.getLogger().info("<ExpectedException action=add>" + expectedEx + "</ExpectedException>");
      return expectedEx;
    } else if (index == 7) {
      expectedEx = "com.gemstone.gemfire.cache.TimeoutException";
      c.getLogger().info("<ExpectedException action=add>" + expectedEx + "</ExpectedException>");
      return expectedEx;
    } else if (index == 11 || index == 15) {
      expectedEx = "com.gemstone.gemfire.cache.CacheWriterException";
      c.getLogger().info("<ExpectedException action=add>" + expectedEx + "</ExpectedException>");
      return expectedEx;
    } else if (index == 19) {
      expectedEx = "java.lang.IllegalArgumentException";
      c.getLogger().info("<ExpectedException action=add>" + expectedEx + "</ExpectedException>");
      return expectedEx;
    } else if (index == 38 || index == 41 ) {
      expectedEx = "com.gemstone.gemfire.cache.RegionDestroyedException";
      c.getLogger().info("<ExpectedException action=add>" + expectedEx + "</ExpectedException>");
      return expectedEx;
    }
    
    return expectedEx;
  }

  private void executeQueryTestCases() {
    HttpHeaders headers = setAcceptAndContentTypeHeaders();
    HttpEntity<Object> entity;
    
    int totalRequests = TEST_DATA.length;
    String expectedEx = null;
      
    for (int index=0; index < totalRequests; index++) {
      try {
        expectedEx = addExpectedException(index);
        final String restRequestUrl = createRestURL(this.baseURL, TEST_DATA[index][URL_INDEX]);

        entity = new HttpEntity<>(TEST_DATA[index][REQUEST_BODY_INDEX], headers);
        ResponseEntity<String> result = RestTestUtils.getRestTemplate().exchange(
            restRequestUrl,
            (HttpMethod)TEST_DATA[index][METHOD_INDEX], entity, String.class);

        validateGetAllResult(index, result);
        validateQueryResult(index, result);

        assertEquals(result.getStatusCode(), TEST_DATA[index][STATUS_CODE_INDEX]);
        assertEquals(result.hasBody(), ((Boolean)TEST_DATA[index][RESPONSE_HAS_BODY_INDEX]).booleanValue());

        verifyRegionSize(index, result);
        //TODO:
        //verify location header

      } catch (HttpClientErrorException e) {

        if( VALID_409_URL_INDEXS.contains(index)) {
          //create-409, conflict testcase. [create on already existing key]

          assertEquals(e.getStatusCode(), TEST_DATA[index][STATUS_CODE_INDEX]);
          assertEquals(StringUtils.hasText(e.getResponseBodyAsString()),((Boolean)TEST_DATA[index][RESPONSE_HAS_BODY_INDEX]).booleanValue());

        }else if (VALID_400_URL_INDEXS.contains(index)) {
          // 400, Bad Request testcases. [create with malformed Json]

          assertEquals(e.getStatusCode(), TEST_DATA[index][STATUS_CODE_INDEX]);
          assertEquals(StringUtils.hasText(e.getResponseBodyAsString()), ((Boolean)TEST_DATA[index][RESPONSE_HAS_BODY_INDEX]).booleanValue());

        }
        else if (VALID_404_URL_INDEXS.contains(index) ) {
          // create-404, Not Found testcase. [create on not-existing region]

          assertEquals(e.getStatusCode(), TEST_DATA[index][STATUS_CODE_INDEX]);
          assertEquals(StringUtils.hasText(e.getResponseBodyAsString()), ((Boolean)TEST_DATA[index][RESPONSE_HAS_BODY_INDEX]).booleanValue());

        }
        else if (VALID_405_URL_INDEXS.contains(index) ) {
          // create-404, Not Found testcase. [create on not-existing region]

          assertEquals(e.getStatusCode(), TEST_DATA[index][STATUS_CODE_INDEX]);
          assertEquals(StringUtils.hasText(e.getResponseBodyAsString()), ((Boolean)TEST_DATA[index][RESPONSE_HAS_BODY_INDEX]).booleanValue());
        }
        else {
        fail( "Index:" + index+ " " +  TEST_DATA[index][METHOD_INDEX] + " " + TEST_DATA[index][URL_INDEX] + " should not have thrown exception ");
        }

      } catch (HttpServerErrorException se) {
        //index=4, create- 500, INTERNAL_SERVER_ERROR testcase. [create on Region with DataPolicy=Empty set]
        //index=7, create- 500, INTERNAL_SERVER_ERROR testcase. [Get, attached cache loader throws Timeout exception]
        //index=11, put- 500, [While doing R.put, CacheWriter.beforeCreate() has thrown CacheWriterException]
        //.... and more test cases
        assertEquals(se.getStatusCode(), TEST_DATA[index][STATUS_CODE_INDEX]);
        assertEquals(StringUtils.hasText(se.getResponseBodyAsString()), ((Boolean)TEST_DATA[index][RESPONSE_HAS_BODY_INDEX]).booleanValue());

      }
      catch (Exception e) {
        caught("caught Exception in executeQueryTestCases " + "Index:" + index+ " " +  TEST_DATA[index][METHOD_INDEX] + " " + TEST_DATA[index][URL_INDEX] + " :: Unexpected ERROR...!!", e);
      } finally {
        c.getLogger().info("<ExpectedException action=remove>" + expectedEx + "</ExpectedException>");
      }
    }
  }
}

// TODO: move following classes to be inner classes

class SimpleCacheLoader implements CacheLoader<String, Object>, Declarable {
	  
  @Override
  public Object load(LoaderHelper helper) {
    //throws TimeoutException  
    throw new TimeoutException("Could not load, Request Timedout...!!");
  }

  @Override
  public void close() {
  }

  @Override
  public void init(Properties props) {
    
  }
}

class SampleCacheWriter  implements CacheWriter<String, Object> {

  @Override
  public void close() {
  }

  @Override
  public void beforeUpdate(EntryEvent event) throws CacheWriterException {
  }

  @Override
  public void beforeCreate(EntryEvent event) throws CacheWriterException {
    throw new CacheWriterException("Put request failed as gemfire has thrown error...!!");
  }

  @Override
  public void beforeDestroy(EntryEvent event) throws CacheWriterException {
    throw new RegionDestroyedException("Region has been destroyed already...!!", "dummyRegion");
  }

  @Override
  public void beforeRegionDestroy(RegionEvent event) throws CacheWriterException {
  }

  @Override
  public void beforeRegionClear(RegionEvent event) throws CacheWriterException {
  }
}

enum QueryType {LIST_ALL_NAMED_QUERY, EXECUTE_NAMED_QUERY, EXECUTE_ADHOC_QUERY }

class QueryResultData {

  private int queryIndex;
  private QueryType type; 
  private int resultSize;
  private List<String> result;
  
  public QueryResultData() {
  }

  @SuppressWarnings("unused")
  public QueryResultData(int index, QueryType type, int size, List<String> result){
    this.queryIndex = index;
    this.type = type;
    this.resultSize = size;
    this.result = result;
  }

  public QueryType getType() {
    return type;
  }

  public void setType(QueryType type) {
    this.type = type;
  }

  public int getQueryIndex() {
    return queryIndex;
  }

  public void setQueryIndex(int queryIndex) {
    this.queryIndex = queryIndex;
  }

  public int getResultSize() {
    return resultSize;
  }

  public void setResultSize(int resultSize) {
    this.resultSize = resultSize;
  }

  public List<String> getResult() {
    return result;
  }

  public void setResult(List<String> result) {
    this.result = result;
  }
  
}
