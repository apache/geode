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
package org.apache.geode.rest.internal.web.controllers;

import static org.apache.geode.distributed.ConfigurationProperties.*;
import static org.junit.Assert.*;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.util.Arrays;
import java.util.Calendar;
import java.util.Collection;
import java.util.HashMap;
import java.util.Map;
import java.util.Properties;

import org.apache.http.HttpEntity;
import org.apache.http.client.ClientProtocolException;
import org.apache.http.client.methods.CloseableHttpResponse;
import org.apache.http.client.methods.HttpDelete;
import org.apache.http.client.methods.HttpGet;
import org.apache.http.client.methods.HttpPost;
import org.apache.http.client.methods.HttpPut;
import org.apache.http.entity.StringEntity;
import org.apache.http.impl.client.CloseableHttpClient;
import org.apache.http.impl.client.HttpClients;
import org.json.JSONArray;
import org.json.JSONException;
import org.json.JSONObject;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import org.apache.geode.cache.AttributesFactory;
import org.apache.geode.cache.Cache;
import org.apache.geode.cache.CacheFactory;
import org.apache.geode.cache.DataPolicy;
import org.apache.geode.cache.Region;
import org.apache.geode.cache.RegionAttributes;
import org.apache.geode.cache.RegionFactory;
import org.apache.geode.cache.RegionShortcut;
import org.apache.geode.cache.client.ClientCache;
import org.apache.geode.cache.client.ClientCacheFactory;
import org.apache.geode.cache.client.ClientRegionFactory;
import org.apache.geode.cache.client.ClientRegionShortcut;
import org.apache.geode.cache.client.internal.LocatorTestBase;
import org.apache.geode.cache.server.CacheServer;
import org.apache.geode.cache.server.ServerLoadProbe;
import org.apache.geode.distributed.DistributedSystem;
import org.apache.geode.internal.AvailablePort;
import org.apache.geode.internal.AvailablePortHelper;
import org.apache.geode.internal.cache.GemFireCacheImpl;
import org.apache.geode.pdx.PdxInstance;
import org.apache.geode.test.dunit.Host;
import org.apache.geode.test.dunit.NetworkUtils;
import org.apache.geode.test.dunit.VM;
import org.apache.geode.test.junit.categories.DistributedTest;
import org.apache.geode.test.junit.runners.CategoryWithParameterizedRunnerFactory;

/**
 * Dunit Test containing inter - operations between REST Client and Gemfire cache client
 *
 * @since GemFire 8.0
 */
@Category(DistributedTest.class)
@RunWith(Parameterized.class)
@Parameterized.UseParametersRunnerFactory(CategoryWithParameterizedRunnerFactory.class)
@SuppressWarnings("serial")
public class RestAPIsAndInterOpsDUnitTest extends LocatorTestBase {

  public static final String PEOPLE_REGION_NAME = "People";

  private static final String findAllPeopleQuery =
      "/queries?id=findAllPeople&q=SELECT%20*%20FROM%20/People";
  private static final String findPeopleByGenderQuery =
      "/queries?id=filterByGender&q=SELECT%20*%20from%20/People%20where%20gender=$1";
  private static final String findPeopleByLastNameQuery =
      "/queries?id=filterByLastName&q=SELECT%20*%20from%20/People%20where%20lastName=$1";

  private static final String[] PARAM_QUERY_IDS_ARRAY =
      {"findAllPeople", "filterByGender", "filterByLastName"};

  final static String QUERY_ARGS =
      "[" + "{" + "\"@type\": \"string\"," + "\"@value\": \"Patel\"" + "}" + "]";

  final static String PERSON_AS_JSON_CAS = "{" + "\"@old\" :" + "{"
      + "\"@type\": \"org.apache.geode.rest.internal.web.controllers.Person\"," + "\"id\": 101,"
      + " \"firstName\": \"Mithali\"," + " \"middleName\": \"Dorai\"," + " \"lastName\": \"Raj\","
      + " \"birthDate\": \"12/04/1982\"," + "\"gender\": \"FEMALE\"" + "}," + "\"@new\" :" + "{"
      + "\"@type\": \"org.apache.geode.rest.internal.web.controllers.Person\"," + "\"id\": 1101,"
      + " \"firstName\": \"Virat\"," + " \"middleName\": \"Premkumar\","
      + " \"lastName\": \"Kohli\"," + " \"birthDate\": \"08/11/1988\"," + "\"gender\": \"MALE\""
      + "}" + "}";

  final static String PERSON_AS_JSON_REPLACE =
      "{" + "\"@type\": \"org.apache.geode.rest.internal.web.controllers.Person\"," + "\"id\": 501,"
          + " \"firstName\": \"Barack\"," + " \"middleName\": \"Hussein\","
          + " \"lastName\": \"Obama\"," + " \"birthDate\": \"04/08/1961\"," + "\"gender\": \"MALE\""
          + "}";

  private static final String PERSON_LIST_AS_JSON = "[" + "{"
      + "\"@type\": \"org.apache.geode.rest.internal.web.controllers.Person\"," + "\"id\": 3,"
      + " \"firstName\": \"Nishka3\"," + " \"middleName\": \"Nilkanth3\","
      + " \"lastName\": \"Patel3\"," + " \"birthDate\": \"07/31/2009\"," + "\"gender\": \"FEMALE\""
      + "}," + "{" + "\"@type\": \"org.apache.geode.rest.internal.web.controllers.Person\","
      + "\"id\": 4," + " \"firstName\": \"Tanay4\"," + " \"middleName\": \"kiran4\","
      + " \"lastName\": \"Patel4\"," + " \"birthDate\": \"23/08/2012\"," + "\"gender\": \"MALE\""
      + "}," + "{" + "\"@type\": \"org.apache.geode.rest.internal.web.controllers.Person\","
      + "\"id\": 5," + " \"firstName\": \"Nishka5\"," + " \"middleName\": \"Nilkanth5\","
      + " \"lastName\": \"Patel5\"," + " \"birthDate\": \"31/09/2009\"," + "\"gender\": \"FEMALE\""
      + "}," + "{" + "\"@type\": \"org.apache.geode.rest.internal.web.controllers.Person\","
      + "\"id\": 6," + " \"firstName\": \"Tanay6\"," + " \"middleName\": \"Kiran6\","
      + " \"lastName\": \"Patel\"," + " \"birthDate\": \"23/08/2012\"," + "\"gender\": \"MALE\""
      + "}," + "{" + "\"@type\": \"org.apache.geode.rest.internal.web.controllers.Person\","
      + "\"id\": 7," + " \"firstName\": \"Nishka7\"," + " \"middleName\": \"Nilkanth7\","
      + " \"lastName\": \"Patel\"," + " \"birthDate\": \"31/09/2009\"," + "\"gender\": \"FEMALE\""
      + "}," + "{" + "\"@type\": \"org.apache.geode.rest.internal.web.controllers.Person\","
      + "\"id\": 8," + " \"firstName\": \"Tanay8\"," + " \"middleName\": \"kiran8\","
      + " \"lastName\": \"Patel\"," + " \"birthDate\": \"23/08/2012\"," + "\"gender\": \"MALE\""
      + "}," + "{" + "\"@type\": \"org.apache.geode.rest.internal.web.controllers.Person\","
      + "\"id\": 9," + " \"firstName\": \"Nishka9\"," + " \"middleName\": \"Nilkanth9\","
      + " \"lastName\": \"Patel\"," + " \"birthDate\": \"31/09/2009\"," + "\"gender\": \"FEMALE\""
      + "}," + "{" + "\"@type\": \"org.apache.geode.rest.internal.web.controllers.Person\","
      + "\"id\": 10," + " \"firstName\": \"Tanay10\"," + " \"middleName\": \"kiran10\","
      + " \"lastName\": \"Patel\"," + " \"birthDate\": \"23/08/2012\"," + "\"gender\": \"MALE\""
      + "}," + "{" + "\"@type\": \"org.apache.geode.rest.internal.web.controllers.Person\","
      + "\"id\": 11," + " \"firstName\": \"Nishka11\"," + " \"middleName\": \"Nilkanth11\","
      + " \"lastName\": \"Patel\"," + " \"birthDate\": \"31/09/2009\"," + "\"gender\": \"FEMALE\""
      + "}," + "{" + "\"@type\": \"org.apache.geode.rest.internal.web.controllers.Person\","
      + "\"id\": 12," + " \"firstName\": \"Tanay12\"," + " \"middleName\": \"kiran12\","
      + " \"lastName\": \"Patel\"," + " \"birthDate\": \"23/08/2012\"," + "\"gender\": \"MALE\""
      + "}" + "]";

  @Parameterized.Parameter
  public String urlContext;

  @Parameterized.Parameters
  public static Collection<String> data() {
    return Arrays.asList("/geode", "/gemfire-api");
  }

  public String startBridgeServerWithRestService(final String hostName, final String[] groups,
      final String locators, final String[] regions, final ServerLoadProbe probe) {
    final int serverPort = AvailablePortHelper.getRandomAvailableTCPPort();

    // create Cache of given VM and start HTTP service with REST APIs service
    startBridgeServer(hostName, serverPort, groups, locators, regions, probe);

    return "http://" + hostName + ":" + serverPort + urlContext + "/v1";
  }

  @SuppressWarnings("deprecation")
  protected int startBridgeServer(String hostName, int restServicerPort, final String[] groups,
      final String locators, final String[] regions, final ServerLoadProbe probe) {

    Properties props = new Properties();
    props.setProperty(MCAST_PORT, String.valueOf(0));
    props.setProperty(LOCATORS, locators);
    props.setProperty(START_DEV_REST_API, "true");
    props.setProperty(HTTP_SERVICE_BIND_ADDRESS, hostName);
    props.setProperty(HTTP_SERVICE_PORT, String.valueOf(restServicerPort));

    DistributedSystem ds = getSystem(props);
    Cache cache = CacheFactory.create(ds);
    ((GemFireCacheImpl) cache).setReadSerialized(true);
    AttributesFactory factory = new AttributesFactory();

    factory.setEnableBridgeConflation(true);
    factory.setDataPolicy(DataPolicy.REPLICATE);
    RegionAttributes attrs = factory.create();
    for (int i = 0; i < regions.length; i++) {
      cache.createRegion(regions[i], attrs);
    }

    CacheServer server = cache.addCacheServer();
    server.setPort(0);
    server.setGroups(groups);
    server.setLoadProbe(probe);
    try {
      server.start();
    } catch (IOException e) {
      e.printStackTrace();
    }
    remoteObjects.put(CACHE_KEY, cache);
    return new Integer(server.getPort());
  }

  public void doPutsInClientCache() {
    ClientCache cache = GemFireCacheImpl.getInstance();
    assertNotNull(cache);
    Region<String, Object> region = cache.getRegion(PEOPLE_REGION_NAME);

    // put person object
    final Person person1 = new Person(101L, "Mithali", "Dorai", "Raj",
        DateTimeUtils.createDate(1982, Calendar.DECEMBER, 4), Gender.FEMALE);
    final Person person2 = new Person(102L, "Sachin", "Ramesh", "Tendulkar",
        DateTimeUtils.createDate(1975, Calendar.DECEMBER, 14), Gender.MALE);
    final Person person3 = new Person(103L, "Saurabh", "Baburav", "Ganguly",
        DateTimeUtils.createDate(1972, Calendar.AUGUST, 29), Gender.MALE);
    final Person person4 = new Person(104L, "Rahul", "subrymanyam", "Dravid",
        DateTimeUtils.createDate(1979, Calendar.MARCH, 17), Gender.MALE);
    final Person person5 = new Person(105L, "Jhulan", "Chidambaram", "Goswami",
        DateTimeUtils.createDate(1983, Calendar.NOVEMBER, 25), Gender.FEMALE);

    region.put("1", person1);
    region.put("2", person2);
    region.put("3", person3);
    region.put("4", person4);
    region.put("5", person5);

    final Person person6 = new Person(101L, "Rahul", "Rajiv", "Gndhi",
        DateTimeUtils.createDate(1970, Calendar.MAY, 14), Gender.MALE);
    final Person person7 = new Person(102L, "Narendra", "Damodar", "Modi",
        DateTimeUtils.createDate(1945, Calendar.DECEMBER, 24), Gender.MALE);
    final Person person8 = new Person(103L, "Atal", "Bihari", "Vajpayee",
        DateTimeUtils.createDate(1920, Calendar.AUGUST, 9), Gender.MALE);
    final Person person9 = new Person(104L, "Soniya", "Rajiv", "Gandhi",
        DateTimeUtils.createDate(1929, Calendar.MARCH, 27), Gender.FEMALE);
    final Person person10 = new Person(104L, "Priyanka", "Robert", "Gandhi",
        DateTimeUtils.createDate(1973, Calendar.APRIL, 15), Gender.FEMALE);

    final Person person11 = new Person(104L, "Murali", "Manohar", "Joshi",
        DateTimeUtils.createDate(1923, Calendar.APRIL, 25), Gender.MALE);
    final Person person12 = new Person(104L, "Lalkrishna", "Parmhansh", "Advani",
        DateTimeUtils.createDate(1910, Calendar.JANUARY, 01), Gender.MALE);
    final Person person13 = new Person(104L, "Shushma", "kumari", "Swaraj",
        DateTimeUtils.createDate(1943, Calendar.AUGUST, 10), Gender.FEMALE);
    final Person person14 = new Person(104L, "Arun", "raman", "jetly",
        DateTimeUtils.createDate(1942, Calendar.OCTOBER, 27), Gender.MALE);
    final Person person15 = new Person(104L, "Amit", "kumar", "shah",
        DateTimeUtils.createDate(1958, Calendar.DECEMBER, 21), Gender.MALE);
    final Person person16 = new Person(104L, "Shila", "kumari", "Dixit",
        DateTimeUtils.createDate(1927, Calendar.FEBRUARY, 15), Gender.FEMALE);

    Map<String, Object> userMap = new HashMap<String, Object>();
    userMap.put("6", person6);
    userMap.put("7", person7);
    userMap.put("8", person8);
    userMap.put("9", person9);
    userMap.put("10", person10);
    userMap.put("11", person11);
    userMap.put("12", person12);
    userMap.put("13", person13);
    userMap.put("14", person14);
    userMap.put("15", person15);
    userMap.put("16", person16);

    region.putAll(userMap);

    if (cache != null)
      cache.getLogger().info("Gemfire Cache Client: Puts successfully done");

  }

  public void doQueryOpsUsingRestApis(String restEndpoint) {
    String currentQueryOp = null;
    try {
      // Query TestCase-1 :: Prepare parameterized Queries
      {
        currentQueryOp = "findAllPeopleQuery";
        CloseableHttpClient httpclient = HttpClients.createDefault();
        HttpPost post = new HttpPost(restEndpoint + findAllPeopleQuery);
        post.addHeader("Content-Type", "application/json");
        post.addHeader("Accept", "application/json");
        CloseableHttpResponse createNamedQueryResponse = httpclient.execute(post);
        assertEquals(createNamedQueryResponse.getStatusLine().getStatusCode(), 201);
        assertNotNull(createNamedQueryResponse.getEntity());
        createNamedQueryResponse.close();

        post = new HttpPost(restEndpoint + findPeopleByGenderQuery);
        post.addHeader("Content-Type", "application/json");
        post.addHeader("Accept", "application/json");
        createNamedQueryResponse = httpclient.execute(post);
        assertEquals(createNamedQueryResponse.getStatusLine().getStatusCode(), 201);
        assertNotNull(createNamedQueryResponse.getEntity());
        createNamedQueryResponse.close();

        post = new HttpPost(restEndpoint + findPeopleByLastNameQuery);
        post.addHeader("Content-Type", "application/json");
        post.addHeader("Accept", "application/json");
        createNamedQueryResponse = httpclient.execute(post);
        assertEquals(createNamedQueryResponse.getStatusLine().getStatusCode(), 201);
        assertNotNull(createNamedQueryResponse.getEntity());
        createNamedQueryResponse.close();
      }

      // Query TestCase-2 :: List all parameterized queries
      {
        currentQueryOp = "listAllQueries";
        HttpGet get = new HttpGet(restEndpoint + "/queries");
        CloseableHttpClient httpclient = HttpClients.createDefault();
        CloseableHttpResponse listAllQueriesResponse = httpclient.execute(get);
        assertEquals(listAllQueriesResponse.getStatusLine().getStatusCode(), 200);
        assertNotNull(listAllQueriesResponse.getEntity());

        HttpEntity entity = listAllQueriesResponse.getEntity();
        InputStream content = entity.getContent();
        BufferedReader reader = new BufferedReader(new InputStreamReader(content));
        String line;
        StringBuffer sb = new StringBuffer();
        while ((line = reader.readLine()) != null) {
          sb.append(line);
        }
        listAllQueriesResponse.close();

        // Check whether received response contains expected query IDs.

        JSONObject jsonObject = new JSONObject(sb.toString());
        JSONArray jsonArray = jsonObject.getJSONArray("queries");
        for (int i = 0; i < jsonArray.length(); i++) {
          assertTrue("PREPARE_PARAMETERIZED_QUERY: function IDs are not matched", Arrays
              .asList(PARAM_QUERY_IDS_ARRAY).contains(jsonArray.getJSONObject(i).getString("id")));
        }
      }

      // Query TestCase-3 :: Run the specified named query passing in scalar values for query
      // parameters.
      {
        currentQueryOp = "filterByLastName";
        CloseableHttpClient httpclient = HttpClients.createDefault();
        HttpPost post = new HttpPost(restEndpoint + "/queries/filterByLastName");
        post.addHeader("Content-Type", "application/json");
        post.addHeader("Accept", "application/json");
        StringEntity entity = new StringEntity(QUERY_ARGS);
        post.setEntity(entity);
        CloseableHttpResponse runNamedQueryResponse = httpclient.execute(post);

        assertEquals(200, runNamedQueryResponse.getStatusLine().getStatusCode());
        assertNotNull(runNamedQueryResponse.getEntity());
      }
    } catch (Exception e) {
      throw new RuntimeException("unexpected exception", e);
    }
  }

  public void verifyUpdatesInClientCache() {
    ClientCache cache = GemFireCacheImpl.getInstance();
    assertNotNull(cache);
    Region<String, Object> region = cache.getRegion(PEOPLE_REGION_NAME);

    {
      Person expectedPerson = new Person(3L, "Nishka3", "Nilkanth3", "Patel3",
          DateTimeUtils.createDate(2009, Calendar.JULY, 31), Gender.FEMALE);
      Object value = region.get("3");
      if (value instanceof PdxInstance) {
        PdxInstance pi3 = (PdxInstance) value;
        Person actualPerson = (Person) pi3.getObject();
        assertEquals(actualPerson.getId(), expectedPerson.getId());
        assertEquals(actualPerson.getFirstName(), expectedPerson.getFirstName());
        assertEquals(actualPerson.getMiddleName(), expectedPerson.getMiddleName());
        assertEquals(actualPerson.getLastName(), expectedPerson.getLastName());
        assertEquals(actualPerson.getBirthDate(), expectedPerson.getBirthDate());
        assertEquals(actualPerson.getGender(), expectedPerson.getGender());
      } else if (value instanceof Person) {
        fail(
            "VerifyUpdatesInClientCache, Get on key 3, Expected to get value of type PdxInstance ");
      }
    }

    // TODO: uncomment it once following issue encountered in put?op=CAS is fixed or document the
    // issue
    // CAS functionality is not working in following test case
    // step-1: Java client, Region.put("K", A);
    // Step-2: Rest CAS request for key "K" with data "@old" = A. CAS is failing as existing
    // PdxInstance in cache and
    // PdxInstance generated from JSON (CAS request) does not match as their value's type are
    // getting changed
    /*
     * //verify update on key "1" { Object obj = region.get("1"); if (obj instanceof PdxInstance) {
     * PdxInstance pi = (PdxInstance)obj; Person p1 = (Person)pi.getObject();
     * System.out.println("Nilkanth1 : verifyUpdatesInClientCache() : GET ON KEY=1" +
     * p1.toString()); }else {
     * System.out.println("Nilkanth1 : verifyUpdatesInClientCache() GET ON KEY=1  returned OBJECT: "
     * + obj.toString()); } }
     */

    // verify update on key "2"
    {
      Person expectedPerson = new Person(501L, "Barack", "Hussein", "Obama",
          DateTimeUtils.createDate(1961, Calendar.APRIL, 8), Gender.MALE);
      Object value = region.get("2");
      if (value instanceof PdxInstance) {
        PdxInstance pi3 = (PdxInstance) value;
        Person actualPerson = (Person) pi3.getObject();
        assertEquals(actualPerson.getId(), expectedPerson.getId());
        assertEquals(actualPerson.getFirstName(), expectedPerson.getFirstName());
        assertEquals(actualPerson.getMiddleName(), expectedPerson.getMiddleName());
        assertEquals(actualPerson.getLastName(), expectedPerson.getLastName());
        assertEquals(actualPerson.getBirthDate(), expectedPerson.getBirthDate());
        assertEquals(actualPerson.getGender(), expectedPerson.getGender());
      } else {
        fail(
            "VerifyUpdatesInClientCache, Get on key 2, Expected to get value of type PdxInstance ");
      }
    }

    // verify Deleted key "13"
    {
      Object obj = region.get("13");
      assertEquals(obj, null);

      obj = region.get("14");
      assertEquals(obj, null);

      obj = region.get("15");
      assertEquals(obj, null);

      obj = region.get("16");
      assertEquals(obj, null);
    }

  }

  public void doUpdatesUsingRestApis(String restEndpoint) {
    // UPdate keys using REST calls
    {

      try {
        CloseableHttpClient httpclient = HttpClients.createDefault();
        HttpPut put = new HttpPut(restEndpoint + "/People/3,4,5,6,7,8,9,10,11,12");
        put.addHeader("Content-Type", "application/json");
        put.addHeader("Accept", "application/json");
        StringEntity entity = new StringEntity(PERSON_LIST_AS_JSON);
        put.setEntity(entity);
        CloseableHttpResponse result = httpclient.execute(put);
      } catch (Exception e) {
        throw new RuntimeException("unexpected exception", e);
      }
    }

    // Delete Single keys
    {
      try {
        CloseableHttpClient httpclient = HttpClients.createDefault();
        HttpDelete delete = new HttpDelete(restEndpoint + "/People/13");
        delete.addHeader("Content-Type", "application/json");
        delete.addHeader("Accept", "application/json");
        CloseableHttpResponse result = httpclient.execute(delete);
      } catch (Exception e) {
        throw new RuntimeException("unexpected exception", e);
      }
    }

    // Delete set of keys
    {
      try {
        CloseableHttpClient httpclient = HttpClients.createDefault();
        HttpDelete delete = new HttpDelete(restEndpoint + "/People/14,15,16");
        delete.addHeader("Content-Type", "application/json");
        delete.addHeader("Accept", "application/json");
        CloseableHttpResponse result = httpclient.execute(delete);
      } catch (Exception e) {
        throw new RuntimeException("unexpected exception", e);
      }
    }

    // REST put?op=CAS for key 1
    /*
     * try { { HttpEntity<Object> entity = new HttpEntity<Object>(PERSON_AS_JSON_CAS, headers);
     * ResponseEntity<String> result = RestTestUtils.getRestTemplate().exchange( restEndpoint +
     * "/People/1?op=cas", HttpMethod.PUT, entity, String.class); } } catch
     * (HttpClientErrorException e) {
     * 
     * fail("Caught HttpClientErrorException while doing put with op=cas"); }catch
     * (HttpServerErrorException se) {
     * fail("Caught HttpServerErrorException while doing put with op=cas"); }
     */

    // REST put?op=REPLACE for key 2
    {
      /*
       * HttpEntity<Object> entity = new HttpEntity<Object>(PERSON_AS_JSON_REPLACE, headers);
       * ResponseEntity<String> result = RestTestUtils.getRestTemplate().exchange( restEndpoint +
       * "/People/2?op=replace", HttpMethod.PUT, entity, String.class);
       */

      try {
        CloseableHttpClient httpclient = HttpClients.createDefault();
        HttpPut put = new HttpPut(restEndpoint + "/People/2?op=replace");
        put.addHeader("Content-Type", "application/json");
        put.addHeader("Accept", "application/json");
        StringEntity entity = new StringEntity(PERSON_AS_JSON_REPLACE);
        put.setEntity(entity);
        CloseableHttpResponse result = httpclient.execute(put);
      } catch (Exception e) {
        throw new RuntimeException("unexpected exception", e);
      }
    }
  }

  public void fetchRestServerEndpoints(String restEndpoint) {
    HttpGet get = new HttpGet(restEndpoint + "/servers");
    get.addHeader("Content-Type", "application/json");
    get.addHeader("Accept", "application/json");
    CloseableHttpClient httpclient = HttpClients.createDefault();
    CloseableHttpResponse response;

    try {
      response = httpclient.execute(get);
      HttpEntity entity = response.getEntity();
      InputStream content = entity.getContent();
      BufferedReader reader = new BufferedReader(new InputStreamReader(content));
      String line;
      StringBuffer str = new StringBuffer();
      while ((line = reader.readLine()) != null) {
        str.append(line);
      }

      // validate the satus code
      assertEquals(response.getStatusLine().getStatusCode(), 200);

      if (response.getStatusLine().getStatusCode() == 200) {
        JSONArray jsonArray = new JSONArray(str.toString());

        // verify total number of REST service endpoints in DS
        assertEquals(jsonArray.length(), 2);
      }

    } catch (ClientProtocolException e) {
      e.printStackTrace();
      fail(" Rest Request should not have thrown ClientProtocolException!");
    } catch (IOException e) {
      e.printStackTrace();
      fail(" Rest Request should not have thrown IOException!");
    } catch (JSONException e) {
      e.printStackTrace();
      fail(" Rest Request should not have thrown  JSONException!");
    }

  }

  public void doGetsUsingRestApis(String restEndpoint) {

    // HttpHeaders headers = setAcceptAndContentTypeHeaders();
    String currentOperation = null;
    JSONObject jObject;
    JSONArray jArray;
    try {
      // 1. Get on key="1" and validate result.
      {
        currentOperation = "GET on key 1";

        HttpGet get = new HttpGet(restEndpoint + "/People/1");
        get.addHeader("Content-Type", "application/json");
        get.addHeader("Accept", "application/json");
        CloseableHttpClient httpclient = HttpClients.createDefault();
        CloseableHttpResponse response = httpclient.execute(get);

        HttpEntity entity = response.getEntity();
        InputStream content = entity.getContent();
        BufferedReader reader = new BufferedReader(new InputStreamReader(content));
        String line;
        StringBuffer str = new StringBuffer();
        while ((line = reader.readLine()) != null) {
          str.append(line);
        }

        jObject = new JSONObject(str.toString());

        assertEquals(jObject.get("id"), 101);
        assertEquals(jObject.get("firstName"), "Mithali");
        assertEquals(jObject.get("middleName"), "Dorai");
        assertEquals(jObject.get("lastName"), "Raj");
        assertEquals(jObject.get("gender"), Gender.FEMALE.name());
      }

      // 2. Get on key="16" and validate result.
      {
        currentOperation = "GET on key 16";

        HttpGet get = new HttpGet(restEndpoint + "/People/16");
        get.addHeader("Content-Type", "application/json");
        get.addHeader("Accept", "application/json");
        CloseableHttpClient httpclient = HttpClients.createDefault();
        CloseableHttpResponse response = httpclient.execute(get);

        HttpEntity entity = response.getEntity();
        InputStream content = entity.getContent();
        BufferedReader reader = new BufferedReader(new InputStreamReader(content));
        String line;
        StringBuffer str = new StringBuffer();
        while ((line = reader.readLine()) != null) {
          str.append(line);
        }

        jObject = new JSONObject(str.toString());

        assertEquals(jObject.get("id"), 104);
        assertEquals(jObject.get("firstName"), "Shila");
        assertEquals(jObject.get("middleName"), "kumari");
        assertEquals(jObject.get("lastName"), "Dixit");
        assertEquals(jObject.get("gender"), Gender.FEMALE.name());
      }

      // 3. Get all (getAll) entries in Region
      {

        HttpGet get = new HttpGet(restEndpoint + "/People");
        get.addHeader("Content-Type", "application/json");
        get.addHeader("Accept", "application/json");
        CloseableHttpClient httpclient = HttpClients.createDefault();
        CloseableHttpResponse result = httpclient.execute(get);
        assertEquals(result.getStatusLine().getStatusCode(), 200);
        assertNotNull(result.getEntity());

        HttpEntity entity = result.getEntity();
        InputStream content = entity.getContent();
        BufferedReader reader = new BufferedReader(new InputStreamReader(content));
        String line;
        StringBuffer sb = new StringBuffer();
        while ((line = reader.readLine()) != null) {
          sb.append(line);
        }
        result.close();

        try {
          jObject = new JSONObject(sb.toString());
          jArray = jObject.getJSONArray("People");
          assertEquals(jArray.length(), 16);
        } catch (JSONException e) {
          fail(" Rest Request ::" + currentOperation + " :: should not have thrown JSONException ");
        }
      }

      // 4. GetAll?limit=10 (10 entries) and verify results
      {
        HttpGet get = new HttpGet(restEndpoint + "/People?limit=10");
        get.addHeader("Content-Type", "application/json");
        get.addHeader("Accept", "application/json");
        CloseableHttpClient httpclient = HttpClients.createDefault();
        CloseableHttpResponse response = httpclient.execute(get);
        assertEquals(response.getStatusLine().getStatusCode(), 200);
        assertNotNull(response.getEntity());

        HttpEntity entity = response.getEntity();
        InputStream content = entity.getContent();
        BufferedReader reader = new BufferedReader(new InputStreamReader(content));
        String line;
        StringBuffer str = new StringBuffer();
        while ((line = reader.readLine()) != null) {
          str.append(line);
        }

        try {
          jObject = new JSONObject(str.toString());
          jArray = jObject.getJSONArray("People");
          assertEquals(jArray.length(), 10);
        } catch (JSONException e) {
          fail(" Rest Request ::" + currentOperation + " :: should not have thrown JSONException ");
        }
      }

      // 5. Get keys - List all keys in region
      {

        HttpGet get = new HttpGet(restEndpoint + "/People/keys");
        get.addHeader("Content-Type", "application/json");
        get.addHeader("Accept", "application/json");
        CloseableHttpClient httpclient = HttpClients.createDefault();
        CloseableHttpResponse response = httpclient.execute(get);
        assertEquals(response.getStatusLine().getStatusCode(), 200);
        assertNotNull(response.getEntity());

        HttpEntity entity = response.getEntity();
        InputStream content = entity.getContent();
        BufferedReader reader = new BufferedReader(new InputStreamReader(content));
        String line;
        StringBuffer str = new StringBuffer();
        while ((line = reader.readLine()) != null) {
          str.append(line);
        }

        try {
          jObject = new JSONObject(str.toString());
          jArray = jObject.getJSONArray("keys");
          assertEquals(jArray.length(), 16);
        } catch (JSONException e) {
          fail(" Rest Request ::" + currentOperation + " :: should not have thrown JSONException ");
        }
      }

      // 6. Get data for specific keys
      {

        HttpGet get = new HttpGet(restEndpoint + "/People/1,3,5,7,9,11");
        get.addHeader("Content-Type", "application/json");
        get.addHeader("Accept", "application/json");
        CloseableHttpClient httpclient = HttpClients.createDefault();
        CloseableHttpResponse response = httpclient.execute(get);
        assertEquals(response.getStatusLine().getStatusCode(), 200);
        assertNotNull(response.getEntity());

        HttpEntity entity = response.getEntity();
        InputStream content = entity.getContent();
        BufferedReader reader = new BufferedReader(new InputStreamReader(content));
        String line;
        StringBuffer str = new StringBuffer();
        while ((line = reader.readLine()) != null) {
          str.append(line);
        }

        try {
          jObject = new JSONObject(str.toString());
          jArray = jObject.getJSONArray("People");
          assertEquals(jArray.length(), 6);

        } catch (JSONException e) {
          fail(" Rest Request ::" + currentOperation + " :: should not have thrown JSONException ");
        }
      }
    } catch (Exception e) {
      throw new RuntimeException("unexpected exception", e);
    }
  }

  public void createRegionInClientCache() {
    ClientCache cache = GemFireCacheImpl.getInstance();
    assertNotNull(cache);
    ClientRegionFactory<String, Object> crf =
        cache.createClientRegionFactory(ClientRegionShortcut.PROXY);
    Region<String, Object> region = crf.create(PEOPLE_REGION_NAME);

  }

  public void createRegion() {
    Cache cache = GemFireCacheImpl.getInstance();
    assertNotNull(cache);

    RegionFactory<String, Object> rf = cache.createRegionFactory(RegionShortcut.REPLICATE);
    Region<String, Object> region = rf.create(PEOPLE_REGION_NAME);
  }

  /**
   * InterOps Test between REST-client, Peer Cache Client and Client Cache
   *
   * @throws Exception
   */

  @Test
  public void testInterOpsWithReplicatedRegion() throws Exception {

    final Host host = Host.getHost(0);
    VM locator = host.getVM(0);
    VM manager = host.getVM(1);
    VM server = host.getVM(2);
    VM client = host.getVM(3);

    // start locator
    int locatorPort = AvailablePort.getRandomAvailablePort(AvailablePort.SOCKET);
    final String locatorHostName = NetworkUtils.getServerHostName(locator.getHost());
    locator.invoke(() -> startLocator(locatorHostName, locatorPort, ""));

    // find locators
    String locators = locatorHostName + "[" + locatorPort + "]";

    // start manager (peer cache)
    manager.invoke(() -> startManager(/* groups */null, locators, new String[] {REGION_NAME},
        CacheServer.DEFAULT_LOAD_PROBE));

    // start startCacheServer With RestService enabled
    final String serverHostName = server.getHost().getHostName();
    String restEndpoint =
        (String) server.invoke(() -> startBridgeServerWithRestService(serverHostName, null,
            locators, new String[] {REGION_NAME}, CacheServer.DEFAULT_LOAD_PROBE));

    // create a client cache
    client.invoke(() -> createClientCache(locatorHostName, locatorPort));

    // create region in Manager, peer cache and Client cache nodes
    manager.invoke(() -> createRegion());
    server.invoke(() -> createRegion());
    client.invoke(() -> createRegionInClientCache());

    // do some person puts from clientcache
    client.invoke(() -> doPutsInClientCache());

    // TEST: fetch all available REST endpoints
    fetchRestServerEndpoints(restEndpoint);

    // Controller VM - config REST Client and make HTTP calls
    doGetsUsingRestApis(restEndpoint);

    // update Data using REST APIs
    doUpdatesUsingRestApis(restEndpoint);

    client.invoke(() -> verifyUpdatesInClientCache());

    // Querying
    doQueryOpsUsingRestApis(restEndpoint);
  }

  private void createClientCache(final String host, final int port) throws Exception {
    // Connect using the GemFire locator and create a Caching_Proxy cache
    ClientCache c =
        new ClientCacheFactory().setPdxReadSerialized(true).addPoolLocator(host, port).create();

    c.createClientRegionFactory(ClientRegionShortcut.PROXY).create(REGION_NAME);
  }

  private int startManager(final String[] groups, final String locators, final String[] regions,
      final ServerLoadProbe probe) throws IOException {
    Properties props = new Properties();
    props.setProperty(MCAST_PORT, String.valueOf(0));
    props.setProperty(LOCATORS, locators);

    props.setProperty(JMX_MANAGER, "true");
    props.setProperty(JMX_MANAGER_START, "true");
    props.setProperty(JMX_MANAGER_PORT, "0");

    final int httpPort = AvailablePortHelper.getRandomAvailableTCPPort();
    // Set REST service related configuration
    props.setProperty(START_DEV_REST_API, "true");
    props.setProperty(HTTP_SERVICE_BIND_ADDRESS, "localhost");
    props.setProperty(HTTP_SERVICE_PORT, String.valueOf(httpPort));

    DistributedSystem ds = getSystem(props);
    Cache cache = CacheFactory.create(ds);
    AttributesFactory factory = new AttributesFactory();

    factory.setEnableBridgeConflation(true);
    factory.setDataPolicy(DataPolicy.REPLICATE);
    RegionAttributes attrs = factory.create();
    for (int i = 0; i < regions.length; i++) {
      cache.createRegion(regions[i], attrs);
    }
    CacheServer server = cache.addCacheServer();
    server.setPort(0);
    server.setGroups(groups);
    server.setLoadProbe(probe);
    server.start();

    return new Integer(server.getPort());
  }
}
