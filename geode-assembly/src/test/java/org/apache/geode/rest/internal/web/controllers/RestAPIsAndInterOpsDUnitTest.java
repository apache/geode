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

import static org.apache.geode.distributed.ConfigurationProperties.HTTP_SERVICE_BIND_ADDRESS;
import static org.apache.geode.distributed.ConfigurationProperties.HTTP_SERVICE_PORT;
import static org.apache.geode.distributed.ConfigurationProperties.JMX_MANAGER;
import static org.apache.geode.distributed.ConfigurationProperties.JMX_MANAGER_PORT;
import static org.apache.geode.distributed.ConfigurationProperties.JMX_MANAGER_START;
import static org.apache.geode.distributed.ConfigurationProperties.LOCATORS;
import static org.apache.geode.distributed.ConfigurationProperties.MCAST_PORT;
import static org.apache.geode.distributed.ConfigurationProperties.START_DEV_REST_API;
import static org.assertj.core.api.Assertions.assertThat;

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
import org.apache.geode.internal.cache.InternalCache;
import org.apache.geode.pdx.PdxInstance;
import org.apache.geode.test.dunit.Host;
import org.apache.geode.test.dunit.NetworkUtils;
import org.apache.geode.test.dunit.VM;
import org.apache.geode.test.junit.categories.DistributedTest;
import org.apache.geode.test.junit.runners.CategoryWithParameterizedRunnerFactory;
import org.apache.http.HttpEntity;
import org.apache.http.client.methods.CloseableHttpResponse;
import org.apache.http.client.methods.HttpDelete;
import org.apache.http.client.methods.HttpGet;
import org.apache.http.client.methods.HttpPost;
import org.apache.http.client.methods.HttpPut;
import org.apache.http.entity.StringEntity;
import org.apache.http.impl.client.CloseableHttpClient;
import org.apache.http.impl.client.HttpClients;
import org.json.JSONArray;
import org.json.JSONObject;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

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

/**
 * Dunit Test containing inter - operations between REST Client and Gemfire cache client
 *
 * @since GemFire 8.0
 */
@Category(DistributedTest.class)
@RunWith(Parameterized.class)
@Parameterized.UseParametersRunnerFactory(CategoryWithParameterizedRunnerFactory.class)
public class RestAPIsAndInterOpsDUnitTest extends LocatorTestBase {

  private static final String PEOPLE_REGION_NAME = "People";

  private static final String findAllPeopleQuery =
      "/queries?id=findAllPeople&q=SELECT%20*%20FROM%20/People";
  private static final String findPeopleByGenderQuery =
      "/queries?id=filterByGender&q=SELECT%20*%20from%20/People%20where%20gender=$1";
  private static final String findPeopleByLastNameQuery =
      "/queries?id=filterByLastName&q=SELECT%20*%20from%20/People%20where%20lastName=$1";

  private static final String[] PARAM_QUERY_IDS_ARRAY =
      {"findAllPeople", "filterByGender", "filterByLastName"};

  private static final String QUERY_ARGS =
      "[" + "{" + "\"@type\": \"string\"," + "\"@value\": \"Patel\"" + "}" + "]";

  private static final String PERSON_AS_JSON_REPLACE =
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

  private String startBridgeServerWithRestService(final String hostName, final String[] groups,
      final String locators, final String[] regions, final ServerLoadProbe probe)
      throws IOException {
    final int serverPort = AvailablePortHelper.getRandomAvailableTCPPort();

    // create Cache of given VM and start HTTP service with REST APIs service
    startBridgeServer(hostName, serverPort, groups, locators, regions, probe);

    return "http://" + hostName + ":" + serverPort + this.urlContext + "/v1";
  }

  private int startBridgeServer(String hostName, int restServicerPort, final String[] groups,
      final String locators, final String[] regions, final ServerLoadProbe probe)
      throws IOException {

    Properties props = new Properties();
    props.setProperty(MCAST_PORT, String.valueOf(0));
    props.setProperty(LOCATORS, locators);
    props.setProperty(START_DEV_REST_API, "true");
    props.setProperty(HTTP_SERVICE_BIND_ADDRESS, hostName);
    props.setProperty(HTTP_SERVICE_PORT, String.valueOf(restServicerPort));

    DistributedSystem ds = getSystem(props);
    InternalCache cache = (InternalCache) CacheFactory.create(ds);
    cache.setReadSerialized(true);
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

    remoteObjects.put(CACHE_KEY, cache);
    return server.getPort();
  }

  private void doPutsInClientCache() {
    ClientCache cache = GemFireCacheImpl.getInstance();
    assertThat(cache).isNotNull();
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

    cache.getLogger().info("Gemfire Cache Client: Puts successfully done");
  }

  private void doQueryOpsUsingRestApis(String restEndpoint) throws IOException {
    // Query TestCase-1 :: Prepare parameterized Queries

    CloseableHttpClient httpclient = HttpClients.createDefault();
    HttpPost post = new HttpPost(restEndpoint + findAllPeopleQuery);
    post.addHeader("Content-Type", "application/json");
    post.addHeader("Accept", "application/json");
    CloseableHttpResponse createNamedQueryResponse = httpclient.execute(post);
    assertThat(createNamedQueryResponse.getStatusLine().getStatusCode()).isEqualTo(201);
    assertThat(createNamedQueryResponse.getEntity()).isNotNull();
    createNamedQueryResponse.close();

    post = new HttpPost(restEndpoint + findPeopleByGenderQuery);
    post.addHeader("Content-Type", "application/json");
    post.addHeader("Accept", "application/json");
    createNamedQueryResponse = httpclient.execute(post);
    assertThat(createNamedQueryResponse.getStatusLine().getStatusCode()).isEqualTo(201);
    assertThat(createNamedQueryResponse.getEntity()).isNotNull();
    createNamedQueryResponse.close();

    post = new HttpPost(restEndpoint + findPeopleByLastNameQuery);
    post.addHeader("Content-Type", "application/json");
    post.addHeader("Accept", "application/json");
    createNamedQueryResponse = httpclient.execute(post);
    assertThat(createNamedQueryResponse.getStatusLine().getStatusCode()).isEqualTo(201);
    assertThat(createNamedQueryResponse.getEntity()).isNotNull();
    createNamedQueryResponse.close();

    // Query TestCase-2 :: List all parameterized queries

    HttpGet get = new HttpGet(restEndpoint + "/queries");
    httpclient = HttpClients.createDefault();
    CloseableHttpResponse listAllQueriesResponse = httpclient.execute(get);
    assertThat(listAllQueriesResponse.getStatusLine().getStatusCode()).isEqualTo(200);
    assertThat(listAllQueriesResponse.getEntity()).isNotNull();

    HttpEntity entity = listAllQueriesResponse.getEntity();
    InputStream content = entity.getContent();
    BufferedReader reader = new BufferedReader(new InputStreamReader(content));
    String line;
    StringBuilder sb = new StringBuilder();
    while ((line = reader.readLine()) != null) {
      sb.append(line);
    }
    listAllQueriesResponse.close();

    // Check whether received response contains expected query IDs.

    JSONObject jsonObject = new JSONObject(sb.toString());
    JSONArray jsonArray = jsonObject.getJSONArray("queries");
    for (int i = 0; i < jsonArray.length(); i++) {
      assertThat(Arrays.asList(PARAM_QUERY_IDS_ARRAY))
          .contains(jsonArray.getJSONObject(i).getString("id"));
    }

    // Query TestCase-3 :: Run the specified named query passing in scalar values for query
    // parameters.

    httpclient = HttpClients.createDefault();
    post = new HttpPost(restEndpoint + "/queries/filterByLastName");
    post.addHeader("Content-Type", "application/json");
    post.addHeader("Accept", "application/json");
    entity = new StringEntity(QUERY_ARGS);
    post.setEntity(entity);
    CloseableHttpResponse runNamedQueryResponse = httpclient.execute(post);

    assertThat(runNamedQueryResponse.getStatusLine().getStatusCode()).isEqualTo(200);
    assertThat(runNamedQueryResponse.getEntity()).isNotNull();
  }

  private void verifyUpdatesInClientCache() {
    ClientCache cache = GemFireCacheImpl.getInstance();
    assertThat(cache).isNotNull();
    Region<String, Object> region = cache.getRegion(PEOPLE_REGION_NAME);

    Person expectedPerson = new Person(3L, "Nishka3", "Nilkanth3", "Patel3",
        DateTimeUtils.createDate(2009, Calendar.JULY, 31), Gender.FEMALE);
    Object value = region.get("3");

    assertThat(value).isInstanceOf(PdxInstance.class);

    PdxInstance pi3 = (PdxInstance) value;
    Person actualPerson = (Person) pi3.getObject();
    assertThat(actualPerson.getId()).isEqualTo(expectedPerson.getId());
    assertThat(actualPerson.getFirstName()).isEqualTo(expectedPerson.getFirstName());
    assertThat(actualPerson.getMiddleName()).isEqualTo(expectedPerson.getMiddleName());
    assertThat(actualPerson.getLastName()).isEqualTo(expectedPerson.getLastName());
    assertThat(actualPerson.getBirthDate()).isEqualTo(expectedPerson.getBirthDate());
    assertThat(actualPerson.getGender()).isEqualTo(expectedPerson.getGender());

    // verify update on key "2"

    expectedPerson = new Person(501L, "Barack", "Hussein", "Obama",
        DateTimeUtils.createDate(1961, Calendar.APRIL, 8), Gender.MALE);
    value = region.get("2");

    assertThat(value).isInstanceOf(PdxInstance.class);

    pi3 = (PdxInstance) value;
    actualPerson = (Person) pi3.getObject();
    assertThat(actualPerson.getId()).isEqualTo(expectedPerson.getId());
    assertThat(actualPerson.getFirstName()).isEqualTo(expectedPerson.getFirstName());
    assertThat(actualPerson.getMiddleName()).isEqualTo(expectedPerson.getMiddleName());
    assertThat(actualPerson.getLastName()).isEqualTo(expectedPerson.getLastName());
    assertThat(actualPerson.getBirthDate()).isEqualTo(expectedPerson.getBirthDate());
    assertThat(actualPerson.getGender()).isEqualTo(expectedPerson.getGender());

    // verify Deleted key "13"

    Object obj = region.get("13");
    assertThat(obj).isNull();

    obj = region.get("14");
    assertThat(obj).isNull();

    obj = region.get("15");
    assertThat(obj).isNull();

    obj = region.get("16");
    assertThat(obj).isNull();
  }

  private void doUpdatesUsingRestApis(String restEndpoint) throws IOException {
    // Update keys using REST calls

    CloseableHttpClient httpclient = HttpClients.createDefault();
    HttpPut put = new HttpPut(restEndpoint + "/People/3,4,5,6,7,8,9,10,11,12");
    put.addHeader("Content-Type", "application/json");
    put.addHeader("Accept", "application/json");
    StringEntity entity = new StringEntity(PERSON_LIST_AS_JSON);
    put.setEntity(entity);
    CloseableHttpResponse result = httpclient.execute(put);
    assertThat(result).isNotNull();

    // Delete Single keys

    httpclient = HttpClients.createDefault();
    HttpDelete delete = new HttpDelete(restEndpoint + "/People/13");
    delete.addHeader("Content-Type", "application/json");
    delete.addHeader("Accept", "application/json");
    result = httpclient.execute(delete);
    assertThat(result).isNotNull();

    // Delete set of keys

    httpclient = HttpClients.createDefault();
    delete = new HttpDelete(restEndpoint + "/People/14,15,16");
    delete.addHeader("Content-Type", "application/json");
    delete.addHeader("Accept", "application/json");
    result = httpclient.execute(delete);
    assertThat(result).isNotNull();

    // REST put?op=CAS for key 1

    // REST put?op=REPLACE for key 2

    httpclient = HttpClients.createDefault();
    put = new HttpPut(restEndpoint + "/People/2?op=replace");
    put.addHeader("Content-Type", "application/json");
    put.addHeader("Accept", "application/json");
    entity = new StringEntity(PERSON_AS_JSON_REPLACE);
    put.setEntity(entity);
    result = httpclient.execute(put);
    assertThat(result).isNotNull();
  }

  private void fetchRestServerEndpoints(String restEndpoint) throws IOException {
    HttpGet get = new HttpGet(restEndpoint + "/servers");
    get.addHeader("Content-Type", "application/json");
    get.addHeader("Accept", "application/json");
    CloseableHttpClient httpclient = HttpClients.createDefault();

    CloseableHttpResponse response = httpclient.execute(get);
    HttpEntity entity = response.getEntity();
    InputStream content = entity.getContent();
    BufferedReader reader = new BufferedReader(new InputStreamReader(content));
    String line;
    StringBuilder sb = new StringBuilder();
    while ((line = reader.readLine()) != null) {
      sb.append(line);
    }

    // validate the status code
    assertThat(response.getStatusLine().getStatusCode()).isEqualTo(200);

    JSONArray jsonArray = new JSONArray(sb.toString());

    // verify total number of REST service endpoints in DS
    assertThat(jsonArray.length()).isEqualTo(2);
  }

  private void doGetsUsingRestApis(String restEndpoint) throws IOException {
    // 1. Get on key="1" and validate result.

    HttpGet get = new HttpGet(restEndpoint + "/People/1");
    get.addHeader("Content-Type", "application/json");
    get.addHeader("Accept", "application/json");
    CloseableHttpClient httpclient = HttpClients.createDefault();
    CloseableHttpResponse response = httpclient.execute(get);

    HttpEntity entity = response.getEntity();
    InputStream content = entity.getContent();
    BufferedReader reader = new BufferedReader(new InputStreamReader(content));
    String line;
    StringBuilder sb = new StringBuilder();
    while ((line = reader.readLine()) != null) {
      sb.append(line);
    }

    JSONObject jObject = new JSONObject(sb.toString());

    assertThat(jObject.get("id")).isEqualTo(101);
    assertThat(jObject.get("firstName")).isEqualTo("Mithali");
    assertThat(jObject.get("middleName")).isEqualTo("Dorai");
    assertThat(jObject.get("lastName")).isEqualTo("Raj");
    assertThat(jObject.get("gender")).isEqualTo(Gender.FEMALE.name());

    // 2. Get on key="16" and validate result.

    get = new HttpGet(restEndpoint + "/People/16");
    get.addHeader("Content-Type", "application/json");
    get.addHeader("Accept", "application/json");
    httpclient = HttpClients.createDefault();
    response = httpclient.execute(get);

    entity = response.getEntity();
    content = entity.getContent();
    reader = new BufferedReader(new InputStreamReader(content));
    sb = new StringBuilder();
    while ((line = reader.readLine()) != null) {
      sb.append(line);
    }

    jObject = new JSONObject(sb.toString());

    assertThat(jObject.get("id")).isEqualTo(104);
    assertThat(jObject.get("firstName")).isEqualTo("Shila");
    assertThat(jObject.get("middleName")).isEqualTo("kumari");
    assertThat(jObject.get("lastName")).isEqualTo("Dixit");
    assertThat(jObject.get("gender")).isEqualTo(Gender.FEMALE.name());

    // 3. Get all (getAll) entries in Region

    get = new HttpGet(restEndpoint + "/People");
    get.addHeader("Content-Type", "application/json");
    get.addHeader("Accept", "application/json");
    httpclient = HttpClients.createDefault();
    CloseableHttpResponse result = httpclient.execute(get);
    assertThat(result.getStatusLine().getStatusCode()).isEqualTo(200);
    assertThat(result.getEntity()).isNotNull();

    entity = result.getEntity();
    content = entity.getContent();
    reader = new BufferedReader(new InputStreamReader(content));
    sb = new StringBuilder();
    while ((line = reader.readLine()) != null) {
      sb.append(line);
    }
    result.close();

    jObject = new JSONObject(sb.toString());
    JSONArray jArray = jObject.getJSONArray("People");
    assertThat(jArray.length()).isEqualTo(16);

    // 4. GetAll?limit=10 (10 entries) and verify results

    get = new HttpGet(restEndpoint + "/People?limit=10");
    get.addHeader("Content-Type", "application/json");
    get.addHeader("Accept", "application/json");
    httpclient = HttpClients.createDefault();
    response = httpclient.execute(get);
    assertThat(response.getStatusLine().getStatusCode()).isEqualTo(200);
    assertThat(response.getEntity()).isNotNull();

    entity = response.getEntity();
    content = entity.getContent();
    reader = new BufferedReader(new InputStreamReader(content));
    sb = new StringBuilder();
    while ((line = reader.readLine()) != null) {
      sb.append(line);
    }

    jObject = new JSONObject(sb.toString());
    jArray = jObject.getJSONArray("People");
    assertThat(jArray.length()).isEqualTo(10);

    // 5. Get keys - List all keys in region

    get = new HttpGet(restEndpoint + "/People/keys");
    get.addHeader("Content-Type", "application/json");
    get.addHeader("Accept", "application/json");
    httpclient = HttpClients.createDefault();
    response = httpclient.execute(get);
    assertThat(response.getStatusLine().getStatusCode()).isEqualTo(200);
    assertThat(response.getEntity()).isNotNull();

    entity = response.getEntity();
    content = entity.getContent();
    reader = new BufferedReader(new InputStreamReader(content));
    sb = new StringBuilder();
    while ((line = reader.readLine()) != null) {
      sb.append(line);
    }

    jObject = new JSONObject(sb.toString());
    jArray = jObject.getJSONArray("keys");
    assertThat(jArray.length()).isEqualTo(16);

    // 6. Get data for specific keys

    get = new HttpGet(restEndpoint + "/People/1,3,5,7,9,11");
    get.addHeader("Content-Type", "application/json");
    get.addHeader("Accept", "application/json");
    httpclient = HttpClients.createDefault();
    response = httpclient.execute(get);
    assertThat(response.getStatusLine().getStatusCode()).isEqualTo(200);
    assertThat(response.getEntity()).isNotNull();

    entity = response.getEntity();
    content = entity.getContent();
    reader = new BufferedReader(new InputStreamReader(content));
    sb = new StringBuilder();
    while ((line = reader.readLine()) != null) {
      sb.append(line);
    }

    jObject = new JSONObject(sb.toString());
    jArray = jObject.getJSONArray("People");
    assertThat(jArray.length()).isEqualTo(6);
  }

  private void createRegionInClientCache() {
    ClientCache cache = GemFireCacheImpl.getInstance();
    assertThat(cache).isNotNull();
    ClientRegionFactory<String, Object> crf =
        cache.createClientRegionFactory(ClientRegionShortcut.PROXY);
    Region<String, Object> region = crf.create(PEOPLE_REGION_NAME);
  }

  private void createRegion() {
    Cache cache = GemFireCacheImpl.getInstance();
    assertThat(cache).isNotNull();

    RegionFactory<String, Object> rf = cache.createRegionFactory(RegionShortcut.REPLICATE);
    Region<String, Object> region = rf.create(PEOPLE_REGION_NAME);
  }

  /**
   * InterOps Test between REST-client, Peer Cache Client and Client Cache
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

  private void createClientCache(final String host, final int port) {
    // Connect using the GemFire locator and create a Caching_Proxy cache
    ClientCache clientCache =
        new ClientCacheFactory().setPdxReadSerialized(true).addPoolLocator(host, port).create();

    clientCache.createClientRegionFactory(ClientRegionShortcut.PROXY).create(REGION_NAME);
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

    return server.getPort();
  }
}
