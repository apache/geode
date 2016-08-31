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
package com.gemstone.gemfire.rest.internal.web;

import static org.junit.Assert.*;

import java.net.MalformedURLException;

import org.apache.http.HttpResponse;
import org.json.JSONArray;
import org.json.JSONObject;
import org.junit.Ignore;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.gemstone.gemfire.test.junit.categories.DistributedTest;
import com.gemstone.gemfire.test.junit.categories.SecurityTest;

@SuppressWarnings("serial")
@Category({ DistributedTest.class, SecurityTest.class })
public class RestSecurityRegionEndpointsDUnitTest extends RestSecurityDUnitTest {

  Logger logger = LoggerFactory.getLogger(RestSecurityRegionEndpointsDUnitTest.class);

  public RestSecurityRegionEndpointsDUnitTest() throws MalformedURLException {
    super();
  }

  /**
   * This test should always return an OK, whether the user is known or unknown.  A phishing script should not be
   * able to determine whether a user/password combination is good
   */
  @Test
  public void testPing() {
    client1.invoke(() -> {
      HttpResponse response = doGet("/ping", "unknown-user", "badpassword");
      assertTrue(isOK(response));
      response = doGet("/ping", "super-user", "1234567");
      assertTrue(isOK(response));
      // TODO - credentials are currently required and shouldn't be
//      response = doGet("/ping", null, null);
//      assertTrue(isOK(response));
    });
  }

  /**
   * Send in a request with a valid user/password combination, but without privliges to access the method.
   */
  @Test
  public void listRegionsValidUser() {
    client1.invoke(() -> {
      HttpResponse response = doGet("", "authRegionReader", "1234567");
      assertEquals("A '403 - Forbidden' was expected", 403, getCode(response));
    });
  }

  /**
   * Send in a request with a valid user/password combination who also has sufficient rights to access the method.
   */
  @Test
  public void listRegionsHavePermission() {
    client1.invoke(() -> {
      HttpResponse response = doGet("", "dataReader", "1234567");
      assertEquals("A '200 - OK' was expected", 200, getCode(response));

      assertTrue(isOK(response));
      JSONObject jsonObject = new JSONObject(getResponseBody(response));
      JSONArray regions = jsonObject.getJSONArray("regions");
      assertNotNull(regions);
      assertTrue(regions.length() > 0);
      JSONObject region = regions.getJSONObject(0);
      assertEquals("AuthRegion", region.get("name"));
      assertEquals("REPLICATE", region.get("type"));

    });
  }

  @Test
  public void listRegionsUnknownUser() {
    client1.invoke(() -> {
      HttpResponse response = doGet("", "unknown-user", "badpassword");
      assertEquals("A '401 - Unauthorized' was expected", 401, getCode(response));
      assertTrue(isUnauthorized(response));
    });
  }


  //TODO: Jetty returns a 403 on Delete.  This has to be fixed.
  @Test
  @Ignore
  public void deleteRegionDataUnauthorized() {
    client1.invoke(() -> {
      HttpResponse response = doDelete("/" + REGION_NAME, "unknown-user", "1234567");
      assertEquals("A '401 - Unauthorized' was expected", 401, getCode(response));
      assertTrue(isUnauthorized(response));

    });
  }

  //TODO: Jetty returns a 403 on Delete.  This has to be fixed.
  @Test
  @Ignore
  public void deleteRegionDataAuthorized() {
    client1.invoke(() -> {
      HttpResponse response = doDelete("/" + REGION_NAME, "super-user", "1234567");
      assertTrue(isOK(response));
    });
  }
}
