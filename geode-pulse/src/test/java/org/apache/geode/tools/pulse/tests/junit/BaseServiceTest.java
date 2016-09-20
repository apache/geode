/*
 *
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
 *
 */
package org.apache.geode.tools.pulse.tests.junit;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.geode.test.junit.categories.UITest;
import com.google.gson.JsonObject;
import org.apache.geode.tools.pulse.internal.json.JSONObject;
import org.apache.http.HttpEntity;
import org.apache.http.client.methods.CloseableHttpResponse;
import org.apache.http.client.methods.HttpUriRequest;
import org.apache.http.client.methods.RequestBuilder;
import org.apache.http.cookie.Cookie;
import org.apache.http.impl.client.BasicCookieStore;
import org.apache.http.impl.client.CloseableHttpClient;
import org.apache.http.impl.client.HttpClients;
import org.apache.http.util.EntityUtils;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Ignore;
import org.junit.Test;
import org.junit.experimental.categories.Category;

import java.io.BufferedReader;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.PrintWriter;
import java.io.StringWriter;
import java.net.URI;
import java.util.List;
import java.util.Properties;


/**
 * Base class to be extended by other JUnit test classes. This class defines and automatically invokes test for testing server login-logout so no need to add
 * this test in each sub-class. It also provides doLogin(), doLogout() and print error functionality as protected
 * functions.
 *
 * <b>Sub-classes should ensure that they call doLogin() in @BeforeClass and doLogout() in @AfterClass otherwise tests will fail.</b>
 *
 *
 */
@Ignore
@Category(UITest.class)
public abstract class BaseServiceTest {

  protected static Properties propsForJUnit = new Properties();
  protected static String strHost = null;
  protected static String strPort = null;
  protected static String LOGIN_URL;
  protected static String LOGOUT_URL;
  protected static String IS_AUTHENTICATED_USER_URL;
  protected static String PULSE_UPDATE_URL;

  protected static final String PULSE_UPDATE_PARAM = "pulseData";
  protected static final String PULSE_UPDATE_1_VALUE = "{'ClusterSelectedRegion':{'regionFullPath':'/GlobalVilage_2/GlobalVilage_9'}}";
  protected static final String PULSE_UPDATE_2_VALUE = "{'ClusterSelectedRegion':{'regionFullPath':'/Rubbish'}}";

  protected static final String PULSE_UPDATE_3_VALUE = "{'ClusterSelectedRegionsMember':{'regionFullPath':'/GlobalVilage_2/GlobalVilage_9'}}";
  protected static final String PULSE_UPDATE_4_VALUE = "{'ClusterSelectedRegionsMember':{'regionFullPath':'/Rubbish'}}";

  protected static final String PULSE_UPDATE_5_VALUE = "{'MemberGatewayHub':{'memberName':'pnq-visitor1'}}";
  protected static final String PULSE_UPDATE_6_VALUE = "{'MemberGatewayHub':{'memberName':'pnq-visitor2'}}";
  protected static CloseableHttpClient httpclient = null;

  private final ObjectMapper mapper = new ObjectMapper();

  @BeforeClass
  public static void beforeClass() throws Exception {
    InputStream stream = BaseServiceTest.class.getClassLoader().getResourceAsStream("pulse.properties");

    try {
      propsForJUnit.load(stream);
    } catch (Exception exProps) {
      System.out.println("BaseServiceTest :: Error loading properties from pulse.properties in classpath");
    }
    strHost = propsForJUnit.getProperty("pulse.host");
    strPort = propsForJUnit.getProperty("pulse.port");
    System.out.println(
        "BaseServiceTest :: Loaded properties from classpath. Checking properties for hostname. Hostname found = " + strHost);
    LOGIN_URL = "http://" + strHost + ":" + strPort + "/pulse/j_spring_security_check";
    LOGOUT_URL = "http://" + strHost + ":" + strPort + "/pulse/clusterLogout";
    IS_AUTHENTICATED_USER_URL = "http://" + strHost + ":" + strPort + "/pulse/authenticateUser";
    PULSE_UPDATE_URL = "http://" + strHost + ":" + strPort + "/pulse/pulseUpdate";

  }
  /**
  *
  * @throws java.lang.Exception
  */
  @Before
  public void setUp() throws Exception {
    doLogout();
    System.out.println("BaseServiceTest :: Setup done");
  }

  /**
  *
  * @throws java.lang.Exception
  */
  @After
  public void tearDown() throws Exception {
    doLogin();
    System.out.println("BaseServiceTest :: Teardown done");
  }

  /**
   * Login to pulse server and setup httpClient for tests
   * To be called from setupBeforeClass in each test class
   */
  protected static void doLogin() throws Exception {
    System.out.println("BaseServiceTest ::  Executing doLogin with user : admin, password : admin.");

    CloseableHttpResponse loginResponse = null;
    try{
      BasicCookieStore cookieStore = new BasicCookieStore();
      httpclient = HttpClients.custom().setDefaultCookieStore(cookieStore).build();
      HttpUriRequest login = RequestBuilder.post().setUri(new URI(LOGIN_URL))
        .addParameter("j_username", "admin").addParameter("j_password", "admin")
        .build();
      loginResponse = httpclient.execute(login);
      try {
           HttpEntity entity = loginResponse.getEntity();
           EntityUtils.consume(entity);
           System.out.println("BaseServiceTest :: HTTP request status : " + loginResponse.getStatusLine());

           List<Cookie> cookies = cookieStore.getCookies();
           if (cookies.isEmpty()) {
           } else {
               for (int i = 0; i < cookies.size(); i++) {
               }
           }
      } finally {
         if(loginResponse != null)
           loginResponse.close();
      }
    } catch(Exception failed) {
      logException(failed);
      throw failed;
    }

    System.out.println("BaseServiceTest ::  Executed doLogin");
  }

  /**
   * Logout to pulse server and close httpClient
   * To be called from setupAfterClass in each test class
   */
  protected static void doLogout() throws Exception {
    System.out.println("BaseServiceTest ::  Executing doLogout with user : admin, password : admin.");
    if(httpclient != null){
      CloseableHttpResponse logoutResponse = null;
      try{
        HttpUriRequest logout = RequestBuilder.get().setUri(new URI(LOGOUT_URL))
            .build();
        logoutResponse = httpclient.execute(logout);
        try {
             HttpEntity entity = logoutResponse.getEntity();
             EntityUtils.consume(entity);
        } finally {
           if(logoutResponse != null)
             logoutResponse.close();
           httpclient.close();
           httpclient = null;
        }
      } catch(Exception failed) {
        logException(failed);
        throw failed;
      }
      System.out.println("BaseServiceTest ::  Executed doLogout");
    } else{
      System.out.println("BaseServiceTest ::  User NOT logged-in");
    }
  }

  /**
   * Print exception string to system.out
   *
   * @param failed
   */
  protected static void logException(Exception failed){
    StringWriter sw = new StringWriter();
    PrintWriter pw = new PrintWriter(sw);
    failed.printStackTrace(pw);
    System.out.println("BaseServiceTest :: Logging exception details : " + sw.getBuffer().toString());
  }

  /**
  *
  * Tests that service returns json object
  *
  * Test method for {@link org.apache.geode.tools.pulse.internal.service.ClusterSelectedRegionService#execute(javax.servlet.http.HttpServletRequest)}.
  */
  @Test
  public void testServerLoginLogout() {
      System.out.println("BaseServiceTest ::  ------TESTCASE BEGIN : SERVER LOGIN-LOGOUT------");
      try{
          doLogin();

          HttpUriRequest pulseupdate = RequestBuilder.get()
            .setUri(new URI(IS_AUTHENTICATED_USER_URL))
            .build();
          CloseableHttpResponse response = httpclient.execute(pulseupdate);
          try {
            HttpEntity entity = response.getEntity();

            System.out.println("BaseServiceTest :: HTTP request status : " + response.getStatusLine());

            BufferedReader respReader = new BufferedReader(new InputStreamReader(entity.getContent()));
            StringWriter sw = new StringWriter();
            PrintWriter pw = new PrintWriter(sw);
            String sz = null;
            while((sz = respReader.readLine()) != null){
              pw.print(sz);
            }
            String jsonResp = sw.getBuffer().toString();
            System.out.println("BaseServiceTest :: JSON response returned : " + jsonResp);
            EntityUtils.consume(entity);

            JsonNode jsonObj = mapper.readTree(jsonResp);
            boolean isUserLoggedIn = jsonObj.get("isUserLoggedIn").booleanValue();
            Assert.assertNotNull("BaseServiceTest :: Server returned null response in 'isUserLoggedIn'", isUserLoggedIn);
            Assert.assertTrue("BaseServiceTest :: User login failed for this username, password", (isUserLoggedIn == true));
          } finally {
            response.close();
          }

          doLogout();
      } catch(Exception failed) {
          logException(failed);
          Assert.fail("Exception ! ");
      }
      System.out.println("BaseServiceTest ::  ------TESTCASE END : SERVER LOGIN-LOGOUT------");
  }
}
