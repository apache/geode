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

import java.io.BufferedReader;
import java.io.InputStreamReader;
import java.io.PrintWriter;
import java.io.StringWriter;
import java.net.URI;

import org.apache.http.HttpEntity;
import org.apache.http.client.methods.CloseableHttpResponse;
import org.apache.http.client.methods.HttpUriRequest;
import org.apache.http.client.methods.RequestBuilder;
import org.apache.http.util.EntityUtils;
import org.junit.*;

import org.apache.geode.tools.pulse.internal.json.JSONArray;
import org.apache.geode.tools.pulse.internal.json.JSONObject;

/**
 * JUnit Tests for MemberGatewayHubService in the back-end server for region detail page
 *
 *
 */
@Ignore
public class MemberGatewayHubServiceTest extends BaseServiceTest {

  /**
   *
   * @throws java.lang.Exception
   */
  @BeforeClass
  public static void setUpBeforeClass() throws Exception {
    doLogin();
    System.out.println("\n\nMemberGatewayHubServiceTest :: Setup done");
  }

  /**
   *
   * @throws java.lang.Exception
   */
  @AfterClass
  public static void tearDownAfterClass() throws Exception {
    doLogout();
    System.out.println("MemberGatewayHubServiceTest :: Teardown done");
  }

  /**
   *
   * @throws java.lang.Exception
   */
  @Override
  @Before
  public void setUp() throws Exception {
  }

  /**
   *
   * @throws java.lang.Exception
   */
  @Override
  @After
  public void tearDown() throws Exception {
  }

  /**
   * Tests that service returns json object
   *
   */
  @Test
  public void testResponseNotNull() {
    System.out.println("MemberGatewayHubServiceTest ::  ------TESTCASE BEGIN : NULL RESPONSE CHECK FOR MEMBER GATEWAY HUB SERVICE --------");
    if(httpclient != null){
        try{
            HttpUriRequest pulseupdate = RequestBuilder.post()
                .setUri(new URI(PULSE_UPDATE_URL))
                .addParameter(PULSE_UPDATE_PARAM, PULSE_UPDATE_5_VALUE)
                .build();
            CloseableHttpResponse response = httpclient.execute(pulseupdate);
            try {
              HttpEntity entity = response.getEntity();

              System.out.println("MemberGatewayHubServiceTest :: HTTP request status : " + response.getStatusLine());
              BufferedReader respReader = new BufferedReader(new InputStreamReader(entity.getContent()));
              StringWriter sw = new StringWriter();
              PrintWriter pw = new PrintWriter(sw);
              String sz = null;
              while((sz = respReader.readLine()) != null){
                pw.print(sz);
              }
              String jsonResp = sw.getBuffer().toString();
              System.out.println("MemberGatewayHubServiceTest :: JSON response returned : " + jsonResp);
              EntityUtils.consume(entity);

              JSONObject jsonObj = new JSONObject(jsonResp);
              Assert.assertNotNull("MemberGatewayHubServiceTest :: Server returned null response for MemberGatewayHub", jsonObj.getJSONObject("MemberGatewayHub"));
            } finally {
              response.close();
            }
        } catch(Exception failed) {
          logException(failed);
          Assert.fail("Exception ! ");
        }
    } else {
      Assert.fail("MemberGatewayHubServiceTest :: No Http connection was established.");
    }
    System.out.println("MemberGatewayHubServiceTest ::  ------TESTCASE END : NULL RESPONSE CHECK FOR MEMBER GATEWAY HUB SERVICE ------\n");
  }

 /**
  *
  * Tests that response is for same region
  *
  * Test method for {@link org.apache.geode.tools.pulse.internal.service.MemberGatewayHubService#execute(javax.servlet.http.HttpServletRequest)}.
  *
  */
  @Test
  public void testResponseIsGatewaySender() {
    System.out.println("MemberGatewayHubServiceTest ::  ------TESTCASE BEGIN : IS GATEWAY SENDER IN RESPONSE CHECK FOR MEMBER GATEWAY HUB SERVICE------");
     if(httpclient != null){
         try{
             HttpUriRequest pulseupdate = RequestBuilder.post()
                 .setUri(new URI(PULSE_UPDATE_URL))
                 .addParameter(PULSE_UPDATE_PARAM, PULSE_UPDATE_5_VALUE)
                 .build();
             CloseableHttpResponse response = httpclient.execute(pulseupdate);
             try {
               HttpEntity entity = response.getEntity();

               System.out.println("MemberGatewayHubServiceTest :: HTTP request status : " + response.getStatusLine());

               BufferedReader respReader = new BufferedReader(new InputStreamReader(entity.getContent()));
               StringWriter sw = new StringWriter();
               PrintWriter pw = new PrintWriter(sw);
               String sz = null;
               while((sz = respReader.readLine()) != null){
                 pw.print(sz);
               }
               String jsonResp = sw.getBuffer().toString();
               System.out.println("MemberGatewayHubServiceTest :: JSON response returned : " + jsonResp);
               EntityUtils.consume(entity);

               JSONObject jsonObj = new JSONObject(jsonResp);
               JSONObject memberGatewayHubObj = jsonObj.getJSONObject("MemberGatewayHub");
               Assert.assertNotNull("MemberGatewayHubServiceTest :: Server returned null response for MemberGatewayHub", memberGatewayHubObj);
               Assert.assertTrue("MemberGatewayHubServiceTest :: Server did not return 'isGatewaySender' for member", memberGatewayHubObj.has("isGatewaySender"));
               Boolean boolIsGatewaySender = memberGatewayHubObj.getBoolean("isGatewaySender");
               Assert.assertEquals("MemberGatewayHubServiceTest :: Server returned wrong value for 'isGatewaySender'. Expected 'isGatewaySender' = true, actual 'isGatewaySender' = " + boolIsGatewaySender, boolIsGatewaySender, true);
             } finally {
               response.close();
             }
         } catch(Exception failed) {
           logException(failed);
           Assert.fail("Exception ! ");
         }
     } else {
       Assert.fail("MemberGatewayHubServiceTest :: No Http connection was established.");
     }
     System.out.println("MemberGatewayHubServiceTest ::  ------TESTCASE END : IS GATEWAY SENDER IN RESPONSE CHECK FOR MEMBER GATEWAY HUB SERVICE ------\n");
  }


  /**
  *
  * Tests that response is for same region
  *
  * Test method for {@link org.apache.geode.tools.pulse.internal.service.MemberGatewayHubService#execute(javax.servlet.http.HttpServletRequest)}.
  *
  */
  @Test
  public void testResponseGatewaySenderCount() {
    System.out.println("MemberGatewayHubServiceTest ::  ------TESTCASE BEGIN : GATEWAY SENDER COUNT IN RESPONSE CHECK FOR MEMBER GATEWAY HUB SERVICE------");
     if(httpclient != null){
         try{
             HttpUriRequest pulseupdate = RequestBuilder.post()
                 .setUri(new URI(PULSE_UPDATE_URL))
                 .addParameter(PULSE_UPDATE_PARAM, PULSE_UPDATE_5_VALUE)
                 .build();
             CloseableHttpResponse response = httpclient.execute(pulseupdate);
             try {
               HttpEntity entity = response.getEntity();

               System.out.println("MemberGatewayHubServiceTest :: HTTP request status : " + response.getStatusLine());

               BufferedReader respReader = new BufferedReader(new InputStreamReader(entity.getContent()));
               StringWriter sw = new StringWriter();
               PrintWriter pw = new PrintWriter(sw);
               String sz = null;
               while((sz = respReader.readLine()) != null){
                 pw.print(sz);
               }
               String jsonResp = sw.getBuffer().toString();
               System.out.println("MemberGatewayHubServiceTest :: JSON response returned : " + jsonResp);
               EntityUtils.consume(entity);

               JSONObject jsonObj = new JSONObject(jsonResp);
               JSONObject memberGatewayHubObj = jsonObj.getJSONObject("MemberGatewayHub");
               Assert.assertNotNull("MemberGatewayHubServiceTest :: Server returned null response for MemberGatewayHub", memberGatewayHubObj);
               Assert.assertTrue("MemberGatewayHubServiceTest :: Server did not return 'isGatewaySender' for member", memberGatewayHubObj.has("isGatewaySender"));
               Boolean boolIsGatewaySender = memberGatewayHubObj.getBoolean("isGatewaySender");

               Assert.assertTrue("MemberGatewayHubServiceTest :: Server did not return 'gatewaySenders' for member", memberGatewayHubObj.has("gatewaySenders"));
               JSONArray arrGatewaySender = memberGatewayHubObj.getJSONArray("gatewaySenders");
               Assert.assertNotNull("MemberGatewayHubServiceTest :: Server returned null response for 'gatewaySenders'", arrGatewaySender);
               Assert.assertTrue( "MemberGatewayHubServiceTest :: Server returned mis-matched values for 'isGatewaySender' and gateway senders array count", ((boolIsGatewaySender && (arrGatewaySender.length() > 0)) || ((! boolIsGatewaySender) && (arrGatewaySender.length() == 0))) );
             } finally {
               response.close();
             }
         } catch(Exception failed) {
           logException(failed);
           Assert.fail("Exception ! ");
         }
     } else {
       Assert.fail("MemberGatewayHubServiceTest :: No Http connection was established.");
     }
     System.out.println("MemberGatewayHubServiceTest ::  ------TESTCASE END : GATEWAY SENDER COUNT IN RESPONSE CHECK FOR MEMBER GATEWAY HUB SERVICE ------\n");
  }

  /**
  *
  * Tests that response is for same region
  *
  * Test method for {@link org.apache.geode.tools.pulse.internal.service.MemberGatewayHubService#execute(javax.servlet.http.HttpServletRequest)}.
  *
  */
  @Test
  public void testResponseGatewaySenderProperties() {
    System.out.println("MemberGatewayHubServiceTest ::  ------TESTCASE BEGIN : GATEWAY SENDER PROPERTIES IN RESPONSE CHECK FOR MEMBER GATEWAY HUB SERVICE------");
     if(httpclient != null){
         try{
             HttpUriRequest pulseupdate = RequestBuilder.post()
                 .setUri(new URI(PULSE_UPDATE_URL))
                 .addParameter(PULSE_UPDATE_PARAM, PULSE_UPDATE_5_VALUE)
                 .build();
             CloseableHttpResponse response = httpclient.execute(pulseupdate);
             try {
               HttpEntity entity = response.getEntity();

               System.out.println("MemberGatewayHubServiceTest :: HTTP request status : " + response.getStatusLine());

               BufferedReader respReader = new BufferedReader(new InputStreamReader(entity.getContent()));
               StringWriter sw = new StringWriter();
               PrintWriter pw = new PrintWriter(sw);
               String sz = null;
               while((sz = respReader.readLine()) != null){
                 pw.print(sz);
               }
               String jsonResp = sw.getBuffer().toString();
               System.out.println("MemberGatewayHubServiceTest :: JSON response returned : " + jsonResp);
               EntityUtils.consume(entity);

               JSONObject jsonObj = new JSONObject(jsonResp);
               JSONObject memberGatewayHubObj = jsonObj.getJSONObject("MemberGatewayHub");
               Assert.assertNotNull("MemberGatewayHubServiceTest :: Server returned null response for MemberGatewayHub", memberGatewayHubObj);
               Assert.assertTrue("MemberGatewayHubServiceTest :: Server did not return 'isGatewaySender' for member", memberGatewayHubObj.has("isGatewaySender"));
               Boolean boolIsGatewaySender = memberGatewayHubObj.getBoolean("isGatewaySender");

               Assert.assertTrue("MemberGatewayHubServiceTest :: Server did not return 'gatewaySenders' for member", memberGatewayHubObj.has("gatewaySenders"));
               JSONArray arrGatewaySender = memberGatewayHubObj.getJSONArray("gatewaySenders");
               Assert.assertNotNull("MemberGatewayHubServiceTest :: Server returned null response for 'gatewaySenders'", arrGatewaySender);
               Assert.assertTrue( "MemberGatewayHubServiceTest :: Server returned mis-matched values for 'isGatewaySender' and gateway senders array count", ((boolIsGatewaySender && (arrGatewaySender.length() > 0)) || ((! boolIsGatewaySender) && (arrGatewaySender.length() == 0))) );

               Assert.assertTrue("MemberGatewayHubServiceTest :: Server did not return 'remoteDSId' for member", ((JSONObject)arrGatewaySender.get(0)).has("remoteDSId"));
               Assert.assertTrue("MemberGatewayHubServiceTest :: Server did not return 'eventsExceedingAlertThreshold' for member", ((JSONObject)arrGatewaySender.get(0)).has("eventsExceedingAlertThreshold"));
             } finally {
               response.close();
             }
         } catch(Exception failed) {
           logException(failed);
           Assert.fail("Exception ! ");
         }
     } else {
       Assert.fail("MemberGatewayHubServiceTest :: No Http connection was established.");
     }
     System.out.println("MemberGatewayHubServiceTest ::  ------TESTCASE END : GATEWAY SENDER PROPERTIES IN RESPONSE CHECK FOR MEMBER GATEWAY HUB SERVICE ------\n");
  }

  /**
  *
  * Tests that response is for same region
  *
  * Test method for {@link org.apache.geode.tools.pulse.internal.service.MemberGatewayHubService#execute(javax.servlet.http.HttpServletRequest)}.
  *
  */
  @Test
  public void testResponseAsyncEventQueueProperties() {
    System.out.println("MemberGatewayHubServiceTest ::  ------TESTCASE BEGIN : ASYNC EVENT QUEUE PROPERTIES IN RESPONSE CHECK FOR MEMBER GATEWAY HUB SERVICE------");
     if(httpclient != null){
         try{
             HttpUriRequest pulseupdate = RequestBuilder.post()
                 .setUri(new URI(PULSE_UPDATE_URL))
                 .addParameter(PULSE_UPDATE_PARAM, PULSE_UPDATE_5_VALUE)
                 .build();
             CloseableHttpResponse response = httpclient.execute(pulseupdate);
             try {
               HttpEntity entity = response.getEntity();

               System.out.println("MemberGatewayHubServiceTest :: HTTP request status : " + response.getStatusLine());

               BufferedReader respReader = new BufferedReader(new InputStreamReader(entity.getContent()));
               StringWriter sw = new StringWriter();
               PrintWriter pw = new PrintWriter(sw);
               String sz = null;
               while((sz = respReader.readLine()) != null){
                 pw.print(sz);
               }
               String jsonResp = sw.getBuffer().toString();
               System.out.println("MemberGatewayHubServiceTest :: JSON response returned : " + jsonResp);
               EntityUtils.consume(entity);

               JSONObject jsonObj = new JSONObject(jsonResp);
               JSONObject memberGatewayHubObj = jsonObj.getJSONObject("MemberGatewayHub");
               Assert.assertNotNull("MemberGatewayHubServiceTest :: Server returned null response for MemberGatewayHub", memberGatewayHubObj);

               Assert.assertTrue("MemberGatewayHubServiceTest :: Server did not return 'asyncEventQueues' for member", memberGatewayHubObj.has("asyncEventQueues"));
               JSONArray arrAsyncEventQueues = memberGatewayHubObj.getJSONArray("asyncEventQueues");
               Assert.assertNotNull("MemberGatewayHubServiceTest :: Server returned null response for 'asyncEventQueues'", arrAsyncEventQueues);

               Assert.assertTrue("MemberGatewayHubServiceTest :: Server did not return 'id' for member", ((JSONObject)arrAsyncEventQueues.get(0)).has("id"));
               Assert.assertTrue("MemberGatewayHubServiceTest :: Server did not return 'primary' for member", ((JSONObject)arrAsyncEventQueues.get(0)).has("primary"));
               Assert.assertTrue("MemberGatewayHubServiceTest :: Server did not return 'senderType' for member", ((JSONObject)arrAsyncEventQueues.get(0)).has("senderType"));
               Assert.assertTrue("MemberGatewayHubServiceTest :: Server did not return 'batchSize' for member", ((JSONObject)arrAsyncEventQueues.get(0)).has("batchSize"));
               Assert.assertTrue("MemberGatewayHubServiceTest :: Server did not return 'batchTimeInterval' for member", ((JSONObject)arrAsyncEventQueues.get(0)).has("batchTimeInterval"));
               Assert.assertTrue("MemberGatewayHubServiceTest :: Server did not return 'batchConflationEnabled' for member", ((JSONObject)arrAsyncEventQueues.get(0)).has("batchConflationEnabled"));
               Assert.assertTrue("MemberGatewayHubServiceTest :: Server did not return 'asyncEventListener' for member", ((JSONObject)arrAsyncEventQueues.get(0)).has("asyncEventListener"));
               Assert.assertTrue("MemberGatewayHubServiceTest :: Server did not return 'queueSize' for member", ((JSONObject)arrAsyncEventQueues.get(0)).has("queueSize"));
             } finally {
               response.close();
             }
         } catch(Exception failed) {
           logException(failed);
           Assert.fail("Exception ! ");
         }
     } else {
       Assert.fail("MemberGatewayHubServiceTest :: No Http connection was established.");
     }
     System.out.println("MemberGatewayHubServiceTest ::  ------TESTCASE END : ASYNC EVENT QUEUE PROPERTIES IN RESPONSE CHECK FOR MEMBER GATEWAY HUB SERVICE ------\n");
  }

  /**
  *
  * Tests that response is for same region
  *
  * Test method for {@link org.apache.geode.tools.pulse.internal.service.MemberGatewayHubService#execute(javax.servlet.http.HttpServletRequest)}.
  *
  */
  @Test
  public void testResponseNoAsyncEventQueues() {
    System.out.println("MemberGatewayHubServiceTest ::  ------TESTCASE BEGIN : NO ASYNC EVENT QUEUES IN RESPONSE CHECK FOR MEMBER GATEWAY HUB SERVICE------");
     if(httpclient != null){
         try{
             HttpUriRequest pulseupdate = RequestBuilder.post()
                 .setUri(new URI(PULSE_UPDATE_URL))
                 .addParameter(PULSE_UPDATE_PARAM, PULSE_UPDATE_6_VALUE)
                 .build();
             CloseableHttpResponse response = httpclient.execute(pulseupdate);
             try {
               HttpEntity entity = response.getEntity();

               System.out.println("MemberGatewayHubServiceTest :: HTTP request status : " + response.getStatusLine());

               BufferedReader respReader = new BufferedReader(new InputStreamReader(entity.getContent()));
               StringWriter sw = new StringWriter();
               PrintWriter pw = new PrintWriter(sw);
               String sz = null;
               while((sz = respReader.readLine()) != null){
                 pw.print(sz);
               }
               String jsonResp = sw.getBuffer().toString();
               System.out.println("MemberGatewayHubServiceTest :: JSON response returned : " + jsonResp);
               EntityUtils.consume(entity);

               JSONObject jsonObj = new JSONObject(jsonResp);
               JSONObject memberGatewayHubObj = jsonObj.getJSONObject("MemberGatewayHub");
               Assert.assertNotNull("MemberGatewayHubServiceTest :: Server returned null response for MemberGatewayHub", memberGatewayHubObj);

               Assert.assertTrue("MemberGatewayHubServiceTest :: Server did not return 'asyncEventQueues' for member", memberGatewayHubObj.has("asyncEventQueues"));
               JSONArray arrAsyncEventQueues = memberGatewayHubObj.getJSONArray("asyncEventQueues");
               Assert.assertNotNull("MemberGatewayHubServiceTest :: Server returned null response for 'asyncEventQueues'", arrAsyncEventQueues);
               Assert.assertTrue("MemberGatewayHubServiceTest :: Server returned non-empty array for member 'pnq-visitor2' which has no event queues", (arrAsyncEventQueues.length() == 0));
             } finally {
               response.close();
             }
         } catch(Exception failed) {
           logException(failed);
           Assert.fail("Exception ! ");
         }
     } else {
       Assert.fail("MemberGatewayHubServiceTest :: No Http connection was established.");
     }
     System.out.println("MemberGatewayHubServiceTest ::  ------TESTCASE END : NO ASYNC EVENT QUEUES PROPERTIES IN RESPONSE CHECK FOR MEMBER GATEWAY HUB SERVICE ------\n");
  }


}
