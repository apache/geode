/*
 * =========================================================================
 *  Copyright (c) 2012-2014 Pivotal Software, Inc. All Rights Reserved.
 *  This product is protected by U.S. and international copyright
 *  and intellectual property laws. Pivotal products are covered by
 *  more patents listed at http://www.pivotal.io/patents.
 * ========================================================================
 */
package com.vmware.gemfire.tools.pulse.tests.junit;

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
import org.junit.After;
import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

import com.vmware.gemfire.tools.pulse.internal.json.JSONArray;
import com.vmware.gemfire.tools.pulse.internal.json.JSONObject;

/**
 * JUnit Tests for ClusterSelectedRegionService in the back-end server for region detail page
 *
 * @author rbhandekar
 *
 */
public class ClusterSelectedRegionServiceTest extends BaseServiceTest {

  /**
   *
   * @author rbhandekar
   * @throws java.lang.Exception
   */
  @BeforeClass
  public static void setUpBeforeClass() throws Exception {
    doLogin();
    System.out.println("\n\nClusterSelectedRegionServiceTest :: Setup done");
  }

  /**
   *
   * @author rbhandekar
   * @throws java.lang.Exception
   */
  @AfterClass
  public static void tearDownAfterClass() throws Exception {
    doLogout();
    System.out.println("ClusterSelectedRegionServiceTest :: Teardown done");
  }

  /**
   *
   * @author rbhandekar
   * @throws java.lang.Exception
   */
  @Override
  @Before
  public void setUp() throws Exception {
  }

  /**
   *
   * @author rbhandekar
   * @throws java.lang.Exception
   */
  @Override
  @After
  public void tearDown() throws Exception {
  }

  /**
   * Tests that service returns json object
   *
   * @author rbhandekar
   */
  @Test
  public void testResponseNotNull() {
    System.out.println("ClusterSelectedRegionServiceTest ::  ------TESTCASE BEGIN : NULL RESPONSE CHECK FOR CLUSTER REGIONS------");
    if(httpclient != null){
        try{
            HttpUriRequest pulseupdate = RequestBuilder.post()
                .setUri(new URI(PULSE_UPDATE_URL))
                .addParameter(PULSE_UPDATE_PARAM, PULSE_UPDATE_1_VALUE)
                .build();
            CloseableHttpResponse response = httpclient.execute(pulseupdate);
            try {
              HttpEntity entity = response.getEntity();

              System.out.println("ClusterSelectedRegionServiceTest :: HTTP request status : " + response.getStatusLine());
              BufferedReader respReader = new BufferedReader(new InputStreamReader(entity.getContent()));
              StringWriter sw = new StringWriter();
              PrintWriter pw = new PrintWriter(sw);
              String sz = null;
              while((sz = respReader.readLine()) != null){
                pw.print(sz);
              }
              String jsonResp = sw.getBuffer().toString();
              System.out.println("ClusterSelectedRegionServiceTest :: JSON response returned : " + jsonResp);
              EntityUtils.consume(entity);

              JSONObject jsonObj = new JSONObject(jsonResp);
              Assert.assertNotNull("ClusterSelectedRegionServiceTest :: Server returned null response for ClusterSelectedRegion", jsonObj.getJSONObject("ClusterSelectedRegion"));
            } finally {
              response.close();
            }
        } catch(Exception failed) {
          logException(failed);
          Assert.fail("Exception ! ");
        }
    } else {
      Assert.fail("ClusterSelectedRegionServiceTest :: No Http connection was established.");
    }
    System.out.println("ClusterSelectedRegionServiceTest ::  ------TESTCASE END : NULL RESPONSE CHECK FOR CLUSTER REGIONS------\n");
  }

  /**
  *
  * Tests that response is for same logged in user
  *
  * @author rbhandekar
  */
  @Test
  public void testResponseUsername() {
    System.out.println("ClusterSelectedRegionServiceTest ::  ------TESTCASE BEGIN : NULL USERNAME IN RESPONSE CHECK FOR CLUSTER REGIONS------");
     if(httpclient != null){
         try{
             HttpUriRequest pulseupdate = RequestBuilder.post()
                 .setUri(new URI(PULSE_UPDATE_URL))
                 .addParameter(PULSE_UPDATE_PARAM, PULSE_UPDATE_1_VALUE)
                 .build();
             CloseableHttpResponse response = httpclient.execute(pulseupdate);
             try {
               HttpEntity entity = response.getEntity();

               System.out.println("ClusterSelectedRegionServiceTest :: HTTP request status : " + response.getStatusLine());

               BufferedReader respReader = new BufferedReader(new InputStreamReader(entity.getContent()));
               StringWriter sw = new StringWriter();
               PrintWriter pw = new PrintWriter(sw);
               String sz = null;
               while((sz = respReader.readLine()) != null){
                 pw.print(sz);
               }
               String jsonResp = sw.getBuffer().toString();
               System.out.println("ClusterSelectedRegionServiceTest :: JSON response returned : " + jsonResp);
               EntityUtils.consume(entity);

               JSONObject jsonObj = new JSONObject(jsonResp);
               JSONObject clusterSelectedRegionObj = jsonObj.getJSONObject("ClusterSelectedRegion");
               Assert.assertNotNull("ClusterSelectedRegionServiceTest :: Server returned null response for ClusterSelectedRegion", clusterSelectedRegionObj);
               String szUser = clusterSelectedRegionObj.getString("userName");
               Assert.assertEquals("ClusterSelectedRegionServiceTest :: Server returned wrong user name. Expected was admin. Server returned = " + szUser, szUser, "admin");
             } finally {
               response.close();
             }
         } catch(Exception failed) {
           logException(failed);
           Assert.fail("Exception ! ");
         }
     } else {
       Assert.fail("ClusterSelectedRegionServiceTest :: No Http connection was established.");
     }
     System.out.println("ClusterSelectedRegionServiceTest ::  ------TESTCASE END : NULL USERNAME IN RESPONSE CHECK FOR CLUSTER REGIONS------\n");
  }

  /**
  *
  * Tests that response is for same region
  *
  * Test method for {@link com.vmware.gemfire.tools.pulse.internal.service.ClusterSelectedRegionService#execute(javax.servlet.http.HttpServletRequest)}.
  *
  * @author rbhandekar
  */
  @Test
  public void testResponseRegionPathMatches() {
    System.out.println("ClusterSelectedRegionServiceTest ::  ------TESTCASE BEGIN : REGION PATH IN RESPONSE CHECK FOR CLUSTER REGIONS------");
     if(httpclient != null){
         try{
             HttpUriRequest pulseupdate = RequestBuilder.post()
                 .setUri(new URI(PULSE_UPDATE_URL))
                 .addParameter(PULSE_UPDATE_PARAM, PULSE_UPDATE_1_VALUE)
                 .build();
             CloseableHttpResponse response = httpclient.execute(pulseupdate);
             try {
               HttpEntity entity = response.getEntity();

               System.out.println("ClusterSelectedRegionServiceTest :: HTTP request status : " + response.getStatusLine());

               BufferedReader respReader = new BufferedReader(new InputStreamReader(entity.getContent()));
               StringWriter sw = new StringWriter();
               PrintWriter pw = new PrintWriter(sw);
               String sz = null;
               while((sz = respReader.readLine()) != null){
                 pw.print(sz);
               }
               String jsonResp = sw.getBuffer().toString();
               System.out.println("ClusterSelectedRegionServiceTest :: JSON response returned : " + jsonResp);
               EntityUtils.consume(entity);

               JSONObject jsonObj = new JSONObject(jsonResp);
               JSONObject clusterSelectedRegionObj = jsonObj.getJSONObject("ClusterSelectedRegion");
               Assert.assertNotNull("ClusterSelectedRegionServiceTest :: Server returned null response for ClusterSelectedRegion",clusterSelectedRegionObj);
               JSONObject jsonObjRegion = clusterSelectedRegionObj.getJSONObject("selectedRegion");
               Assert.assertNotNull("ClusterSelectedRegionServiceTest :: Server returned null response for selectedRegion",jsonObjRegion);
               Assert.assertTrue("ClusterSelectedRegionServiceTest :: Server did not return 'path' of region",jsonObjRegion.has("path"));
               String szPath = jsonObjRegion.getString("path");
               Assert.assertEquals("ClusterSelectedRegionServiceTest :: Server returned wrong region path. Expected region path = /GlobalVilage_2/GlobalVilage_9 , actual region path = " + szPath, szPath, "/GlobalVilage_2/GlobalVilage_9");
             } finally {
               response.close();
             }
         } catch(Exception failed) {
           logException(failed);
           Assert.fail("Exception ! ");
         }
     } else {
       Assert.fail("ClusterSelectedRegionServiceTest :: No Http connection was established.");
     }
     System.out.println("ClusterSelectedRegionServiceTest ::  ------TESTCASE END : REGION PATH IN RESPONSE CHECK FOR CLUSTER REGIONS------\n");
  }

  /**
  *
  * Tests that response is for same region
  *
  * Test method for {@link com.vmware.gemfire.tools.pulse.internal.service.ClusterSelectedRegionService#execute(javax.servlet.http.HttpServletRequest)}.
  *
  * @author rbhandekar
  */
  @Test
  public void testResponseNonExistentRegion() {
    System.out.println("ClusterSelectedRegionServiceTest ::  ------TESTCASE BEGIN : NON-EXISTENT REGION CHECK FOR CLUSTER REGIONS------");
     if(httpclient != null){
         try{
             HttpUriRequest pulseupdate = RequestBuilder.post()
                 .setUri(new URI(PULSE_UPDATE_URL))
                 .addParameter(PULSE_UPDATE_PARAM, PULSE_UPDATE_2_VALUE)
                 .build();
             CloseableHttpResponse response = httpclient.execute(pulseupdate);
             try {
               HttpEntity entity = response.getEntity();

               System.out.println("ClusterSelectedRegionServiceTest :: HTTP request status : " + response.getStatusLine());

               BufferedReader respReader = new BufferedReader(new InputStreamReader(entity.getContent()));
               StringWriter sw = new StringWriter();
               PrintWriter pw = new PrintWriter(sw);
               String sz = null;
               while((sz = respReader.readLine()) != null){
                 pw.print(sz);
               }
               String jsonResp = sw.getBuffer().toString();
               System.out.println("ClusterSelectedRegionServiceTest :: JSON response returned : " + jsonResp);
               EntityUtils.consume(entity);

               JSONObject jsonObj = new JSONObject(jsonResp);
               JSONObject clusterSelectedRegionObj = jsonObj.getJSONObject("ClusterSelectedRegion");
               Assert.assertNotNull("ClusterSelectedRegionServiceTest :: Server returned null response for ClusterSelectedRegion",clusterSelectedRegionObj);
               JSONObject jsonObjRegion = clusterSelectedRegionObj.getJSONObject("selectedRegion");
               Assert.assertNotNull("ClusterSelectedRegionServiceTest :: Server returned null response for selectedRegion",jsonObjRegion);
               Assert.assertTrue("ClusterSelectedRegionServiceTest :: Server did not return error on non-existent region",jsonObjRegion.has("errorOnRegion"));
             } finally {
               response.close();
             }
         } catch(Exception failed) {
           logException(failed);
           Assert.fail("Exception ! ");
         }
     } else {
       Assert.fail("ClusterSelectedRegionServiceTest :: No Http connection was established.");
     }
     System.out.println("ClusterSelectedRegionServiceTest ::  ------TESTCASE END : NON-EXISTENT REGION CHECK FOR CLUSTER REGIONS------\n");
  }

  /**
  *
  * Tests that service returns json object
  *
  * Test method for {@link com.vmware.gemfire.tools.pulse.internal.service.ClusterSelectedRegionService#execute(javax.servlet.http.HttpServletRequest)}.
  *
  * @author rbhandekar
  */
  @Test
  public void testResponseMemerberCount() {
   System.out.println("ClusterSelectedRegionServiceTest ::  ------TESTCASE BEGIN : MISMATCHED MEMBERCOUNT FOR REGION CHECK FOR CLUSTER REGIONS------");
   if(httpclient != null){
       try{
           HttpUriRequest pulseupdate = RequestBuilder.post()
               .setUri(new URI(PULSE_UPDATE_URL))
               .addParameter(PULSE_UPDATE_PARAM, PULSE_UPDATE_1_VALUE)
               .build();
           CloseableHttpResponse response = httpclient.execute(pulseupdate);
           try {
             HttpEntity entity = response.getEntity();

             System.out.println("ClusterSelectedRegionServiceTest :: HTTP request status : " + response.getStatusLine());

             BufferedReader respReader = new BufferedReader(new InputStreamReader(entity.getContent()));
             StringWriter sw = new StringWriter();
             PrintWriter pw = new PrintWriter(sw);
             String sz = null;
             while((sz = respReader.readLine()) != null){
               pw.print(sz);
             }
             String jsonResp = sw.getBuffer().toString();
             System.out.println("ClusterSelectedRegionServiceTest :: JSON response returned : " + jsonResp);
             EntityUtils.consume(entity);

             JSONObject jsonObj = new JSONObject(jsonResp);
             JSONObject clusterSelectedRegionObj = jsonObj.getJSONObject("ClusterSelectedRegion");
             Assert.assertNotNull("ClusterSelectedRegionServiceTest :: Server returned null ClusterSelectedRegion",clusterSelectedRegionObj);
             JSONObject jsonObjRegion = clusterSelectedRegionObj.getJSONObject("selectedRegion");
             Assert.assertNotNull("ClusterSelectedRegionServiceTest :: Server returned null for selectedRegion",jsonObjRegion);
             Assert.assertTrue("ClusterSelectedRegionServiceTest :: Server did not return 'memberCount' of region",jsonObjRegion.has("memberCount"));
             int memberCount = jsonObjRegion.getInt("memberCount");
             Assert.assertTrue("ClusterSelectedRegionServiceTest :: Server did not return 'members' of region",jsonObjRegion.has("members"));
             JSONArray arrMembers = jsonObjRegion.getJSONArray("members");
             Assert.assertNotNull("ClusterSelectedRegionServiceTest :: Server returned null response in selectedRegion",arrMembers);
             int members = arrMembers.length();
             Assert.assertEquals("ClusterSelectedRegionServiceTest :: Server returned mismatched member count and region members", members, memberCount);
           } finally {
             response.close();
           }
       } catch(Exception failed) {
         logException(failed);
         Assert.fail("Exception ! ");
       }
   } else {
     Assert.fail("ClusterSelectedRegionServiceTest :: No Http connection was established.");
   }
   System.out.println("ClusterSelectedRegionServiceTest ::  ------TESTCASE END : MISMATCHED MEMBERCOUNT FOR REGION CHECK FOR CLUSTER REGIONS------\n");
  }

}
