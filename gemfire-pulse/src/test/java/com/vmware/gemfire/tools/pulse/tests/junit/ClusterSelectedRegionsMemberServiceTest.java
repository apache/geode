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
import java.util.Iterator;

import org.apache.http.HttpEntity;
import org.apache.http.client.methods.CloseableHttpResponse;
import org.apache.http.client.methods.HttpUriRequest;
import org.apache.http.client.methods.RequestBuilder;
import org.apache.http.util.EntityUtils;
import org.junit.*;

import com.vmware.gemfire.tools.pulse.internal.json.JSONObject;

/**
 * JUnit Tests for ClusterSelectedRegionsMemberService in the back-end server for region detail page
 *
 * @author rbhandekar
 *
 */
@Ignore
public class ClusterSelectedRegionsMemberServiceTest  extends BaseServiceTest {

  /**
   * @throws java.lang.Exception
   *
   * @author rbhandekar
   */
  @BeforeClass
  public static void setUpBeforeClass() throws Exception {
    doLogin();
    System.out.println("\n\nClusterSelectedRegionsMemberServiceTest :: Setup done");
  }

  /**
   * @throws java.lang.Exception
   *
   * @author rbhandekar
   */
  @AfterClass
  public static void tearDownAfterClass() throws Exception {
    doLogout();
    System.out.println("ClusterSelectedRegionsMemberServiceTest :: Teardown done");
  }

  /**
   * @throws java.lang.Exception
   */
  @Override
  @Before
  public void setUp() throws Exception {
    System.out.println("running setup -- ClusterSelectedRegionsMemberServiceTest");
  }

  /**
   * @throws java.lang.Exception
   */
  @Override
  @After
  public void tearDown() throws Exception {
    System.out.println("running teardown -- ClusterSelectedRegionsMemberServiceTest");
  }

  /**
   * Tests that service returns json object
   *
   * @author rbhandekar
   */
  @Test
  public void testResponseNotNull() {
    System.out.println("ClusterSelectedRegionsMemberServiceTest ::  ------TESTCASE BEGIN : NULL RESPONSE CHECK FOR CLUSTER REGION MEMBERS------");
    if(httpclient != null){
        try{
            HttpUriRequest pulseupdate = RequestBuilder.post()
                .setUri(new URI(PULSE_UPDATE_URL))
                .addParameter(PULSE_UPDATE_PARAM, PULSE_UPDATE_3_VALUE)
                .build();
            CloseableHttpResponse response = httpclient.execute(pulseupdate);
            try {
              HttpEntity entity = response.getEntity();

              System.out.println("ClusterSelectedRegionsMemberServiceTest :: HTTP request status : " + response.getStatusLine());

              BufferedReader respReader = new BufferedReader(new InputStreamReader(entity.getContent()));
              StringWriter sw = new StringWriter();
              PrintWriter pw = new PrintWriter(sw);
              String sz = null;
              while((sz = respReader.readLine()) != null){
                pw.print(sz);
              }
              String jsonResp = sw.getBuffer().toString();
              System.out.println("ClusterSelectedRegionsMemberServiceTest :: JSON response returned : " + jsonResp);
              EntityUtils.consume(entity);

              JSONObject jsonObj = new JSONObject(jsonResp);
              Assert.assertNotNull("ClusterSelectedRegionsMemberServiceTest :: Server returned null response for ClusterSelectedRegionsMember", jsonObj.getJSONObject("ClusterSelectedRegionsMember"));
            } finally {
              response.close();
            }
        } catch(Exception failed) {
          logException(failed);
          Assert.fail("Exception ! ");
        }
    } else {
      Assert.fail("ClusterSelectedRegionsMemberServiceTest :: No Http connection was established.");
    }
    System.out.println("ClusterSelectedRegionsMemberServiceTest ::  ------TESTCASE END : NULL RESPONSE CHECK FOR CLUSTER REGION MEMBERS------");
  }

  /**
  *
  * Tests that response is for same logged in user
  *
  * @author rbhandekar
  */
  @Test
  public void testResponseUsername() {
    System.out.println("ClusterSelectedRegionsMemberServiceTest ::  ------TESTCASE BEGIN : NULL USERNAME IN RESPONSE CHECK FOR CLUSTER REGION MEMBERS------");
     if(httpclient != null){
         try{
             HttpUriRequest pulseupdate = RequestBuilder.post()
                 .setUri(new URI(PULSE_UPDATE_URL))
                 .addParameter(PULSE_UPDATE_PARAM, PULSE_UPDATE_3_VALUE)
                 .build();
             CloseableHttpResponse response = httpclient.execute(pulseupdate);
             try {
               HttpEntity entity = response.getEntity();

               System.out.println("ClusterSelectedRegionsMemberServiceTest :: HTTP request status : " + response.getStatusLine());

               BufferedReader respReader = new BufferedReader(new InputStreamReader(entity.getContent()));
               StringWriter sw = new StringWriter();
               PrintWriter pw = new PrintWriter(sw);
               String sz = null;
               while((sz = respReader.readLine()) != null){
                 pw.print(sz);
               }
               String jsonResp = sw.getBuffer().toString();
               System.out.println("ClusterSelectedRegionsMemberServiceTest :: JSON response returned : " + jsonResp);
               EntityUtils.consume(entity);

               JSONObject jsonObj = new JSONObject(jsonResp);
               JSONObject clusterSelectedRegionObj = jsonObj.getJSONObject("ClusterSelectedRegionsMember");
               Assert.assertNotNull("ClusterSelectedRegionsMemberServiceTest :: Server returned null response for ClusterSelectedRegionsMember", clusterSelectedRegionObj);
               Assert.assertTrue("ClusterSelectedRegionsMemberServiceTest :: Server did not return 'userName' in request",clusterSelectedRegionObj.has("userName"));
               String szUser = clusterSelectedRegionObj.getString("userName");
               Assert.assertEquals("ClusterSelectedRegionsMemberServiceTest :: Server returned wrong user name. Expected was admin. Server returned = " + szUser, szUser, "admin");
             } finally {
               response.close();
             }
         } catch(Exception failed) {
           logException(failed);
           Assert.fail("Exception ! ");
         }
     } else {
       Assert.fail("ClusterSelectedRegionsMemberServiceTest :: No Http connection was established.");
     }
     System.out.println("ClusterSelectedRegionsMemberServiceTest ::  ------TESTCASE END : NULL USERNAME IN RESPONSE CHECK FOR CLUSTER REGION MEMBERS------");
  }

  /**
  *
  * Tests that response is for same region
  *
  *
  * @author rbhandekar
  */
  @Test
  public void testResponseRegionOnMemberInfoMatches() {
    System.out.println("ClusterSelectedRegionsMemberServiceTest ::  ------TESTCASE BEGIN : MEMBER INFO RESPONSE CHECK FOR CLUSTER REGION MEMBERS------");
     if(httpclient != null){
         try{
             HttpUriRequest pulseupdate = RequestBuilder.post()
                 .setUri(new URI(PULSE_UPDATE_URL))
                 .addParameter(PULSE_UPDATE_PARAM, PULSE_UPDATE_3_VALUE)
                 .build();
             CloseableHttpResponse response = httpclient.execute(pulseupdate);
             try {
               HttpEntity entity = response.getEntity();

               System.out.println("ClusterSelectedRegionsMemberServiceTest :: HTTP request status : " + response.getStatusLine());

               BufferedReader respReader = new BufferedReader(new InputStreamReader(entity.getContent()));
               StringWriter sw = new StringWriter();
               PrintWriter pw = new PrintWriter(sw);
               String sz = null;
               while((sz = respReader.readLine()) != null){
                 pw.print(sz);
               }
               String jsonResp = sw.getBuffer().toString();
               System.out.println("ClusterSelectedRegionsMemberServiceTest :: JSON response returned : " + jsonResp);
               EntityUtils.consume(entity);

               JSONObject jsonObj = new JSONObject(jsonResp);
               JSONObject clusterSelectedRegionObj = jsonObj.getJSONObject("ClusterSelectedRegionsMember");
               Assert.assertNotNull("ClusterSelectedRegionsMemberServiceTest :: Server returned null response for ClusterSelectedRegionsMember",clusterSelectedRegionObj);
               JSONObject jsonObjRegion = clusterSelectedRegionObj.getJSONObject("selectedRegionsMembers");
               Assert.assertNotNull("ClusterSelectedRegionsMemberServiceTest :: Server returned null response for selectedRegionsMembers",jsonObjRegion);
               Iterator<String> itrMemberNames = jsonObjRegion.keys();
               Assert.assertNotNull("ClusterSelectedRegionsMemberServiceTest :: Server returned null region on member info", itrMemberNames);
               while(itrMemberNames.hasNext()){
                 String szMemberName = itrMemberNames.next();
                 Assert.assertNotNull("ClusterSelectedRegionsMemberServiceTest :: Server returned null member name", szMemberName);
                 Assert.assertTrue("Server did not return member details",jsonObjRegion.has(szMemberName));
                 JSONObject jsonMemberObj = jsonObjRegion.getJSONObject(szMemberName);

                 Assert.assertTrue("ClusterSelectedRegionsMemberServiceTest :: Server did not return 'regionFullPath' of region on member",jsonMemberObj.has("regionFullPath"));
                 String szPath = jsonMemberObj.getString("regionFullPath");
                 Assert.assertEquals("ClusterSelectedRegionsMemberServiceTest :: Server returned wrong region path for region on member", szPath, "/GlobalVilage_2/GlobalVilage_9");
               }
             } finally {
               response.close();
             }
         } catch(Exception failed) {
           logException(failed);
           Assert.fail("Exception ! ");
         }
     } else {
       Assert.fail("ClusterSelectedRegionsMemberServiceTest :: No Http connection was established.");
     }
     System.out.println("ClusterSelectedRegionsMemberServiceTest ::  ------TESTCASE END : MEMBER INFO RESPONSE CHECK FOR CLUSTER REGION MEMBERS------");
  }

  /**
  *
  * Tests that response is for same region
  *
  *
  * @author rbhandekar
  */
  @Test
  public void testResponseNonExistentRegion() {
    System.out.println("ClusterSelectedRegionsMemberServiceTest ::  ------TESTCASE BEGIN : NON-EXISTENT REGION CHECK FOR CLUSTER REGION MEMBERS------");
     if(httpclient != null){
         try{
           System.out.println("Test for non-existent region : /Rubbish");
             HttpUriRequest pulseupdate = RequestBuilder.post()
                 .setUri(new URI(PULSE_UPDATE_URL))
                 .addParameter(PULSE_UPDATE_PARAM, PULSE_UPDATE_4_VALUE)
                 .build();
             CloseableHttpResponse response = httpclient.execute(pulseupdate);
             try {
               HttpEntity entity = response.getEntity();

               System.out.println("ClusterSelectedRegionsMemberServiceTest :: HTTP request status : " + response.getStatusLine());

               BufferedReader respReader = new BufferedReader(new InputStreamReader(entity.getContent()));
               StringWriter sw = new StringWriter();
               PrintWriter pw = new PrintWriter(sw);
               String sz = null;
               while((sz = respReader.readLine()) != null){
                 pw.print(sz);
               }
               String jsonResp = sw.getBuffer().toString();
               System.out.println("ClusterSelectedRegionsMemberServiceTest :: JSON response returned : " + jsonResp);
               EntityUtils.consume(entity);

               JSONObject jsonObj = new JSONObject(jsonResp);
               JSONObject clusterSelectedRegionObj = jsonObj.getJSONObject("ClusterSelectedRegionsMember");
               Assert.assertNotNull("ClusterSelectedRegionsMemberServiceTest :: Server returned null for ClusterSelectedRegionsMember",clusterSelectedRegionObj);
               JSONObject jsonObjRegion = clusterSelectedRegionObj.getJSONObject("selectedRegionsMembers");
               Assert.assertNotNull("ClusterSelectedRegionsMemberServiceTest :: Server returned null for selectedRegionsMembers",jsonObjRegion);
               Assert.assertTrue("ClusterSelectedRegionsMemberServiceTest :: Server did not return error on non-existent region",jsonObjRegion.has("errorOnRegion"));
             } finally {
               response.close();
             }
         } catch(Exception failed) {
           logException(failed);
           Assert.fail("Exception ! ");
         }
     } else {
       Assert.fail("ClusterSelectedRegionsMemberServiceTest :: No Http connection was established.");
     }
     System.out.println("ClusterSelectedRegionsMemberServiceTest ::  ------TESTCASE END : NON-EXISTENT REGION CHECK FOR CLUSTER REGION MEMBERS------");
  }

  /**
  *
  * Tests that response is for same region
  *
  *
  * @author rbhandekar
  */
  @Test
  public void testResponseRegionOnMemberAccessor() {
    System.out.println("ClusterSelectedRegionsMemberServiceTest ::  ------TESTCASE BEGIN : ACCESSOR RESPONSE CHECK FOR CLUSTER REGION MEMBERS------");
     if(httpclient != null){
         try{
             HttpUriRequest pulseupdate = RequestBuilder.post()
                 .setUri(new URI(PULSE_UPDATE_URL))
                 .addParameter(PULSE_UPDATE_PARAM, PULSE_UPDATE_3_VALUE)
                 .build();
             CloseableHttpResponse response = httpclient.execute(pulseupdate);
             try {
               HttpEntity entity = response.getEntity();

               System.out.println("ClusterSelectedRegionsMemberServiceTest :: HTTP request status : " + response.getStatusLine());

               BufferedReader respReader = new BufferedReader(new InputStreamReader(entity.getContent()));
               StringWriter sw = new StringWriter();
               PrintWriter pw = new PrintWriter(sw);
               String sz = null;
               while((sz = respReader.readLine()) != null){
                 pw.print(sz);
               }
               String jsonResp = sw.getBuffer().toString();
               System.out.println("ClusterSelectedRegionsMemberServiceTest :: JSON response returned : " + jsonResp);
               EntityUtils.consume(entity);

               JSONObject jsonObj = new JSONObject(jsonResp);
               JSONObject clusterSelectedRegionObj = jsonObj.getJSONObject("ClusterSelectedRegionsMember");
               Assert.assertNotNull("ClusterSelectedRegionsMemberServiceTest :: Server returned null response for ClusterSelectedRegionsMember",clusterSelectedRegionObj);
               JSONObject jsonObjRegion = clusterSelectedRegionObj.getJSONObject("selectedRegionsMembers");
               Assert.assertNotNull("ClusterSelectedRegionsMemberServiceTest :: Server returned null response for selectedRegionsMembers",jsonObjRegion);
               Iterator<String> itrMemberNames = jsonObjRegion.keys();
               Assert.assertNotNull("ClusterSelectedRegionsMemberServiceTest :: Server returned null region on member info", itrMemberNames);
               while(itrMemberNames.hasNext()){
                 String szMemberName = itrMemberNames.next();
                 Assert.assertNotNull("ClusterSelectedRegionsMemberServiceTest :: Server returned null member name", szMemberName);
                 Assert.assertTrue("Server did not return member details",jsonObjRegion.has(szMemberName));
                 JSONObject jsonMemberObj = jsonObjRegion.getJSONObject(szMemberName);

                 Assert.assertTrue("ClusterSelectedRegionsMemberServiceTest :: Server did not return 'accessor' of region on member",jsonMemberObj.has("accessor"));
                 String szAccessor = jsonMemberObj.getString("accessor");
                 Assert.assertTrue("ClusterSelectedRegionsMemberServiceTest :: Server returned non-boolean value for accessor attribute", ((szAccessor.equalsIgnoreCase("True"))
                         || (szAccessor.equalsIgnoreCase("False"))) );
               }
             } finally {
               response.close();
             }
         } catch(Exception failed) {
           logException(failed);
           Assert.fail("Exception ! ");
         }
     } else {
       Assert.fail("ClusterSelectedRegionsMemberServiceTest :: No Http connection was established.");
     }
     System.out.println("ClusterSelectedRegionsMemberServiceTest ::  ------TESTCASE END : ACCESSOR RESPONSE CHECK FOR CLUSTER REGION MEMBERS------");
  }
}
