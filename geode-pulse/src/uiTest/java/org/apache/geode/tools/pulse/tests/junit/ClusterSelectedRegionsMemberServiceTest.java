/*
 *
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
 *
 */
package org.apache.geode.tools.pulse.tests.junit;

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
import org.json.JSONObject;
import org.junit.After;
import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Ignore;
import org.junit.Test;

/**
 * JUnit Tests for ClusterSelectedRegionsMemberService in the back-end server for region detail page
 *
 *
 */
@Ignore
public class ClusterSelectedRegionsMemberServiceTest extends BaseServiceTest {

  @BeforeClass
  public static void setUpBeforeClass() throws Exception {
    doLogin();
    System.out.println("\n\nClusterSelectedRegionsMemberServiceTest :: Setup done");
  }

  @AfterClass
  public static void tearDownAfterClass() throws Exception {
    doLogout();
    System.out.println("ClusterSelectedRegionsMemberServiceTest :: Teardown done");
  }

  @Override
  @Before
  public void setUp() {
    System.out.println("running setup -- ClusterSelectedRegionsMemberServiceTest");
  }

  @Override
  @After
  public void tearDown() {
    System.out.println("running teardown -- ClusterSelectedRegionsMemberServiceTest");
  }

  /**
   * Tests that service returns json object
   *
   */
  @Test
  public void testResponseNotNull() {
    System.out.println(
        "ClusterSelectedRegionsMemberServiceTest ::  ------TESTCASE BEGIN : NULL RESPONSE CHECK FOR CLUSTER REGION MEMBERS------");
    if (httpclient != null) {
      try {
        HttpUriRequest pulseupdate = RequestBuilder.post().setUri(new URI(PULSE_UPDATE_URL))
            .addParameter(PULSE_UPDATE_PARAM, PULSE_UPDATE_3_VALUE).build();
        try (CloseableHttpResponse response = httpclient.execute(pulseupdate)) {
          HttpEntity entity = response.getEntity();

          System.out.println("ClusterSelectedRegionsMemberServiceTest :: HTTP request status : "
              + response.getStatusLine());

          BufferedReader respReader =
              new BufferedReader(new InputStreamReader(entity.getContent()));
          StringWriter sw = new StringWriter();
          PrintWriter pw = new PrintWriter(sw);
          String sz;
          while ((sz = respReader.readLine()) != null) {
            pw.print(sz);
          }
          String jsonResp = sw.getBuffer().toString();
          System.out.println(
              "ClusterSelectedRegionsMemberServiceTest :: JSON response returned : " + jsonResp);
          EntityUtils.consume(entity);

          JSONObject jsonObj = new JSONObject(jsonResp);
          Assert.assertNotNull(
              "ClusterSelectedRegionsMemberServiceTest :: Server returned null response for ClusterSelectedRegionsMember",
              jsonObj.getJSONObject("ClusterSelectedRegionsMember"));
        }
      } catch (Exception failed) {
        logException(failed);
        Assert.fail("Exception ! ");
      }
    } else {
      Assert.fail("ClusterSelectedRegionsMemberServiceTest :: No Http connection was established.");
    }
    System.out.println(
        "ClusterSelectedRegionsMemberServiceTest ::  ------TESTCASE END : NULL RESPONSE CHECK FOR CLUSTER REGION MEMBERS------");
  }

  /**
   *
   * Tests that response is for same logged in user
   *
   */
  @Test
  public void testResponseUsername() {
    System.out.println(
        "ClusterSelectedRegionsMemberServiceTest ::  ------TESTCASE BEGIN : NULL USERNAME IN RESPONSE CHECK FOR CLUSTER REGION MEMBERS------");
    if (httpclient != null) {
      try {
        HttpUriRequest pulseupdate = RequestBuilder.post().setUri(new URI(PULSE_UPDATE_URL))
            .addParameter(PULSE_UPDATE_PARAM, PULSE_UPDATE_3_VALUE).build();
        try (CloseableHttpResponse response = httpclient.execute(pulseupdate)) {
          HttpEntity entity = response.getEntity();

          System.out.println("ClusterSelectedRegionsMemberServiceTest :: HTTP request status : "
              + response.getStatusLine());

          BufferedReader respReader =
              new BufferedReader(new InputStreamReader(entity.getContent()));
          StringWriter sw = new StringWriter();
          PrintWriter pw = new PrintWriter(sw);
          String sz;
          while ((sz = respReader.readLine()) != null) {
            pw.print(sz);
          }
          String jsonResp = sw.getBuffer().toString();
          System.out.println(
              "ClusterSelectedRegionsMemberServiceTest :: JSON response returned : " + jsonResp);
          EntityUtils.consume(entity);

          JSONObject jsonObj = new JSONObject(jsonResp);
          JSONObject clusterSelectedRegionObj =
              jsonObj.getJSONObject("ClusterSelectedRegionsMember");
          Assert.assertNotNull(
              "ClusterSelectedRegionsMemberServiceTest :: Server returned null response for ClusterSelectedRegionsMember",
              clusterSelectedRegionObj);
          Assert.assertTrue(
              "ClusterSelectedRegionsMemberServiceTest :: Server did not return 'userName' in request",
              clusterSelectedRegionObj.has("userName"));
          String szUser = clusterSelectedRegionObj.getString("userName");
          Assert.assertEquals(
              "ClusterSelectedRegionsMemberServiceTest :: Server returned wrong user name. Expected was admin. Server returned = "
                  + szUser,
              szUser, "admin");
        }
      } catch (Exception failed) {
        logException(failed);
        Assert.fail("Exception ! ");
      }
    } else {
      Assert.fail("ClusterSelectedRegionsMemberServiceTest :: No Http connection was established.");
    }
    System.out.println(
        "ClusterSelectedRegionsMemberServiceTest ::  ------TESTCASE END : NULL USERNAME IN RESPONSE CHECK FOR CLUSTER REGION MEMBERS------");
  }

  /**
   *
   * Tests that response is for same region
   *
   *
   */
  @Test
  public void testResponseRegionOnMemberInfoMatches() {
    System.out.println(
        "ClusterSelectedRegionsMemberServiceTest ::  ------TESTCASE BEGIN : MEMBER INFO RESPONSE CHECK FOR CLUSTER REGION MEMBERS------");
    if (httpclient != null) {
      try {
        HttpUriRequest pulseupdate = RequestBuilder.post().setUri(new URI(PULSE_UPDATE_URL))
            .addParameter(PULSE_UPDATE_PARAM, PULSE_UPDATE_3_VALUE).build();
        try (CloseableHttpResponse response = httpclient.execute(pulseupdate)) {
          HttpEntity entity = response.getEntity();

          System.out.println("ClusterSelectedRegionsMemberServiceTest :: HTTP request status : "
              + response.getStatusLine());

          BufferedReader respReader =
              new BufferedReader(new InputStreamReader(entity.getContent()));
          StringWriter sw = new StringWriter();
          PrintWriter pw = new PrintWriter(sw);
          String sz;
          while ((sz = respReader.readLine()) != null) {
            pw.print(sz);
          }
          String jsonResp = sw.getBuffer().toString();
          System.out.println(
              "ClusterSelectedRegionsMemberServiceTest :: JSON response returned : " + jsonResp);
          EntityUtils.consume(entity);

          JSONObject jsonObj = new JSONObject(jsonResp);
          JSONObject clusterSelectedRegionObj =
              jsonObj.getJSONObject("ClusterSelectedRegionsMember");
          Assert.assertNotNull(
              "ClusterSelectedRegionsMemberServiceTest :: Server returned null response for ClusterSelectedRegionsMember",
              clusterSelectedRegionObj);
          JSONObject jsonObjRegion =
              clusterSelectedRegionObj.getJSONObject("selectedRegionsMembers");
          Assert.assertNotNull(
              "ClusterSelectedRegionsMemberServiceTest :: Server returned null response for selectedRegionsMembers",
              jsonObjRegion);
          @SuppressWarnings("unchecked")
          Iterator<String> itrMemberNames = jsonObjRegion.keys();
          Assert.assertNotNull(
              "ClusterSelectedRegionsMemberServiceTest :: Server returned null region on member info",
              itrMemberNames);
          while (itrMemberNames.hasNext()) {
            String szMemberName = itrMemberNames.next();
            Assert.assertNotNull(
                "ClusterSelectedRegionsMemberServiceTest :: Server returned null member name",
                szMemberName);
            Assert.assertTrue("Server did not return member details",
                jsonObjRegion.has(szMemberName));
            JSONObject jsonMemberObj = jsonObjRegion.getJSONObject(szMemberName);

            Assert.assertTrue(
                "ClusterSelectedRegionsMemberServiceTest :: Server did not return 'regionFullPath' of region on member",
                jsonMemberObj.has("regionFullPath"));
            String szPath = jsonMemberObj.getString("regionFullPath");
            Assert.assertEquals(
                "ClusterSelectedRegionsMemberServiceTest :: Server returned wrong region path for region on member",
                szPath, "/GlobalVilage_2/GlobalVilage_9");
          }
        }
      } catch (Exception failed) {
        logException(failed);
        Assert.fail("Exception ! ");
      }
    } else {
      Assert.fail("ClusterSelectedRegionsMemberServiceTest :: No Http connection was established.");
    }
    System.out.println(
        "ClusterSelectedRegionsMemberServiceTest ::  ------TESTCASE END : MEMBER INFO RESPONSE CHECK FOR CLUSTER REGION MEMBERS------");
  }

  /**
   *
   * Tests that response is for same region
   *
   *
   */
  @Test
  public void testResponseNonExistentRegion() {
    System.out.println(
        "ClusterSelectedRegionsMemberServiceTest ::  ------TESTCASE BEGIN : NON-EXISTENT REGION CHECK FOR CLUSTER REGION MEMBERS------");
    if (httpclient != null) {
      try {
        System.out.println("Test for non-existent region : /Rubbish");
        HttpUriRequest pulseupdate = RequestBuilder.post().setUri(new URI(PULSE_UPDATE_URL))
            .addParameter(PULSE_UPDATE_PARAM, PULSE_UPDATE_4_VALUE).build();
        try (CloseableHttpResponse response = httpclient.execute(pulseupdate)) {
          HttpEntity entity = response.getEntity();

          System.out.println("ClusterSelectedRegionsMemberServiceTest :: HTTP request status : "
              + response.getStatusLine());

          BufferedReader respReader =
              new BufferedReader(new InputStreamReader(entity.getContent()));
          StringWriter sw = new StringWriter();
          PrintWriter pw = new PrintWriter(sw);
          String sz;
          while ((sz = respReader.readLine()) != null) {
            pw.print(sz);
          }
          String jsonResp = sw.getBuffer().toString();
          System.out.println(
              "ClusterSelectedRegionsMemberServiceTest :: JSON response returned : " + jsonResp);
          EntityUtils.consume(entity);

          JSONObject jsonObj = new JSONObject(jsonResp);
          JSONObject clusterSelectedRegionObj =
              jsonObj.getJSONObject("ClusterSelectedRegionsMember");
          Assert.assertNotNull(
              "ClusterSelectedRegionsMemberServiceTest :: Server returned null for ClusterSelectedRegionsMember",
              clusterSelectedRegionObj);
          JSONObject jsonObjRegion =
              clusterSelectedRegionObj.getJSONObject("selectedRegionsMembers");
          Assert.assertNotNull(
              "ClusterSelectedRegionsMemberServiceTest :: Server returned null for selectedRegionsMembers",
              jsonObjRegion);
          Assert.assertTrue(
              "ClusterSelectedRegionsMemberServiceTest :: Server did not return error on non-existent region",
              jsonObjRegion.has("errorOnRegion"));
        }
      } catch (Exception failed) {
        logException(failed);
        Assert.fail("Exception ! ");
      }
    } else {
      Assert.fail("ClusterSelectedRegionsMemberServiceTest :: No Http connection was established.");
    }
    System.out.println(
        "ClusterSelectedRegionsMemberServiceTest ::  ------TESTCASE END : NON-EXISTENT REGION CHECK FOR CLUSTER REGION MEMBERS------");
  }

  /**
   *
   * Tests that response is for same region
   *
   *
   */
  @Test
  public void testResponseRegionOnMemberAccessor() {
    System.out.println(
        "ClusterSelectedRegionsMemberServiceTest ::  ------TESTCASE BEGIN : ACCESSOR RESPONSE CHECK FOR CLUSTER REGION MEMBERS------");
    if (httpclient != null) {
      try {
        HttpUriRequest pulseupdate = RequestBuilder.post().setUri(new URI(PULSE_UPDATE_URL))
            .addParameter(PULSE_UPDATE_PARAM, PULSE_UPDATE_3_VALUE).build();
        try (CloseableHttpResponse response = httpclient.execute(pulseupdate)) {
          HttpEntity entity = response.getEntity();

          System.out.println("ClusterSelectedRegionsMemberServiceTest :: HTTP request status : "
              + response.getStatusLine());

          BufferedReader respReader =
              new BufferedReader(new InputStreamReader(entity.getContent()));
          StringWriter sw = new StringWriter();
          PrintWriter pw = new PrintWriter(sw);
          String sz;
          while ((sz = respReader.readLine()) != null) {
            pw.print(sz);
          }
          String jsonResp = sw.getBuffer().toString();
          System.out.println(
              "ClusterSelectedRegionsMemberServiceTest :: JSON response returned : " + jsonResp);
          EntityUtils.consume(entity);

          JSONObject jsonObj = new JSONObject(jsonResp);
          JSONObject clusterSelectedRegionObj =
              jsonObj.getJSONObject("ClusterSelectedRegionsMember");
          Assert.assertNotNull(
              "ClusterSelectedRegionsMemberServiceTest :: Server returned null response for ClusterSelectedRegionsMember",
              clusterSelectedRegionObj);
          JSONObject jsonObjRegion =
              clusterSelectedRegionObj.getJSONObject("selectedRegionsMembers");
          Assert.assertNotNull(
              "ClusterSelectedRegionsMemberServiceTest :: Server returned null response for selectedRegionsMembers",
              jsonObjRegion);
          @SuppressWarnings("unchecked")
          Iterator<String> itrMemberNames = jsonObjRegion.keys();
          Assert.assertNotNull(
              "ClusterSelectedRegionsMemberServiceTest :: Server returned null region on member info",
              itrMemberNames);
          while (itrMemberNames.hasNext()) {
            String szMemberName = itrMemberNames.next();
            Assert.assertNotNull(
                "ClusterSelectedRegionsMemberServiceTest :: Server returned null member name",
                szMemberName);
            Assert.assertTrue("Server did not return member details",
                jsonObjRegion.has(szMemberName));
            JSONObject jsonMemberObj = jsonObjRegion.getJSONObject(szMemberName);

            Assert.assertTrue(
                "ClusterSelectedRegionsMemberServiceTest :: Server did not return 'accessor' of region on member",
                jsonMemberObj.has("accessor"));
            String szAccessor = jsonMemberObj.getString("accessor");
            Assert.assertTrue(
                "ClusterSelectedRegionsMemberServiceTest :: Server returned non-boolean value for accessor attribute",
                ((szAccessor.equalsIgnoreCase("True")) || (szAccessor.equalsIgnoreCase("False"))));
          }
        }
      } catch (Exception failed) {
        logException(failed);
        Assert.fail("Exception ! ");
      }
    } else {
      Assert.fail("ClusterSelectedRegionsMemberServiceTest :: No Http connection was established.");
    }
    System.out.println(
        "ClusterSelectedRegionsMemberServiceTest ::  ------TESTCASE END : ACCESSOR RESPONSE CHECK FOR CLUSTER REGION MEMBERS------");
  }
}
