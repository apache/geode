package com.gemstone.gemfire.rest.internal.web;

import static org.junit.Assert.*;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;

import org.apache.http.HttpEntity;
import org.apache.http.client.ClientProtocolException;
import org.apache.http.client.methods.CloseableHttpResponse;
import org.apache.http.client.methods.HttpGet;
import org.apache.http.impl.client.CloseableHttpClient;
import org.apache.http.impl.client.HttpClients;
import org.json.JSONArray;
import org.json.JSONException;
import org.junit.Test;
import org.junit.experimental.categories.Category;

import com.gemstone.gemfire.internal.AvailablePortHelper;
import com.gemstone.gemfire.security.AbstractSecureServerDUnitTest;
import com.gemstone.gemfire.test.junit.categories.DistributedTest;
import com.gemstone.gemfire.test.junit.categories.SecurityTest;

@Category({ DistributedTest.class, SecurityTest.class})
public class RestSecurityDUnitTest extends AbstractSecureServerDUnitTest {
  private String endPoint = null;
  public RestSecurityDUnitTest(){
    int[] ports = AvailablePortHelper.getRandomAvailableTCPPorts(2);
    this.jmxPort = ports[0];
    this.restPort = ports[1];
    endPoint = "http://localhost:"+restPort+"/gemfire-api/v1";
  }
  @Test
  public void test(){
    client1.invoke(()->{
      JSONArray response = doGet("/servers");
      assertEquals(response.length(), 1);
      assertEquals(response.get(0), "http://localhost:"+this.restPort);
    });
  }


  private JSONArray doGet(String uri) {
    HttpGet get = new HttpGet(endPoint + uri);
    get.addHeader("Content-Type", "application/json");
    get.addHeader("Accept", "application/json");
    CloseableHttpClient httpclient = HttpClients.createDefault();
    CloseableHttpResponse response;

    try {
      response = httpclient.execute(get);
      HttpEntity entity = response.getEntity();
      InputStream content = entity.getContent();
      BufferedReader reader = new BufferedReader(new InputStreamReader(
        content));
      String line;
      StringBuffer str = new StringBuffer();
      while ((line = reader.readLine()) != null) {
        str.append(line);
      }

      //validate the satus code
      assertEquals(response.getStatusLine().getStatusCode(), 200);
      return new JSONArray(str.toString());
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
    return null;
  }

}
