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
package org.apache.geode.session.tests;

import java.io.IOException;
import java.net.URISyntaxException;
import java.util.function.Function;

import javax.servlet.http.HttpServletRequest;

import org.apache.http.Header;
import org.apache.http.StatusLine;
import org.apache.http.client.methods.CloseableHttpResponse;
import org.apache.http.client.methods.HttpGet;
import org.apache.http.client.protocol.HttpClientContext;
import org.apache.http.client.utils.URIBuilder;
import org.apache.http.impl.client.BasicCookieStore;
import org.apache.http.impl.client.CloseableHttpClient;
import org.apache.http.impl.client.HttpClients;
import org.apache.http.impl.cookie.BasicClientCookie;
import org.apache.http.protocol.BasicHttpContext;
import org.apache.http.protocol.HttpContext;
import org.apache.http.util.EntityUtils;

import org.apache.geode.modules.session.CommandServlet;
import org.apache.geode.modules.session.QueryCommand;

/**
 * A simple http client that talks to a server running the session-testing-war.
 *
 * This client sends commands to the {@link CommandServlet} over http to modify session properties
 * and returns the results. The client has support for connecting to multiple servers and sending
 * the session cookie returned by one server to other servers, to emulate the behavior of a client
 * talking to the servers through a load balancer.
 *
 * The client currently only targets servers running on "localhost"
 *
 * To set the server this client is targeting, use {@link #setPort}.
 */
public class Client {
  private static final String HOST = "localhost";
  private int port = 8080;
  private String cookie;
  private HttpContext context;

  private URIBuilder reqURIBuild;
  private CloseableHttpClient httpclient;

  public Client() {
    reqURIBuild = new URIBuilder();
    reqURIBuild.setScheme("http");

    httpclient = HttpClients.createDefault();
    context = new BasicHttpContext();

    cookie = null;
  }

  /**
   * Change the server that the client is targeting.
   *
   * @param port the port that the server is listening on
   */
  public void setPort(int port) {
    this.port = port;
  }

  /**
   * Get the value of a session attribute on the server
   */
  public Response get(String key) throws IOException, URISyntaxException {
    return get(key, true);
  }

  /**
   * Get the value of a session attribute on the server
   */
  public Response get(String key, boolean storeRespCookie) throws IOException, URISyntaxException {
    resetURI();
    reqURIBuild.setParameter("cmd", QueryCommand.GET.name());
    reqURIBuild.setParameter("param", key);

    return doRequest(new HttpGet(reqURIBuild.build()), storeRespCookie);
  }

  /**
   * Set the value of a session attribute on the server
   */
  public Response set(String key, String value) throws IOException, URISyntaxException {
    return set(key, value, true);
  }

  /**
   * Set the value of a session attribute on the server
   */
  public Response set(String key, String value, boolean storeRespCookie)
      throws IOException, URISyntaxException {
    resetURI();
    reqURIBuild.setParameter("cmd", QueryCommand.SET.name());
    reqURIBuild.setParameter("param", key);
    reqURIBuild.setParameter("value", value);

    return doRequest(new HttpGet(reqURIBuild.build()), storeRespCookie);
  }

  /**
   * Instantiate and execute a function in the server. Note that this function must be present in
   * the sesssion testing war.
   */
  public Response executionFunction(
      Class<? extends Function<HttpServletRequest, String>> functionClass)
      throws IOException, URISyntaxException {
    resetURI();
    reqURIBuild.setParameter("cmd", QueryCommand.FUNCTION.name());
    reqURIBuild.setParameter("function", functionClass.getName());

    return doRequest(new HttpGet(reqURIBuild.build()), true);
  }

  /**
   * Remove the session attribute on the server
   *
   * @param key - the session attribute to remove
   */
  public Response remove(String key) throws IOException, URISyntaxException {
    return remove(key, true);
  }

  /**
   * Remove the session attribute on the server
   *
   * @param key - the session attribute to remove
   * @param storeRespCookie - whether or not to store the session cookie of this request
   */
  public Response remove(String key, boolean storeRespCookie)
      throws URISyntaxException, IOException {
    resetURI();
    reqURIBuild.setParameter("cmd", QueryCommand.REMOVE.name());
    reqURIBuild.setParameter("param", key);

    return doRequest(new HttpGet(reqURIBuild.build()), storeRespCookie);
  }

  /**
   * Invalidate this clients session on the server
   */
  public Response invalidate() throws IOException, URISyntaxException {
    return invalidate(true);
  }

  /**
   * Invalidate this clients session on the server
   */
  public Response invalidate(boolean storeRespCookie) throws IOException, URISyntaxException {
    resetURI();
    reqURIBuild.setParameter("cmd", QueryCommand.INVALIDATE.name());
    reqURIBuild.setParameter("param", "null");

    return doRequest(new HttpGet(reqURIBuild.build()), storeRespCookie);
  }

  /**
   * Set the maximum inactive interval for this client's session on the server.
   *
   * If this time interval elapses without activity on the session, the session will expire.
   *
   * @param time - Time in seconds until the session should expire
   */
  public Response setMaxInactive(int time) throws IOException, URISyntaxException {
    return setMaxInactive(time, true);
  }

  /**
   * Set the maximum inactive interval for this client's session on the server.
   *
   * If this time interval elapses without activity on the session, the session will expire.
   *
   * @param time - Time in seconds until the session should expire
   */
  public Response setMaxInactive(int time, boolean storeRespCookie)
      throws IOException, URISyntaxException {
    resetURI();
    reqURIBuild.setParameter("cmd", QueryCommand.SET_MAX_INACTIVE.name());
    reqURIBuild.setParameter("value", Integer.toString(time));

    return doRequest(new HttpGet(reqURIBuild.build()), storeRespCookie);
  }

  private void resetURI() {
    reqURIBuild.setHost(HOST + ":" + port);
    reqURIBuild.clearParameters();
  }

  /**
   * Sends a request to the server and returns a custom response object with the result
   *
   * @param storeRespCookie if true, retain the value of a "Set-Cookie" header returned in the
   *        response.
   */
  private Response doRequest(HttpGet req, boolean storeRespCookie) throws IOException {
    addCookieHeader(req);

    CloseableHttpResponse resp = httpclient.execute(req, context);

    boolean isNew = true;
    String reqCookie = getCookieHeader(resp);
    if (reqCookie == null) {
      isNew = false;
      reqCookie = this.cookie;
    } else if (storeRespCookie) {
      this.cookie = reqCookie;
    }

    StatusLine status = resp.getStatusLine();
    if (status.getStatusCode() != 200) {
      throw new IOException("Http request to " + req.getURI().getHost() + "["
          + req.getURI().getPort() + "] failed. " + status);
    }

    Response response = new Response(reqCookie, EntityUtils.toString(resp.getEntity()), isNew);
    resp.close();
    return response;
  }

  private void addCookieHeader(HttpGet req) {
    // Set the cookie header
    if (cookie != null) {
      BasicClientCookie cookie = new BasicClientCookie("JSESSIONID", this.cookie);
      cookie.setDomain(req.getURI().getHost());
      cookie.setPath("/");

      BasicCookieStore cookieStore = new BasicCookieStore();
      cookieStore.addCookie(cookie);

      context.setAttribute(HttpClientContext.COOKIE_STORE, cookieStore);
    }
  }

  private String getCookieHeader(CloseableHttpResponse resp) {
    Header lastHeader = resp.getLastHeader("Set-Cookie");

    if (lastHeader == null) {
      return null;
    }
    return lastHeader.getElements()[0].getValue();
  }

  /**
   * A response received from the server.
   *
   * Currently contains the text value of the response body, as well as the session cookie.
   */
  public class Response {
    private final String sessionCookie;
    private final String response;
    private final boolean isNew;

    public Response(String sessionCookie, String response, boolean isNew) {
      this.sessionCookie = sessionCookie;
      this.response = response;
      this.isNew = isNew;
    }

    /**
     * The session cookie associated with this client.
     */
    public String getSessionCookie() {
      return sessionCookie;
    }

    /**
     *
     * The String value of the response body.
     */
    public String getResponse() {
      return response;
    }

    /**
     * @return true if this response contained a "Set-Cookie" header, indicating that the server is
     *         setting a new value for the session cookie.
     */
    public boolean isNew() {
      return isNew;
    }

    @Override
    public String toString() {
      return "Response{" + "sessionCookie='" + sessionCookie + '\'' + ", response='" + response
          + '\'' + '}';
    }
  }
}
