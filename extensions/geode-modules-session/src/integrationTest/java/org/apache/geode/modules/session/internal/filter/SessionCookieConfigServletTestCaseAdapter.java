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
package org.apache.geode.modules.session.internal.filter;

import java.util.ArrayList;
import java.util.List;

import jakarta.servlet.Filter;
import jakarta.servlet.FilterChain;
import jakarta.servlet.FilterConfig;
import jakarta.servlet.ServletRequest;
import jakarta.servlet.ServletResponse;
import jakarta.servlet.SessionCookieConfig;
import jakarta.servlet.http.HttpServlet;
import org.springframework.mock.web.MockFilterChain;
import org.springframework.mock.web.MockFilterConfig;
import org.springframework.mock.web.MockHttpServletRequest;
import org.springframework.mock.web.MockHttpServletResponse;
import org.springframework.mock.web.MockServletContext;

/**
 * Test adapter for servlet and filter integration tests with SessionCookieConfig support.
 *
 * <p>
 * <b>Jakarta EE 10 Migration:</b> This class was completely rewritten for Spring Mock Web.
 *
 * <p>
 * <b>Original Implementation (pre-migration):</b>
 * <ul>
 * <li>Extended MockRunner's {@code BasicServletTestCaseAdapter}</li>
 * <li>Provided SessionCookieConfig support via custom {@code WebMockObjectFactory}</li>
 * <li>Relied on MockRunner's servlet test infrastructure</li>
 * </ul>
 *
 * <p>
 * <b>Current Implementation (Spring Mock Web):</b>
 * <ul>
 * <li>Standalone class (no inheritance from test frameworks)</li>
 * <li>Uses Spring's {@code MockHttpServletRequest}, {@code MockHttpServletResponse},
 * {@code MockFilterChain}</li>
 * <li>Custom {@code MyMockServletContext} with {@code SessionCookieConfig} implementation</li>
 * <li>Manual filter chain execution with request/response capture</li>
 * </ul>
 *
 * <p>
 * <b>Why the rewrite:</b> MockRunner lacks Jakarta EE support, requiring migration to
 * Spring Mock Web. Spring's mock objects have different APIs and initialization behavior,
 * necessitating a complete reimplementation of the test adapter pattern.
 */
public class SessionCookieConfigServletTestCaseAdapter {

  protected MyMockServletContext servletContext;
  protected MockHttpServletRequest request;
  protected MockHttpServletResponse response;
  protected MockFilterConfig filterConfig;
  protected HttpServlet servlet;
  protected List<Filter> filters = new ArrayList<>();

  protected ServletRequest filteredRequest;
  protected ServletResponse filteredResponse;

  private MyMockSessionCookieConfig sessionCookieConfig = new MyMockSessionCookieConfig();
  private boolean doChain = false;

  protected void setUp() throws Exception {
    setup();
  }

  protected void setup() {
    servletContext = new MyMockServletContext();
    request = new MockHttpServletRequest(servletContext);
    // CRITICAL: Spring's MockHttpServletRequest initializes with an empty string ("") for the
    // HTTP method, not "GET" like Mockrunner did. Without explicitly setting the method,
    // HttpServlet.service() won't dispatch to doGet()/doPost()/etc., causing servlets to
    // execute but do nothing. This was the root cause of testGetAttributeRequest2 failures.
    request.setMethod("GET");
    response = new MockHttpServletResponse();
  }

  @SuppressWarnings("unchecked")
  protected <T extends Filter> T createFilter(Class<T> filterClass) {
    try {
      T filter = filterClass.getDeclaredConstructor().newInstance();
      // Use the filterConfig if it was set, otherwise create a new one
      if (filterConfig == null) {
        filterConfig = new MockFilterConfig(servletContext);
      }
      filter.init(filterConfig);
      filters.add(filter);
      return filter;
    } catch (Exception e) {
      throw new RuntimeException("Failed to create filter", e);
    }
  }

  @SuppressWarnings("unchecked")
  protected <T extends HttpServlet> T createServlet(Class<T> servletClass) {
    try {
      servlet = servletClass.getDeclaredConstructor().newInstance();
      servlet.init(); // Initialize the servlet
      return (T) servlet;
    } catch (Exception e) {
      throw new RuntimeException("Failed to create servlet", e);
    }
  }

  protected HttpServlet getServlet() {
    return servlet;
  }

  /**
   * Executes the filter chain and captures the filtered request/response.
   *
   * <p>
   * <b>Why the custom implementation:</b> MockRunner's {@code BasicServletTestCaseAdapter}
   * handled filter execution and request/response capture automatically. Spring Mock Web's
   * {@code MockFilterChain} doesn't capture intermediate request/response objects, so we
   * inject a custom capturing filter at the end of the chain to grab the filtered
   * request/response for test assertions via {@link #getFilteredRequest()}.
   */
  protected void doFilter() {
    try {
      Filter capturingFilter = new Filter() {
        @Override
        public void init(FilterConfig filterConfig) {}

        @Override
        public void doFilter(ServletRequest req, ServletResponse resp, FilterChain chain) {
          filteredRequest = req;
          filteredResponse = resp;
          try {
            chain.doFilter(req, resp);
          } catch (Exception e) {
            throw new RuntimeException(e);
          }
        }

        @Override
        public void destroy() {}
      };

      List<Filter> allFilters = new ArrayList<>(filters);
      allFilters.add(capturingFilter);

      FilterChain chain = new MockFilterChain(servlet, allFilters.toArray(new Filter[0]));
      chain.doFilter(request, response);
    } catch (Exception e) {
      throw new RuntimeException("Filter execution failed", e);
    }
  }

  protected ServletRequest getFilteredRequest() {
    return filteredRequest != null ? filteredRequest : request;
  }

  protected void setDoChain(boolean doChain) {
    this.doChain = doChain;
  }

  protected WebMockObjectFactory getWebMockObjectFactory() {
    return new WebMockObjectFactory(this, servletContext, request, response);
  }

  protected static class MyMockServletContext extends MockServletContext {
    private final MyMockSessionCookieConfig sessionCookieConfig = new MyMockSessionCookieConfig();

    @Override
    public SessionCookieConfig getSessionCookieConfig() {
      return sessionCookieConfig;
    }
  }

  /**
   * Custom SessionCookieConfig implementation for testing.
   *
   * <p>
   * <b>Why this exists:</b> MockRunner's {@code MockSessionCookieConfig} doesn't implement
   * the {@code SessionCookieConfig} interface in older versions. The original code had a workaround
   * class that extended MockRunner's class AND implemented the interface. Spring Mock Web doesn't
   * provide a SessionCookieConfig implementation at all, so this is a full implementation
   * supporting all Jakarta Servlet SessionCookieConfig methods for test purposes.
   */
  private static class MyMockSessionCookieConfig implements SessionCookieConfig {
    private java.util.Map<String, String> attributes = new java.util.HashMap<>();
    private String name;
    private String domain;
    private String path;
    private String comment;
    private boolean httpOnly;
    private boolean secure;
    private int maxAge = -1;

    public java.util.Map<String, String> getAttributes() {
      return attributes;
    }

    public void setAttribute(String name, String value) {
      attributes.put(name, value);
    }

    @Override
    public String getAttribute(String name) {
      return attributes.get(name);
    }

    @Override
    public String getName() {
      return name;
    }

    @Override
    public void setName(String name) {
      this.name = name;
    }

    @Override
    public String getDomain() {
      return domain;
    }

    @Override
    public void setDomain(String domain) {
      this.domain = domain;
    }

    @Override
    public String getPath() {
      return path;
    }

    @Override
    public void setPath(String path) {
      this.path = path;
    }

    @Override
    public String getComment() {
      return comment;
    }

    @Override
    public void setComment(String comment) {
      this.comment = comment;
    }

    @Override
    public boolean isHttpOnly() {
      return httpOnly;
    }

    @Override
    public void setHttpOnly(boolean httpOnly) {
      this.httpOnly = httpOnly;
    }

    @Override
    public boolean isSecure() {
      return secure;
    }

    @Override
    public void setSecure(boolean secure) {
      this.secure = secure;
    }

    @Override
    public int getMaxAge() {
      return maxAge;
    }

    @Override
    public void setMaxAge(int maxAge) {
      this.maxAge = maxAge;
    }
  }

  /**
   * Compatibility wrapper providing MockRunner's WebMockObjectFactory API using Spring Mock
   * objects.
   *
   * <p>
   * <b>Why this exists:</b> The original test code expects MockRunner's
   * {@code WebMockObjectFactory}
   * API for accessing mock servlet objects. This class provides the same API contract but delegates
   * to Spring Mock Web objects internally, allowing existing test code to work without changes.
   *
   * <p>
   * Key API compatibility methods:
   * <ul>
   * <li>{@code getMockServletContext()} - returns Spring's MockServletContext</li>
   * <li>{@code getMockRequest()} - returns Spring's MockHttpServletRequest</li>
   * <li>{@code getMockResponse()} - returns Spring's MockHttpServletResponse</li>
   * <li>{@code createMockRequest()} - creates new Spring MockHttpServletRequest</li>
   * <li>{@code addRequestWrapper()} - simulates request wrapping (copies state instead)</li>
   * </ul>
   */
  public static class WebMockObjectFactory {
    private final SessionCookieConfigServletTestCaseAdapter adapter;
    private final MockServletContext servletContext;
    private final MockHttpServletRequest request;
    private final MockHttpServletResponse response;

    public WebMockObjectFactory(MockServletContext servletContext,
        MockHttpServletRequest request,
        MockHttpServletResponse response) {
      this.adapter = null;
      this.servletContext = servletContext;
      this.request = request;
      this.response = response;
    }

    public WebMockObjectFactory(SessionCookieConfigServletTestCaseAdapter adapter,
        MockServletContext servletContext,
        MockHttpServletRequest request,
        MockHttpServletResponse response) {
      this.adapter = adapter;
      this.servletContext = servletContext;
      this.request = request;
      this.response = response;
    }

    public MockServletContext getMockServletContext() {
      return servletContext;
    }

    public MockHttpServletRequest getMockRequest() {
      return adapter != null ? adapter.request : request;
    }

    public MockHttpServletResponse getMockResponse() {
      return adapter != null ? adapter.response : response;
    }

    public MockHttpServletRequest createMockRequest() {
      return new MockHttpServletRequest(servletContext);
    }

    public void addRequestWrapper(MockHttpServletRequest newRequest) {
      if (adapter != null) {
        // Spring Mock Web doesn't support request wrapping like MockRunner did.
        // Instead, copy the new request's properties into the existing request object.
        // This simulates the wrapping behavior expected by test code that creates
        // a new request with different URI/cookies and expects it to be "wrapped" into the chain.
        adapter.request.setRequestURI(newRequest.getRequestURI());
        adapter.request.setContextPath(newRequest.getContextPath());
        // Copy cookies
        for (jakarta.servlet.http.Cookie cookie : newRequest.getCookies()) {
          adapter.request.setCookies(cookie);
        }
      }
    }
  }
}
