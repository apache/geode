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
package com.gemstone.gemfire.management.internal.web.http;

import static org.junit.Assert.*;

import java.net.URI;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.jmock.Mockery;
import org.jmock.lib.legacy.ClassImposteriser;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.springframework.http.HttpEntity;
import org.springframework.http.MediaType;
import org.springframework.util.LinkedMultiValueMap;
import org.springframework.util.MultiValueMap;

import com.gemstone.gemfire.management.internal.web.AbstractWebTestCase;
import com.gemstone.gemfire.management.internal.web.domain.Link;
import com.gemstone.gemfire.test.junit.categories.UnitTest;

/**
 * The ClientHttpRequestJUnitTest class is a test suite of test cases testing the contract and functionality of the
 * ClientHttpRequest class.
 * <p/>
 * @see com.gemstone.gemfire.management.internal.web.AbstractWebTestCase
 * @see com.gemstone.gemfire.management.internal.web.http.ClientHttpRequest
 * @see org.jmock.Mockery
 * @see org.junit.Assert
 * @see org.junit.Test
 * @since GemFire 8.0
 */
@Category(UnitTest.class)
public class ClientHttpRequestJUnitTest extends AbstractWebTestCase {

  private Mockery mockContext;

  @Before
  public void setUp() {
    mockContext = new Mockery();
    mockContext.setImposteriser(ClassImposteriser.INSTANCE);
  }

  @After
  public void tearDown() {
    mockContext.assertIsSatisfied();
    mockContext = null;
  }

  @Test
  public void testCreateClientHttpRequest() throws Exception {
    final Link expectedLink = new Link("test", toUri("http://host.domain.com:8080/app/service"));
    final ClientHttpRequest request = new ClientHttpRequest(expectedLink);

    assertNotNull(request);
    assertEquals(expectedLink, request.getLink());
  }

  @Test(expected = AssertionError.class)
  public void testCreateClientHttpRequestWithNullLink() {
    new ClientHttpRequest(null);
  }

  @Test
  public void testGetMethod() throws Exception {
    Link link = new Link("get-resource", toUri("http://host.domain.com:8080/app/resources/{id}"));
    ClientHttpRequest request = new ClientHttpRequest(link);

    assertEquals(link, request.getLink());
    assertEquals(org.springframework.http.HttpMethod.GET, request.getMethod());

    link = new Link("delete-resource", toUri("http://host.domain.com:8080/app/resources/{id}"), HttpMethod.DELETE);
    request = new ClientHttpRequest(link);

    assertEquals(link, request.getLink());
    assertEquals(org.springframework.http.HttpMethod.DELETE, request.getMethod());

    link = new Link("delete-resource", toUri("http://host.domain.com:8080/app/service"), HttpMethod.HEAD);
    request = new ClientHttpRequest(link);

    assertEquals(link, request.getLink());
    assertEquals(org.springframework.http.HttpMethod.HEAD, request.getMethod());

    link = new Link("delete-resource", toUri("http://host.domain.com:8080/app/service"), HttpMethod.OPTIONS);
    request = new ClientHttpRequest(link);

    assertEquals(link, request.getLink());
    assertEquals(org.springframework.http.HttpMethod.OPTIONS, request.getMethod());

    link = new Link("delete-resource", toUri("http://host.domain.com:8080/app/resources"), HttpMethod.POST);
    request = new ClientHttpRequest(link);

    assertEquals(link, request.getLink());
    assertEquals(org.springframework.http.HttpMethod.POST, request.getMethod());

    link = new Link("delete-resource", toUri("http://host.domain.com:8080/app/resources"), HttpMethod.PUT);
    request = new ClientHttpRequest(link);

    assertEquals(link, request.getLink());
    assertEquals(org.springframework.http.HttpMethod.PUT, request.getMethod());

    link = new Link("delete-resource", toUri("http://host.domain.com:8080/app"), HttpMethod.TRACE);
    request = new ClientHttpRequest(link);

    assertEquals(link, request.getLink());
    assertEquals(org.springframework.http.HttpMethod.TRACE, request.getMethod());
  }

  @Test
  public void testIsDelete() throws Exception {
    final Link expectedLink = new Link("delete", toUri("http://host.domain.com:8080/app/resources/{id}"), HttpMethod.DELETE);
    final ClientHttpRequest request = new ClientHttpRequest(expectedLink);

    assertEquals(expectedLink, request.getLink());
    assertTrue(request.isDelete());
    assertFalse(request.isGet());
    assertFalse(request.isPost());
    assertFalse(request.isPut());
  }

  @Test
  public void testIsGet() throws Exception {
    final Link expectedLink = new Link("get", toUri("http://host.domain.com:8080/app/resources/{id}"), HttpMethod.GET);
    final ClientHttpRequest request = new ClientHttpRequest(expectedLink);

    assertEquals(expectedLink, request.getLink());
    assertFalse(request.isDelete());
    assertTrue(request.isGet());
    assertFalse(request.isPost());
    assertFalse(request.isPut());
  }

  @Test
  public void testIsPost() throws Exception {
    final Link expectedLink = new Link("post", toUri("http://host.domain.com:8080/app/resources"), HttpMethod.POST);
    final ClientHttpRequest request = new ClientHttpRequest(expectedLink);

    assertEquals(expectedLink, request.getLink());
    assertFalse(request.isDelete());
    assertFalse(request.isGet());
    assertTrue(request.isPost());
    assertFalse(request.isPut());
  }

  @Test
  public void testIsPut() throws Exception {
    final Link expectedLink = new Link("put", toUri("http://host.domain.com:8080/app/resources/{id}"), HttpMethod.PUT);
    final ClientHttpRequest request = new ClientHttpRequest(expectedLink);

    assertEquals(expectedLink, request.getLink());
    assertFalse(request.isDelete());
    assertFalse(request.isGet());
    assertFalse(request.isPost());
    assertTrue(request.isPut());
  }

  @Test
  public void testGetPathVariables() throws Exception {
    final Link expectedLink = new Link("test", toUri("http://host.domain.com:8080/app/libraries/{name}/books/{author}/{title}"));
    final ClientHttpRequest request = new ClientHttpRequest(expectedLink);

    assertEquals(expectedLink, request.getLink());
    assertEquals(Arrays.asList("name", "author", "title"), request.getPathVariables());
  }

  @Test
  public void testGetPathVariablesWithUriHavingNoPathVariables() throws Exception {
    final Link expectedLink = new Link("test", toUri("http://host.domain.com:8080/app/service"));
    final ClientHttpRequest request = new ClientHttpRequest(expectedLink);

    assertEquals(expectedLink, request.getLink());

    final List<String> actualPathVariables = request.getPathVariables();

    assertNotNull(actualPathVariables);
    assertTrue(actualPathVariables.isEmpty());
  }

  @Test
  public void testGetURI() throws Exception {
    final URI expectedURI = toUri("http://host.domain.com:8080/app/service");
    final Link expectedLink = new Link("test", expectedURI);
    final ClientHttpRequest request = new ClientHttpRequest(expectedLink);

    assertEquals(expectedLink, request.getLink());
    assertEquals(expectedURI, request.getURI());
  }

  @Test
  public void testGetURLForGet() throws Exception {
    final Link expectedLink = new Link("find", toUri("http://host.domain.com:8080/app/libraries/{name}/books"), HttpMethod.GET);
    final ClientHttpRequest request = new ClientHttpRequest(expectedLink);

    request.addParameterValues("author", "Rowling");
    request.addParameterValues("category", "science-fiction");

    assertEquals(expectedLink, request.getLink());
    assertEquals("http://host.domain.com:8080/app/libraries/amazon/books?author=Rowling&category=science-fiction",
      toString(request.getURL(Collections.singletonMap("name", "amazon"))));
  }

  @Test
  public void testGetURLForGetEncoded() throws Exception {
    final Link expectedLink = new Link("readValue4Key", toUri("http://host.domain.com:8080/app/regions/{region}/keys/{key}"), HttpMethod.GET);
    final ClientHttpRequest request = new ClientHttpRequest(expectedLink);

    final Map<String, Object> uriVariables = new HashMap<String, Object>(4);

    uriVariables.put("region", "Customers/Accounts/Orders");
    uriVariables.put("key", "123");
    uriVariables.put("item", "456");

    assertEquals(expectedLink, request.getLink());
    assertEquals("http://host.domain.com:8080/app/regions/Customers%2FAccounts%2FOrders/keys/123",
      toString(request.getURL(uriVariables)));
  }

  @Test
  public void testGetURLForGetWithQueryParametersNoBody() throws Exception {
    final Link expectedLink = new Link("find", toUri("http://host.domain.com:8080/app/libraries/{name}/books/{author}"), HttpMethod.GET);
    final ClientHttpRequest request = new ClientHttpRequest(expectedLink);

    request.addParameterValues("author", "Rowling");
    request.addParameterValues("category", "science-fiction");
    request.addParameterValues("name", "Boston");
    request.addParameterValues("year", "2007");

    final Map<String, Object> uriVariables = new HashMap<String, Object>(4);

    uriVariables.put("author", "Rowling");
    uriVariables.put("category", "mystery");
    uriVariables.put("isbn", "0-123456789");
    uriVariables.put("name", "Amazon");

    assertEquals(expectedLink, request.getLink());
    assertEquals("http://host.domain.com:8080/app/libraries/Amazon/books/Rowling?category=science-fiction&year=2007",
      toString(request.getURL(uriVariables)));
  }

  @Test
  public void testGetURLForDelete() throws Exception {
    final Link expectedLink = new Link("delete-all", toUri("http://host.domain.com:8080/app/libraries/{name}/books"), HttpMethod.DELETE);
    final ClientHttpRequest request = new ClientHttpRequest(expectedLink);

    request.addParameterValues("category", "romance");

    assertEquals(expectedLink, request.getLink());
    assertEquals("http://host.domain.com:8080/app/libraries/congress/books?category=romance",
      toString(request.getURL(Collections.singletonMap("name", "congress"))));
  }

  @Test
  public void testGetURLForPost() throws Exception {
    final Link expectedLink = new Link("post", toUri("http://host.domain.com:8080/app/libraries/{name}/books"), HttpMethod.POST);
    final ClientHttpRequest request = new ClientHttpRequest(expectedLink);

    request.addParameterValues("author", "Douglas Adams");
    request.addParameterValues("title", "The Hitchhiker's Guide to the Galaxy");
    request.addParameterValues("year", "1979");
    request.addParameterValues("isbn", "0345453743");

    assertEquals(expectedLink, request.getLink());
    assertEquals("http://host.domain.com:8080/app/libraries/royal/books",
      toString(request.getURL(Collections.singletonMap("name", "royal"))));
  }

  @Test
  public void testGetURLForPut() throws Exception {
    final Link expectedLink = new Link("put", toUri("http://host.domain.com:8080/app/libraries/{name}/books/{isbn}"), HttpMethod.PUT);
    final ClientHttpRequest request = new ClientHttpRequest(expectedLink);

    request.addParameterValues("year", "1983");

    final Map<String, String> uriVariables = new HashMap<String, String>(2);

    uriVariables.put("name", "royal");
    uriVariables.put("isbn", "0345453743");

    assertEquals(expectedLink, request.getLink());
    assertEquals("http://host.domain.com:8080/app/libraries/royal/books/0345453743",
      toString(request.getURL(uriVariables)));
  }

  @Test
  public void testCreateRequestEntityForGet() throws Exception {
    final Link expectedLink = new Link("find", toUri("http://host.domain.com:8080/app/libraries/{name}/books"), HttpMethod.GET);
    final ClientHttpRequest request = new ClientHttpRequest(expectedLink);

    assertEquals(expectedLink, request.getLink());

    request.addHeaderValues(HttpHeader.CONTENT_TYPE.getName(), MediaType.TEXT_PLAIN_VALUE);
    request.addParameterValues("author", "Rowling");
    request.addParameterValues("category", "science-fiction");

    final HttpEntity<?> requestEntity = request.createRequestEntity();

    assertNotNull(requestEntity);
    assertNotNull(requestEntity.getHeaders());
    assertEquals(MediaType.TEXT_PLAIN, requestEntity.getHeaders().getContentType());
    assertNull(requestEntity.getBody());
  }

  @Test
  public void testCreateRequestEntityForDelete() throws Exception {
    final Link expectedLink = new Link("delete-all", toUri("http://host.domain.com:8080/app/libraries/{name}/books"), HttpMethod.DELETE);
    final ClientHttpRequest request = new ClientHttpRequest(expectedLink);

    assertEquals(expectedLink, request.getLink());

    request.addHeaderValues(HttpHeader.ACCEPT.getName(), MediaType.APPLICATION_JSON_VALUE);
    request.addParameterValues("category", "romance");

    final HttpEntity<?> requestEntity = request.createRequestEntity();

    assertNotNull(requestEntity);
    assertNotNull(requestEntity.getHeaders());
    assertEquals(Collections.singletonList(MediaType.APPLICATION_JSON), requestEntity.getHeaders().getAccept());
    assertNull(requestEntity.getBody());
  }

  @Test
  @SuppressWarnings("unchecked")
  public void testCreateRequestEntityForPost() throws Exception {
    final Link expectedLink = new Link("post", toUri("http://host.domain.com:8080/app/libraries/{name}/books"), HttpMethod.POST);
    final ClientHttpRequest request = new ClientHttpRequest(expectedLink);

    assertEquals(expectedLink, request.getLink());

    final MultiValueMap<String, Object> expectedRequestParameters = new LinkedMultiValueMap<String, Object>(4);

    expectedRequestParameters.add("author", "Douglas Adams");
    expectedRequestParameters.add("title", "The Hitchhiker's Guide to the Galaxy");
    expectedRequestParameters.add("year", "1979");
    expectedRequestParameters.add("isbn", "0345453743");

    request.addHeaderValues(HttpHeader.CONTENT_TYPE.getName(), MediaType.APPLICATION_FORM_URLENCODED_VALUE);
    request.addParameterValues("author", expectedRequestParameters.getFirst("author"));
    request.addParameterValues("title", expectedRequestParameters.getFirst("title"));
    request.addParameterValues("year", expectedRequestParameters.getFirst("year"));
    request.addParameterValues("isbn", expectedRequestParameters.getFirst("isbn"));

    final HttpEntity<MultiValueMap<String, Object>> requestEntity = (HttpEntity<MultiValueMap<String, Object>>)
      request.createRequestEntity();

    assertNotNull(requestEntity);
    assertNotNull(requestEntity.getHeaders());
    assertEquals(MediaType.APPLICATION_FORM_URLENCODED, requestEntity.getHeaders().getContentType());
    assertEquals(expectedRequestParameters, requestEntity.getBody());
  }

  @Test
  @SuppressWarnings("unchecked")
  public void testCreateRequestEntityForPut() throws Exception {
    final Link expectedLink = new Link("put", toUri("http://host.domain.com:8080/app/libraries/{name}/books/{isbn}"), HttpMethod.PUT);
    final ClientHttpRequest request = new ClientHttpRequest(expectedLink);

    assertEquals(expectedLink, request.getLink());

    final MultiValueMap<String, Object> expectedRequestParameters = new LinkedMultiValueMap<String, Object>(4);

    expectedRequestParameters.add("year", "1979");

    request.addHeaderValues(HttpHeader.ACCEPT.getName(), MediaType.TEXT_XML_VALUE);
    request.addHeaderValues(HttpHeader.CONTENT_TYPE.getName(), MediaType.APPLICATION_FORM_URLENCODED_VALUE);
    request.addParameterValues("year", expectedRequestParameters.getFirst("year"));

    final HttpEntity<MultiValueMap<String, Object>> requestEntity = (HttpEntity<MultiValueMap<String, Object>>)
      request.createRequestEntity();

    assertNotNull(requestEntity);
    assertNotNull(requestEntity.getHeaders());
    assertEquals(Collections.singletonList(MediaType.TEXT_XML), requestEntity.getHeaders().getAccept());
    assertEquals(MediaType.APPLICATION_FORM_URLENCODED, requestEntity.getHeaders().getContentType());
    assertEquals(expectedRequestParameters, requestEntity.getBody());
  }

  @Test
  public void testCreateRequestEntityOnPost() throws Exception {
    final Library mockLibrary = mockContext.mock(Library.class, "testCreateRequestEntityOnPost.Library");
    final Link expectedLink = new Link("post", toUri("http://host.domain.com:8080/app/libraries"), HttpMethod.POST);

    final ClientHttpRequest request = new ClientHttpRequest(expectedLink);

    assertEquals(expectedLink, request.getLink());
    assertTrue(request.isPost());
    assertNull(request.getContent());

    request.setContent(mockLibrary);

    assertSame(mockLibrary, request.getContent());

    final HttpEntity<?> requestEntity = request.createRequestEntity();

    assertNotNull(requestEntity);
    assertTrue(requestEntity.getBody() instanceof Library);
  }

  @Test
  public void testCreateRequestEntityOnPut() throws Exception {
    final Book mockBook = mockContext.mock(Book.class, "testCreateRequestEntityOnPut.Book");
    final Link expectedLink = new Link("put", toUri("http://host.domain.com:8080/app/libraries/{name}/books/{id}"), HttpMethod.PUT);

    final ClientHttpRequest request = new ClientHttpRequest(expectedLink);

    assertEquals(expectedLink, request.getLink());
    assertTrue(request.isPut());
    assertNull(request.getContent());

    request.setContent(mockBook);
    request.addParameterValues("isbn", "0-123456789");
    request.addParameterValues("category", "science-fiction", "sci-fi", "fiction");

    assertSame(mockBook, request.getContent());
    assertEquals("0-123456789", request.getParameterValue("isbn"));
    assertTrue(request.getParameterValues("category").containsAll(Arrays.asList("science-fiction", "sci-fi", "fiction")));

    final HttpEntity<?> requestEntity = request.createRequestEntity();

    assertNotNull(requestEntity);
    assertTrue(requestEntity.getBody() instanceof MultiValueMap);
    assertEquals(MediaType.APPLICATION_FORM_URLENCODED, requestEntity.getHeaders().getContentType());
  }

  @Test
  public void testSetAndGetHeaderValues() throws Exception {
    final Link expectedLink = new Link("put", toUri("http://host.domain.com:8080/app/libraries"), HttpMethod.PUT);
    final ClientHttpRequest request = new ClientHttpRequest(expectedLink);

    assertEquals(expectedLink, request.getLink());
    assertTrue(request.getHeaders().isEmpty());

    request.addHeaderValues(HttpHeader.CONTENT_TYPE.getName(), MediaType.APPLICATION_JSON_VALUE);
    request.addHeaderValues(HttpHeader.ACCEPT.getName(), MediaType.APPLICATION_JSON_VALUE,
      MediaType.APPLICATION_XML_VALUE, MediaType.TEXT_PLAIN_VALUE);

    assertEquals(MediaType.APPLICATION_JSON_VALUE, request.getHeaderValue(HttpHeader.CONTENT_TYPE.getName()));
    assertEquals(1, request.getHeaderValues(HttpHeader.CONTENT_TYPE.getName()).size());
    assertEquals(MediaType.APPLICATION_JSON_VALUE, request.getHeaderValue(HttpHeader.ACCEPT.getName()));
    assertEquals(3, request.getHeaderValues(HttpHeader.ACCEPT.getName()).size());
    assertTrue(request.getHeaderValues(HttpHeader.ACCEPT.getName()).containsAll(Arrays.asList(
      MediaType.APPLICATION_JSON_VALUE, MediaType.APPLICATION_XML_VALUE, MediaType.TEXT_PLAIN_VALUE)));

    request.setHeader(HttpHeader.ACCEPT.getName(), MediaType.APPLICATION_OCTET_STREAM_VALUE);

    assertEquals(MediaType.APPLICATION_OCTET_STREAM_VALUE, request.getHeaderValue(HttpHeader.ACCEPT.getName()));
    assertEquals(1, request.getHeaderValues(HttpHeader.ACCEPT.getName()).size());
    assertTrue(request.getHeaderValues(HttpHeader.ACCEPT.getName()).containsAll(Arrays.asList(
      MediaType.APPLICATION_OCTET_STREAM_VALUE)));
  }

  @Test
  public void testSetAndGetParameterValues() throws Exception {
    final Link expectedLink = new Link("put", toUri("http://host.domain.com:8080/app/libraries"), HttpMethod.PUT);
    final ClientHttpRequest request = new ClientHttpRequest(expectedLink);

    assertEquals(expectedLink, request.getLink());
    assertTrue(request.getParameters().isEmpty());

    request.addParameterValues("parameterOne", "value");
    request.addParameterValues("parameterTwo", "test", "testing", "tested");

    assertEquals("value", request.getParameterValue("parameterOne"));
    assertEquals(1, request.getParameterValues("parameterOne").size());
    assertEquals("test", request.getParameterValue("parameterTwo"));
    assertEquals(3, request.getParameterValues("parameterTwo").size());
    assertTrue(request.getParameterValues("parameterTwo").containsAll(Arrays.asList("test", "testing", "tested")));

    request.setParameter("parameterTwo", "development");

    assertEquals("development", request.getParameterValue("parameterTwo"));
    assertEquals(1, request.getParameterValues("parameterTwo").size());
    assertTrue(request.getParameterValues("parameterTwo").containsAll(Arrays.asList("development")));
  }

  @SuppressWarnings("unused")
  private static interface Library {
    public String getName();
  }

  @SuppressWarnings("unused")
  private static interface Book {
    public String getAuthor();
    public String getIsbn();
    public String getTitle();
    public Integer getYear();
  }

}
