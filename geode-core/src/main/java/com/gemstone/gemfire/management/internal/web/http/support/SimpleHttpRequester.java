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
package com.gemstone.gemfire.management.internal.web.http.support;

import java.util.Set;

import org.springframework.http.HttpHeaders;
import org.springframework.http.HttpMethod;
import org.springframework.http.client.SimpleClientHttpRequestFactory;
import org.springframework.web.client.RestTemplate;

/**
 * The SimpleHttpRequester class is a Adapter/facade for the Spring RestTemplate class for abstracting HTTP requests
 * and operations.
 * <p/>
 * @see org.springframework.http.client.SimpleClientHttpRequestFactory
 * @see org.springframework.web.client.RestTemplate
 * @since 8.0
 */
@SuppressWarnings("unused")
public class SimpleHttpRequester {

  protected static final int DEFAULT_CONNECT_TIMEOUT = (30 * 1000); // 30 seconds

  private final RestTemplate restTemplate;

  /**
   * Default constructor to create an instance of the SimpleHttpRequester class using the default connection timeout
   * of 30 seconds.
   */
  public SimpleHttpRequester() {
    this(DEFAULT_CONNECT_TIMEOUT);
  }

  /**
   * Constructs an instance of the SimpleHttpRequester class with the specified connection timeout.
   * <p/>
   * @param connectTimeout an integer value specifying the timeout value in milliseconds for establishing the HTTP
   * connection to the HTTP server.
   */
  public SimpleHttpRequester(final int connectTimeout) {
    final SimpleClientHttpRequestFactory clientHttpRequestFactory = new SimpleClientHttpRequestFactory();

    clientHttpRequestFactory.setConnectTimeout(connectTimeout);

    this.restTemplate = new RestTemplate(clientHttpRequestFactory);
  }

  /**
   * Gets an instance of the Spring RestTemplate to perform the HTTP operations.
   * <p/>
   * @return an instance of the Spring RestTemplate for performing HTTP operations.
   * @see org.springframework.web.client.RestTemplate
   */
  public RestTemplate getRestTemplate() {
    return restTemplate;
  }

  /**
   * Performs an HTTP DELETE operation on the requested resource identified/located by the specified URL.
   * <p/>
   * @param url a String value identifying or locating the resource intended for the HTTP operation.
   * @param urlVariables an array of variables to substitute in the URI/URL template.
   * @see org.springframework.web.client.RestTemplate#delete(String, Object...)
   */
  public void delete(final String url, final Object... urlVariables) {
    getRestTemplate().delete(url, urlVariables);
  }

  /**
   * Performs an HTTP GET operation on the requested resource identified/located by the specified URL.
   * <p/>
   * @param url a String value identifying or locating the resource intended for the HTTP operation.
   * @param urlVariables an array of variables to substitute in the URI/URL template.
   * @see org.springframework.web.client.RestTemplate#getForObject(String, Class, Object...)
   */
  public <T> T get(final String url, final Class<T> responseType, final Object... urlVariables) {
    return getRestTemplate().getForObject(url, responseType, urlVariables);
  }

  /**
   * Retrieves the HTTP HEADERS for the requested resource identified/located by the specified URL.
   * <p/>
   * @param url a String value identifying or locating the resource intended for the HTTP operation.
   * @param urlVariables an array of variables to substitute in the URI/URL template.
   * @see org.springframework.web.client.RestTemplate#headForHeaders(String, Object...)
   */
  public HttpHeaders headers(final String url, final Object... urlVariables) {
    return getRestTemplate().headForHeaders(url, urlVariables);
  }

  /**
   * Request the available/allowed HTTP operations on the resource identified/located by the specified URL.
   * <p/>
   * @param url a String value identifying or locating the resource intended for the HTTP operation.
   * @param urlVariables an array of variables to substitute in the URI/URL template.
   * @see org.springframework.web.client.RestTemplate#optionsForAllow(String, Object...)
   */
  public Set<HttpMethod> options(final String url, final Object... urlVariables) {
    return getRestTemplate().optionsForAllow(url, urlVariables);
  }

  /**
   * Performs an HTTP POST operation on the requested resource identified/located by the specified URL.
   * <p/>
   * @param url a String value identifying or locating the resource intended for the HTTP operation.
   * @param urlVariables an array of variables to substitute in the URI/URL template.
   * @see org.springframework.web.client.RestTemplate#postForObject(String, Object, Class, Object...) z
   */
  public <T> T post(final String url, final Object requestBody, final Class<T> responseType, final Object... urlVariables) {
    return getRestTemplate().postForObject(url, requestBody, responseType, urlVariables);
  }

  /**
   * Performs an HTTP PUT operation on the requested resource identified/located by the specified URL.
   * <p/>
   * @param url a String value identifying or locating the resource intended for the HTTP operation.
   * @param urlVariables an array of variables to substitute in the URI/URL template.
   * @see org.springframework.web.client.RestTemplate#put(String, Object, Object...)
   */
  public void put(final String url, final Object requestBody, final Object... urlVariables) {
    getRestTemplate().put(url, requestBody, urlVariables);
  }

}
