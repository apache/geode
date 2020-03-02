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

package org.apache.geode.management.internal.web.http.support;

import java.io.IOException;
import java.net.URI;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Arrays;
import java.util.List;
import java.util.Properties;

import org.apache.commons.io.FileUtils;
import org.apache.commons.io.IOUtils;
import org.springframework.http.HttpEntity;
import org.springframework.http.HttpHeaders;
import org.springframework.http.HttpMethod;
import org.springframework.http.MediaType;
import org.springframework.http.ResponseEntity;
import org.springframework.http.client.ClientHttpRequest;
import org.springframework.http.client.ClientHttpResponse;
import org.springframework.http.client.SimpleClientHttpRequestFactory;
import org.springframework.http.converter.HttpMessageConverter;
import org.springframework.http.converter.json.MappingJackson2HttpMessageConverter;
import org.springframework.web.client.DefaultResponseErrorHandler;
import org.springframework.web.client.RestTemplate;
import org.springframework.web.util.UriComponentsBuilder;

import org.apache.geode.internal.GemFireVersion;
import org.apache.geode.management.internal.web.http.converter.SerializableObjectHttpMessageConverter;
import org.apache.geode.security.AuthenticationFailedException;
import org.apache.geode.security.NotAuthorizedException;


/**
 * The HttpRequester class is a Adapter/facade for the Spring RestTemplate class for abstracting
 * HTTP requests and operations.
 * <p/>
 *
 * @see org.springframework.http.client.SimpleClientHttpRequestFactory
 * @see org.springframework.web.client.RestTemplate
 * @since GemFire 8.0
 */
@SuppressWarnings("unused")
public class HttpRequester {
  private final RestTemplate restTemplate;
  private Properties securityProperties;

  protected static final String USER_AGENT_HTTP_REQUEST_HEADER_VALUE =
      "GemFire-Shell/v" + GemFireVersion.getGemFireVersion();

  // a list of acceptable content/media types supported by Gfsh
  private final List<MediaType> acceptableMediaTypes = Arrays.asList(MediaType.APPLICATION_JSON,
      MediaType.TEXT_PLAIN, MediaType.APPLICATION_OCTET_STREAM);

  public HttpRequester() {
    this(null, null);
  }

  public HttpRequester(Properties securityProperties) {
    this(securityProperties, null);
  }

  HttpRequester(Properties securityProperties, RestTemplate restTemplate) {
    final SimpleClientHttpRequestFactory clientHttpRequestFactory =
        new SimpleClientHttpRequestFactory();
    this.securityProperties = securityProperties;
    if (restTemplate == null) {
      this.restTemplate = new RestTemplate(clientHttpRequestFactory);
    } else {
      this.restTemplate = restTemplate;
    }

    // add our custom HttpMessageConverter for serializing DTO Objects into the HTTP request message
    // body and de-serializing HTTP response message body content back into DTO Objects
    List<HttpMessageConverter<?>> converters = this.restTemplate.getMessageConverters();
    // remove the MappingJacksonHttpConverter
    for (int i = converters.size() - 1; i >= 0; i--) {
      HttpMessageConverter converter = converters.get(i);
      if (converter instanceof MappingJackson2HttpMessageConverter) {
        converters.remove(converter);
      }
    }
    converters.add(new SerializableObjectHttpMessageConverter());

    this.restTemplate.setErrorHandler(new DefaultResponseErrorHandler() {
      @Override
      public void handleError(final ClientHttpResponse response) throws IOException {
        String body = IOUtils.toString(response.getBody(), StandardCharsets.UTF_8);
        final String message = String.format("The HTTP request failed with: %1$d - %2$s.",
            response.getRawStatusCode(), body);

        if (response.getRawStatusCode() == 401) {
          throw new AuthenticationFailedException(message);
        } else if (response.getRawStatusCode() == 403) {
          throw new NotAuthorizedException(message);
        } else {
          throw new RuntimeException(message);
        }
      }
    });
  }

  /**
   * Gets an instance of the Spring RestTemplate to perform the HTTP operations.
   * <p/>
   *
   * @return an instance of the Spring RestTemplate for performing HTTP operations.
   * @see org.springframework.web.client.RestTemplate
   */
  public RestTemplate getRestTemplate() {
    return restTemplate;
  }

  public <T> T get(URI url, Class<T> responseType) {
    return exchange(url, HttpMethod.GET, null, responseType);
  }

  public <T> T post(URI url, Object content, Class<T> responseType) {
    return exchange(url, HttpMethod.POST, content, responseType);
  }

  <T> T exchange(URI url, HttpMethod method, Object content,
      Class<T> responseType) {
    HttpHeaders headers = new HttpHeaders();
    addHeaderValues(headers);

    HttpEntity<Object> httpEntity = new HttpEntity<>(content, headers);

    final ResponseEntity<T> response = restTemplate.exchange(url, method, httpEntity, responseType);
    return response.getBody();
  }

  /**
   * @return either a json representation of ResultModel or a Path
   */
  public Object executeWithResponseExtractor(URI url) {
    return restTemplate.execute(url, HttpMethod.POST, this::addHeaderValues, this::extractResponse);
  }

  void addHeaderValues(ClientHttpRequest request) {
    addHeaderValues(request.getHeaders());
  }

  /**
   * @return either a json representation of ResultModel or a Path
   */
  Object extractResponse(ClientHttpResponse response) throws IOException {
    MediaType mediaType = response.getHeaders().getContentType();
    if (mediaType.equals(MediaType.APPLICATION_JSON)) {
      return org.apache.commons.io.IOUtils.toString(response.getBody(), "UTF-8");
    } else {
      Path tempFile = Files.createTempFile("fileDownload", "");
      if (tempFile.toFile().exists()) {
        FileUtils.deleteQuietly(tempFile.toFile());
      }
      Files.copy(response.getBody(), tempFile);
      return tempFile;
    }
  }

  void addHeaderValues(HttpHeaders headers) {
    // update the headers
    headers.add(HttpHeaders.USER_AGENT, USER_AGENT_HTTP_REQUEST_HEADER_VALUE);
    headers.setAccept(acceptableMediaTypes);

    if (this.securityProperties != null) {
      for (String key : securityProperties.stringPropertyNames()) {
        headers.add(key, securityProperties.getProperty(key));
      }
    }
  }

  /**
   * build the url using the path and query params
   *
   * @param path : the part after the baseUrl
   * @param queryParams this needs to be an even number of strings in the form of paramName,
   *        paramValue....
   */
  public static URI createURI(String baseUrl, String path, String... queryParams) {
    UriComponentsBuilder builder = UriComponentsBuilder.fromHttpUrl(baseUrl).path(path);

    if (queryParams != null) {
      if (queryParams.length % 2 != 0) {
        throw new IllegalArgumentException("invalid queryParams count");
      }
      for (int i = 0; i < queryParams.length; i += 2) {
        builder.queryParam(queryParams[i], queryParams[i + 1]);
      }
    }
    return builder.build().encode().toUri();
  }
}
