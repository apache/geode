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

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.io.File;
import java.io.FileInputStream;
import java.net.URI;
import java.nio.file.Path;
import java.util.Properties;

import org.apache.commons.io.FileUtils;
import org.apache.commons.io.IOUtils;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;
import org.mockito.ArgumentCaptor;
import org.springframework.http.HttpHeaders;
import org.springframework.http.HttpMethod;
import org.springframework.http.HttpStatus;
import org.springframework.http.MediaType;
import org.springframework.http.ResponseEntity;
import org.springframework.http.client.ClientHttpResponse;
import org.springframework.mock.http.client.MockClientHttpResponse;
import org.springframework.web.client.RestTemplate;


public class HttpRequesterTest {
  @Rule
  public TemporaryFolder temporaryFolder = new TemporaryFolder();

  private HttpRequester requester;
  private ClientHttpResponse response;
  private RestTemplate restTemplate;
  private Properties securityProps;
  private URI uri;
  private ResponseEntity<String> responseEntity;

  @Before
  public void setup() {
    uri = URI.create("http://test.org/test");
    restTemplate = mock(RestTemplate.class);
    @SuppressWarnings("unchecked")
    ResponseEntity<String> responseEntity = mock(ResponseEntity.class);
    when(restTemplate.exchange(any(), any(), any(), eq(String.class))).thenReturn(responseEntity);
    when(responseEntity.getBody()).thenReturn("done");

    securityProps = new Properties();
    securityProps.setProperty("user", "me");
    securityProps.setProperty("password", "secret");

  }

  @Test
  public void extractResponseOfJsonString() throws Exception {
    String responseString = "my response";
    requester = new HttpRequester();
    response =
        new MockClientHttpResponse(IOUtils.toInputStream(responseString, "UTF-8"), HttpStatus.OK);

    response.getHeaders().add(HttpHeaders.CONTENT_TYPE, MediaType.APPLICATION_JSON_VALUE);
    Object result = requester.extractResponse(response);
    assertThat(result).isEqualTo(responseString);
  }

  @Test
  public void extractResponseOfFileDownload() throws Exception {
    File responseFile = temporaryFolder.newFile();
    FileUtils.writeStringToFile(responseFile, "some file contents", "UTF-8");
    requester = new HttpRequester();
    response = new MockClientHttpResponse(new FileInputStream(responseFile), HttpStatus.OK);
    response.getHeaders().add(HttpHeaders.CONTENT_TYPE, MediaType.APPLICATION_OCTET_STREAM_VALUE);
    Object result = requester.extractResponse(response);
    Path fileResult = (Path) result;
    assertThat(fileResult).hasSameTextualContentAs(responseFile.toPath());
  }

  @Test
  public void createURI() throws Exception {
    requester = new HttpRequester();
    assertThat(requester.createURI("http://test.org", "abc").toString())
        .isEqualTo("http://test.org/abc");
    assertThat(requester.createURI("http://test.org", "abc", "key", "value").toString())
        .isEqualTo("http://test.org/abc?key=value");


    assertThat(requester.createURI("http://test.org", "abc", "a-b", "c d").toString())
        .isEqualTo("http://test.org/abc?a-b=c%20d");

    assertThatThrownBy(() -> requester.createURI("http://test.org", "abc", "key"))
        .isInstanceOf(IllegalArgumentException.class);
  }

  @Test
  public void get() throws Exception {
    requester = spy(new HttpRequester(securityProps, restTemplate));
    String result = requester.get(uri, String.class);

    assertThat(result).isEqualTo("done");
    verify(requester).exchange(uri, HttpMethod.GET, null, String.class);

    verifyHeaderIsUpdated();
  }

  @Test
  public void post() throws Exception {
    requester = spy(new HttpRequester(securityProps, restTemplate));
    String result =
        requester.post(uri, "myData", String.class);

    assertThat(result).isEqualTo("done");
    verify(requester).exchange(uri, HttpMethod.POST,
        "myData", String.class);

    verifyHeaderIsUpdated();
  }

  private void verifyHeaderIsUpdated() {
    ArgumentCaptor<HttpHeaders> headerCaptor = ArgumentCaptor.forClass(HttpHeaders.class);
    verify(requester).addHeaderValues(headerCaptor.capture());

    HttpHeaders headers = headerCaptor.getValue();
    assertThat(headers.get("user").get(0)).isEqualTo("me");
    assertThat(headers.get("password").get(0)).isEqualTo("secret");
  }
}
