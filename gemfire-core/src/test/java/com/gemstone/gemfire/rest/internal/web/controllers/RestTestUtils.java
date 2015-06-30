/*=========================================================================
 * Copyright (c) 2010-2014 Pivotal Software, Inc. All Rights Reserved.
 * This product is protected by U.S. and international copyright
 * and intellectual property laws. Pivotal products are covered by
 * one or more patents listed at http://www.pivotal.io/patents.
 *=========================================================================
 */
package com.gemstone.gemfire.rest.internal.web.controllers;

import java.net.URI;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.List;
import org.springframework.http.converter.ByteArrayHttpMessageConverter;
import org.springframework.http.converter.HttpMessageConverter;
import org.springframework.http.converter.ResourceHttpMessageConverter;
import org.springframework.http.converter.StringHttpMessageConverter;
import org.springframework.http.converter.json.Jackson2ObjectMapperFactoryBean;
import org.springframework.http.converter.json.MappingJackson2HttpMessageConverter;
import org.springframework.web.client.RestTemplate;
import org.springframework.web.util.UriComponentsBuilder;


/**
 * The RestTestUtils class contains core functionality for Spring REST Template
 * <p/>
 * 
 * @author Nilkanth Patel
 * @see org.springframework.context.ApplicationContext
 * @see org.springframework.data.gemfire.GemfireTemplate
 * @since 8.0
 */
public class RestTestUtils  {

  public static final String BASE_URL = "http://localhost:8080";
  public static final String GEMFIRE_REST_API_CONTEXT = "/gemfire-api";
  public static final String GEMFIRE_REST_API_VERSION = "/v1";

  public static final URI GEMFIRE_REST_API_WEB_SERVICE_URL = URI
      .create(BASE_URL + GEMFIRE_REST_API_CONTEXT + GEMFIRE_REST_API_VERSION);

  private static RestTemplate restTemplate;

  public static RestTemplate getRestTemplate() {
    if (restTemplate == null) {
      restTemplate = new RestTemplate();

      final List<HttpMessageConverter<?>> messageConverters = new ArrayList<HttpMessageConverter<?>>();

      messageConverters.add(new ByteArrayHttpMessageConverter());
      messageConverters.add(new ResourceHttpMessageConverter());
      messageConverters.add(new StringHttpMessageConverter());
      messageConverters.add(createMappingJackson2HttpMessageConverter());
      
      restTemplate.setMessageConverters(messageConverters);
    }
    return restTemplate;
  }

  public static HttpMessageConverter<Object> createMappingJackson2HttpMessageConverter() {
    final Jackson2ObjectMapperFactoryBean objectMapperFactoryBean = new Jackson2ObjectMapperFactoryBean();

    objectMapperFactoryBean.setFailOnEmptyBeans(true);
    objectMapperFactoryBean.setIndentOutput(true);
    objectMapperFactoryBean.setDateFormat(new SimpleDateFormat("MM/dd/yyyy"));
    objectMapperFactoryBean
        .setFeaturesToDisable(com.fasterxml.jackson.databind.DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES);
    objectMapperFactoryBean
        .setFeaturesToEnable(
            com.fasterxml.jackson.core.JsonParser.Feature.ALLOW_COMMENTS,
            com.fasterxml.jackson.core.JsonParser.Feature.ALLOW_SINGLE_QUOTES,
            com.fasterxml.jackson.databind.DeserializationFeature.ACCEPT_EMPTY_STRING_AS_NULL_OBJECT);
    objectMapperFactoryBean.afterPropertiesSet();

    final MappingJackson2HttpMessageConverter httpMessageConverter = new MappingJackson2HttpMessageConverter();
    httpMessageConverter.setObjectMapper(objectMapperFactoryBean.getObject());
    return httpMessageConverter;
  }
  
  /*
  protected static HttpMessageConverter<Object> createMarshallingHttpMessageConverter() {
    final Jaxb2Marshaller jaxbMarshaller = new Jaxb2Marshaller();

    jaxbMarshaller.setContextPaths("com.gemstone.gemfire.web.rest.domain",
        "com.gemstone.gemfire.web.controllers.support");
    jaxbMarshaller.setMarshallerProperties(Collections.singletonMap(
        "jaxb.formatted.output", Boolean.TRUE));

    return new MarshallingHttpMessageConverter(jaxbMarshaller);
  }
  */
  
  public static URI toUri(final String... pathSegments) {
    return toUri(GEMFIRE_REST_API_WEB_SERVICE_URL, pathSegments);
  }

  public static URI toUri(final URI baseUrl, final String... pathSegments) {
    return UriComponentsBuilder.fromUri(baseUrl).pathSegment(pathSegments)
        .build().toUri();
  }
}
