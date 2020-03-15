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
package org.apache.geode.rest.internal.web;

import static org.apache.geode.distributed.ConfigurationProperties.HTTP_SERVICE_BIND_ADDRESS;
import static org.apache.geode.distributed.ConfigurationProperties.HTTP_SERVICE_PORT;
import static org.apache.geode.distributed.ConfigurationProperties.LOG_LEVEL;
import static org.apache.geode.distributed.ConfigurationProperties.MCAST_PORT;
import static org.apache.geode.distributed.ConfigurationProperties.START_DEV_REST_API;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNotSame;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.text.SimpleDateFormat;
import java.util.Calendar;
import java.util.Collections;
import java.util.Date;
import java.util.HashSet;
import java.util.List;
import java.util.Properties;
import java.util.Set;

import javax.annotation.Resource;

import com.fasterxml.jackson.core.JsonParser.Feature;
import com.fasterxml.jackson.databind.DeserializationFeature;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.junit.After;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.http.HttpStatus;
import org.springframework.http.client.ClientHttpResponse;
import org.springframework.http.converter.json.Jackson2ObjectMapperFactoryBean;
import org.springframework.http.converter.json.MappingJackson2HttpMessageConverter;
import org.springframework.util.Assert;
import org.springframework.util.ObjectUtils;
import org.springframework.util.StringUtils;
import org.springframework.web.client.ResponseErrorHandler;
import org.springframework.web.client.RestTemplate;

import org.apache.geode.cache.Cache;
import org.apache.geode.cache.CacheFactory;
import org.apache.geode.cache.DataPolicy;
import org.apache.geode.cache.Region;
import org.apache.geode.cache.RegionFactory;
import org.apache.geode.internal.AvailablePortHelper;
import org.apache.geode.internal.GemFireVersion;
import org.apache.geode.internal.util.IOUtils;
import org.apache.geode.management.internal.AgentUtil;
import org.apache.geode.pdx.PdxInstance;
import org.apache.geode.pdx.PdxReader;
import org.apache.geode.pdx.PdxSerializable;
import org.apache.geode.pdx.PdxWriter;
import org.apache.geode.pdx.ReflectionBasedAutoSerializer;
import org.apache.geode.test.junit.categories.RestAPITest;
import org.apache.geode.test.junit.rules.RequiresGeodeHome;

/**
 * The GemFireRestInterfaceTest class is a test suite of test cases testing the contract and
 * functionality of the GemFire Developer REST API, mixing Java clients, this test GemFire's Cache
 * Region API, along with a REST-based client, also this test using Spring's RestTemplate, testing
 * the proper interaction, especially in the case of an application domain object type having a
 * java.util.Date property.
 *
 * @see org.junit.Test
 * @see org.junit.runner.RunWith
 * @see org.springframework.web.client.RestTemplate
 * @see org.apache.geode.cache.Cache
 * @see org.apache.geode.cache.Region
 * @see org.apache.geode.pdx.PdxInstance
 * @see org.apache.geode.pdx.ReflectionBasedAutoSerializer
 * @since Geode 1.0.0
 */
@Category({RestAPITest.class})
public class RestInterfaceIntegrationTest {

  @Rule
  public RequiresGeodeHome requiresGeodeHome = new RequiresGeodeHome();

  protected static int DEFAULT_HTTP_SERVICE_PORT = 8189;

  protected static final String REST_API_SERVICE_ENDPOINT =
      "http://localhost:%1$d/gemfire-api/v1/%2$s/%3$s";

  @Autowired
  private Cache gemfireCache;

  @Autowired
  private ObjectMapper objectMapper;

  @Resource(name = "gemfireProperties")
  private Properties gemfireProperties;

  @Resource(name = "People")
  private Region<String, Object> people;

  @Before
  public void setupGemFire() {
    AgentUtil agentUtil = new AgentUtil(GemFireVersion.getGemFireVersion());
    if (agentUtil.findWarLocation("geode-web-api") == null) {
      fail("unable to locate geode-web-api WAR file");
    }

    if (gemfireCache == null) {
      gemfireProperties = (gemfireProperties != null ? gemfireProperties : new Properties());

      gemfireCache = new CacheFactory()
          // .setPdxSerializer(new
          // ReflectionBasedAutoSerializer(Person.class.getPackage().getName().concat(".*")))
          .setPdxSerializer(
              new ReflectionBasedAutoSerializer(Person.class.getName().replaceAll("\\$", ".")))
          .setPdxReadSerialized(true).setPdxIgnoreUnreadFields(false)
          .set("name", getClass().getSimpleName()).set(MCAST_PORT, "0").set(LOG_LEVEL, "config")
          .set(HTTP_SERVICE_BIND_ADDRESS, "localhost")
          .set(HTTP_SERVICE_PORT, String.valueOf(getHttpServicePort()))
          .set(START_DEV_REST_API, "true").create();

      RegionFactory<String, Object> peopleRegionFactory = gemfireCache.createRegionFactory();

      peopleRegionFactory.setDataPolicy(DataPolicy.PARTITION);
      peopleRegionFactory.setKeyConstraint(String.class);
      peopleRegionFactory.setValueConstraint(Object.class);

      people = peopleRegionFactory.create("People");
    }
  }

  @After
  public void tearDown() {
    gemfireCache.close();
  }

  protected synchronized int getHttpServicePort() {
    try {
      return Integer
          .parseInt(StringUtils.trimWhitespace(gemfireProperties.getProperty(HTTP_SERVICE_PORT)));
    } catch (NumberFormatException ignore) {
      int httpServicePort = getHttpServicePort(DEFAULT_HTTP_SERVICE_PORT);
      gemfireProperties.setProperty(HTTP_SERVICE_PORT, String.valueOf(httpServicePort));
      return httpServicePort;
    }
  }

  private int getHttpServicePort(final int defaultHttpServicePort) {
    int httpServicePort = AvailablePortHelper.getRandomAvailableTCPPort();
    return (httpServicePort > 1024 && httpServicePort < 65536 ? httpServicePort
        : defaultHttpServicePort);
  }

  protected ObjectMapper getObjectMapper() {
    if (objectMapper == null) {
      Jackson2ObjectMapperFactoryBean objectMapperFactoryBean =
          new Jackson2ObjectMapperFactoryBean();

      objectMapperFactoryBean.setFailOnEmptyBeans(true);
      objectMapperFactoryBean.setFeaturesToEnable(Feature.ALLOW_COMMENTS);
      objectMapperFactoryBean.setFeaturesToEnable(Feature.ALLOW_SINGLE_QUOTES);
      objectMapperFactoryBean
          .setFeaturesToEnable(DeserializationFeature.ACCEPT_EMPTY_STRING_AS_NULL_OBJECT);
      objectMapperFactoryBean
          .setFeaturesToDisable(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES);
      objectMapperFactoryBean.setIndentOutput(true);
      objectMapperFactoryBean.setSimpleDateFormat("MM/dd/yyyy");
      objectMapperFactoryBean.afterPropertiesSet();

      objectMapper = objectMapperFactoryBean.getObject();
    }

    return objectMapper;
  }

  protected Region<String, Object> getPeopleRegion() {
    assertNotNull("The 'People' Region was not properly initialized!", people);
    return people;
  }

  protected String getAdhocQueryRestApiEndpoint(final String query) {
    return getAdhocQueryRestApiEndpoint(getHttpServicePort(), query);
  }

  protected String getAdhocQueryRestApiEndpoint(final int httpServicePort, final String query) {
    return String.format(REST_API_SERVICE_ENDPOINT, httpServicePort, "queries",
        String.format("adhoc?q=%1$s", query));
  }

  protected String getRegionGetRestApiEndpoint(final Region<?, ?> region, final String key) {
    return getRegionGetRestApiEndpoint(getHttpServicePort(), region, key);
  }

  protected String getRegionGetRestApiEndpoint(final int httpServicePort, final Region<?, ?> region,
      final String key) {
    return String.format(REST_API_SERVICE_ENDPOINT, httpServicePort, region.getName(), key);
  }

  protected Date createDate(final int year, final int month, final int dayOfMonth) {
    Calendar dateTime = Calendar.getInstance();
    dateTime.clear();
    dateTime.set(Calendar.YEAR, year);
    dateTime.set(Calendar.MONTH, month);
    dateTime.set(Calendar.DAY_OF_MONTH, dayOfMonth);
    return dateTime.getTime();
  }

  protected Person createPerson(final String firstName, final String lastName,
      final Date birthDate) {
    return new Person(firstName, lastName, birthDate);
  }

  protected RestTemplate createRestTemplate() {
    MappingJackson2HttpMessageConverter httpMessageConverter =
        new MappingJackson2HttpMessageConverter();

    httpMessageConverter.setObjectMapper(getObjectMapper());

    return setErrorHandler(
        new RestTemplate(Collections.singletonList(httpMessageConverter)));
  }

  private RestTemplate setErrorHandler(final RestTemplate restTemplate) {
    restTemplate.setErrorHandler(new ResponseErrorHandler() {
      private final Set<HttpStatus> errorStatuses = new HashSet<>();

      /* non-static */ {
        errorStatuses.add(HttpStatus.BAD_REQUEST);
        errorStatuses.add(HttpStatus.UNAUTHORIZED);
        errorStatuses.add(HttpStatus.FORBIDDEN);
        errorStatuses.add(HttpStatus.NOT_FOUND);
        errorStatuses.add(HttpStatus.METHOD_NOT_ALLOWED);
        errorStatuses.add(HttpStatus.NOT_ACCEPTABLE);
        errorStatuses.add(HttpStatus.REQUEST_TIMEOUT);
        errorStatuses.add(HttpStatus.CONFLICT);
        errorStatuses.add(HttpStatus.PAYLOAD_TOO_LARGE);
        errorStatuses.add(HttpStatus.URI_TOO_LONG);
        errorStatuses.add(HttpStatus.UNSUPPORTED_MEDIA_TYPE);
        errorStatuses.add(HttpStatus.TOO_MANY_REQUESTS);
        errorStatuses.add(HttpStatus.INTERNAL_SERVER_ERROR);
        errorStatuses.add(HttpStatus.NOT_IMPLEMENTED);
        errorStatuses.add(HttpStatus.BAD_GATEWAY);
        errorStatuses.add(HttpStatus.SERVICE_UNAVAILABLE);
      }

      @Override
      public boolean hasError(final ClientHttpResponse response) throws IOException {
        return errorStatuses.contains(response.getStatusCode());
      }

      @Override
      public void handleError(final ClientHttpResponse response) throws IOException {
        System.err.printf("%1$d - %2$s%n", response.getRawStatusCode(), response.getStatusText());
        System.err.println(readBody(response));
      }

      private String readBody(final ClientHttpResponse response) throws IOException {
        BufferedReader responseBodyReader = null;

        try {
          responseBodyReader = new BufferedReader(new InputStreamReader(response.getBody()));

          StringBuilder buffer = new StringBuilder();
          String line;

          while ((line = responseBodyReader.readLine()) != null) {
            buffer.append(line).append(System.getProperty("line.separator"));
          }

          return buffer.toString().trim();
        } finally {
          IOUtils.close(responseBodyReader);
        }
      }
    });

    return restTemplate;
  }

  @Test
  public void testRegionObjectWithDatePropertyAccessedWithRestApi() {
    String key = "1";
    Person jonDoe = createPerson("Jon", "Doe", createDate(1977, Calendar.OCTOBER, 31));

    assertTrue(getPeopleRegion().isEmpty());

    getPeopleRegion().put(key, jonDoe);

    assertFalse(getPeopleRegion().isEmpty());
    assertEquals(1, getPeopleRegion().size());
    assertTrue(getPeopleRegion().containsKey(key));

    Object jonDoeRef = getPeopleRegion().get(key);

    assertTrue(jonDoeRef instanceof PdxInstance);
    assertEquals(jonDoe.getClass().getName(), ((PdxInstance) jonDoeRef).getClassName());
    assertEquals(jonDoe.getFirstName(), ((PdxInstance) jonDoeRef).getField("firstName"));
    assertEquals(jonDoe.getLastName(), ((PdxInstance) jonDoeRef).getField("lastName"));
    assertEquals(jonDoe.getBirthDate(), ((PdxInstance) jonDoeRef).getField("birthDate"));

    RestTemplate restTemplate = createRestTemplate();

    Person jonDoeResource = restTemplate
        .getForObject(getRegionGetRestApiEndpoint(getPeopleRegion(), key), Person.class);

    assertNotNull(jonDoeResource);
    assertNotSame(jonDoe, jonDoeResource);
    assertEquals(jonDoe, jonDoeResource);

    /*
     * Object result = runQueryUsingApi(getPeopleRegion().getRegionService(),
     * String.format("SELECT * FROM %1$s", getPeopleRegion().getFullPath()));
     *
     * System.out.printf("(OQL Query using API) Person is (%1$s)%n", result);
     */

    String url = getAdhocQueryRestApiEndpoint(
        String.format("SELECT * FROM %1$s", getPeopleRegion().getFullPath()));

    System.out.printf("URL (%1$s)%n", url);

    List<?> queryResults = restTemplate.getForObject(url, List.class);

    assertNotNull(queryResults);
    assertFalse(queryResults.isEmpty());
    assertEquals(1, queryResults.size());

    jonDoeResource = objectMapper.convertValue(queryResults.get(0), Person.class);

    assertNotNull(jonDoeResource);
    assertNotSame(jonDoe, jonDoeResource);
    assertEquals(jonDoe, jonDoeResource);
  }

  public static class Person implements PdxSerializable {

    protected static final String DEFAULT_BIRTH_DATE_FORMAT_PATTERN = "MM/dd/yyyy";

    private Date birthDate;

    private String firstName;
    private String lastName;

    @SuppressWarnings("unused")
    public Person() {}

    @SuppressWarnings("unused")
    public Person(final String firstName, final String lastName) {
      this(firstName, lastName, null);
    }

    public Person(final String firstName, final String lastName, final Date birthDate) {
      setFirstName(firstName);
      setLastName(lastName);
      setBirthDate(birthDate);
    }

    public Date getBirthDate() {
      return birthDate;
    }

    public void setBirthDate(final Date birthDate) {
      Assert.isTrue(birthDate == null || birthDate.compareTo(Calendar.getInstance().getTime()) <= 0,
          "A Person's date of birth cannot be after today!");
      this.birthDate = birthDate;
    }

    public String getFirstName() {
      return firstName;
    }

    public void setFirstName(final String firstName) {
      Assert.hasText(firstName, "The Person must have a first name!");
      this.firstName = firstName;
    }

    public String getLastName() {
      return lastName;
    }

    public void setLastName(final String lastName) {
      Assert.hasText(firstName, "The Person must have a last name!");
      this.lastName = lastName;
    }

    protected String format(final Date dateTime) {
      return format(dateTime, DEFAULT_BIRTH_DATE_FORMAT_PATTERN);
    }

    protected String format(final Date dateTime, final String dateFormatPattern) {
      return (dateTime == null ? null
          : new SimpleDateFormat(StringUtils.hasText(dateFormatPattern) ? dateFormatPattern
              : DEFAULT_BIRTH_DATE_FORMAT_PATTERN).format(dateTime));
    }

    @Override
    public void toData(final PdxWriter writer) {
      writer.writeString("firstName", getFirstName());
      writer.writeString("lastName", getLastName());
      writer.writeDate("birthDate", getBirthDate());
    }

    @Override
    public void fromData(final PdxReader reader) {
      setFirstName(reader.readString("firstName"));
      setLastName(reader.readString("lastName"));
      setBirthDate(reader.readDate("birthDate"));
    }

    @Override
    public boolean equals(final Object obj) {
      if (obj == this) {
        return true;
      }

      if (!(obj instanceof Person)) {
        return false;
      }

      Person that = (Person) obj;

      return ObjectUtils.nullSafeEquals(getFirstName(), that.getFirstName())
          && ObjectUtils.nullSafeEquals(getLastName(), that.getLastName())
          && ObjectUtils.nullSafeEquals(getBirthDate(), that.getBirthDate());
    }

    @Override
    public int hashCode() {
      int hashValue = 17;
      hashValue = 37 * hashValue + ObjectUtils.nullSafeHashCode(getFirstName());
      hashValue = 37 * hashValue + ObjectUtils.nullSafeHashCode(getLastName());
      hashValue = 37 * hashValue + ObjectUtils.nullSafeHashCode(getBirthDate());
      return hashValue;
    }

    @Override
    public String toString() {
      return String.format("{ @type = %1$s, firstName = %2$s, lastName = %3$s, birthDate = %4$s }",
          getClass().getName(), getFirstName(), getLastName(), format(getBirthDate()));
    }
  }

}
