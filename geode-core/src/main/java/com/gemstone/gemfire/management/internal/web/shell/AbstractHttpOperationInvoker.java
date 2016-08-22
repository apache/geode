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

package com.gemstone.gemfire.management.internal.web.shell;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.net.URI;
import java.util.Arrays;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import javax.management.ObjectName;
import javax.management.QueryExp;

import com.gemstone.gemfire.internal.GemFireVersion;
import com.gemstone.gemfire.internal.lang.StringUtils;
import com.gemstone.gemfire.internal.logging.LogService;
import com.gemstone.gemfire.internal.util.IOUtils;
import com.gemstone.gemfire.management.DistributedSystemMXBean;
import com.gemstone.gemfire.management.internal.MBeanJMXAdapter;
import com.gemstone.gemfire.management.internal.ManagementConstants;
import com.gemstone.gemfire.management.internal.cli.shell.Gfsh;
import com.gemstone.gemfire.management.internal.web.domain.Link;
import com.gemstone.gemfire.management.internal.web.domain.QueryParameterSource;
import com.gemstone.gemfire.management.internal.web.http.ClientHttpRequest;
import com.gemstone.gemfire.management.internal.web.http.HttpHeader;
import com.gemstone.gemfire.management.internal.web.http.HttpMethod;
import com.gemstone.gemfire.management.internal.web.http.converter.SerializableObjectHttpMessageConverter;
import com.gemstone.gemfire.management.internal.web.shell.support.HttpMBeanProxyFactory;
import com.gemstone.gemfire.management.internal.web.util.UriUtils;
import com.gemstone.gemfire.security.AuthenticationFailedException;
import com.gemstone.gemfire.security.NotAuthorizedException;

import org.apache.logging.log4j.Logger;
import org.springframework.http.HttpStatus;
import org.springframework.http.MediaType;
import org.springframework.http.ResponseEntity;
import org.springframework.http.client.ClientHttpResponse;
import org.springframework.http.client.SimpleClientHttpRequestFactory;
import org.springframework.web.client.ResourceAccessException;
import org.springframework.web.client.ResponseErrorHandler;
import org.springframework.web.client.RestTemplate;

/**
 * The AbstractHttpOperationInvoker class is an abstract base class encapsulating common functionality for all
 * HTTP-based OperationInvoker implementations.
 * @see com.gemstone.gemfire.management.internal.cli.shell.Gfsh
 * @see com.gemstone.gemfire.management.internal.cli.shell.OperationInvoker
 * @see com.gemstone.gemfire.management.internal.web.shell.HttpOperationInvoker
 * @see com.gemstone.gemfire.management.internal.web.shell.RestHttpOperationInvoker
 * @see com.gemstone.gemfire.management.internal.web.shell.SimpleHttpOperationInvoker
 * @see org.springframework.http.client.SimpleClientHttpRequestFactory
 * @see org.springframework.web.client.RestTemplate
 * @since GemFire 8.0
 */
@SuppressWarnings("unused")
public abstract class AbstractHttpOperationInvoker implements HttpOperationInvoker {

  protected static final long DEFAULT_INITIAL_DELAY = TimeUnit.SECONDS.toMillis(1);
  protected static final long DEFAULT_PERIOD = TimeUnit.MILLISECONDS.toMillis(500);

  protected static final String MBEAN_ATTRIBUTE_LINK_RELATION = "mbean-attribute";
  protected static final String MBEAN_OPERATION_LINK_RELATION = "mbean-operation";
  protected static final String MBEAN_QUERY_LINK_RELATION = "mbean-query";
  protected static final String PING_LINK_RELATION = "ping";
  protected static final String DEFAULT_ENCODING = UriUtils.DEFAULT_ENCODING;
  protected static final String REST_API_BASE_URL = "http://localhost:8080";
  protected static final String REST_API_VERSION = "/v1";
  protected static final String REST_API_WEB_APP_CONTEXT = "/gemfire";
  protected static final String REST_API_URL = REST_API_BASE_URL + REST_API_WEB_APP_CONTEXT + REST_API_VERSION;
  protected static final String USER_AGENT_HTTP_REQUEST_HEADER_VALUE = "GemFire-Shell/v" + GemFireVersion.getGemFireVersion();

  protected static final TimeUnit DEFAULT_TIME_UNIT = TimeUnit.MILLISECONDS;

  // the ID of the GemFire distributed system (cluster)
  private Integer clusterId = CLUSTER_ID_WHEN_NOT_CONNECTED;

  // Executor for scheduling periodic Runnable task to assess the state of the Manager's HTTP service or Web Service
  // hosting the M&M REST API (interface)
  private final ScheduledExecutorService executorService;

  // a reference to the GemFire shell (Gfsh) instance using this HTTP-based OperationInvoker for command execution
  // and processing
  private final Gfsh gfsh;

  // a list of acceptable content/media types supported by Gfsh
  private final List<MediaType> acceptableMediaTypes = Arrays.asList(MediaType.APPLICATION_JSON, MediaType.TEXT_PLAIN);

  // a Java Logger used to log severe, warning, informational and debug messages during the operation of this invoker
  private final Logger logger = LogService.getLogger();

  // the Spring RestTemplate used to send HTTP requests and make REST API calls
  private volatile RestTemplate restTemplate;

  // the base URL of the GemFire Manager's embedded HTTP service and REST API interface
  private final String baseUrl;


  protected Map<String, String> securityProperties;

  /**
   * Default, public, no-arg constructor to create an instance of the AbstractHttpOperationInvoker class
   * for testing purposes.
   */
  AbstractHttpOperationInvoker(final String baseUrl) {
    this.baseUrl = baseUrl;
    this.executorService = null;
    this.gfsh = null;
    this.restTemplate = null;
  }

  /**
   * Constructs an instance of the AbstractHttpOperationInvoker class with a reference to the GemFire shell (Gfsh)
   * instance using this HTTP-based OperationInvoker to send commands to the GemFire Manager via HTTP for processing.
   * @param gfsh a reference to the instance of the GemFire shell (Gfsh) using this HTTP-based OperationInvoker for
   * command processing.
   * @throws AssertionError if the reference to the Gfsh instance is null.
   * @see #AbstractHttpOperationInvoker(com.gemstone.gemfire.management.internal.cli.shell.Gfsh, String, Map)
   * @see com.gemstone.gemfire.management.internal.cli.shell.Gfsh
   */
  public AbstractHttpOperationInvoker(final Gfsh gfsh, Map<String, String> securityProperties) {
    this(gfsh, REST_API_URL, securityProperties);
  }

  /**
   * Constructs an instance of the AbstractHttpOperationInvoker class with a reference to the GemFire shell (Gfsh)
   * instance using this HTTP-based OperationInvoker to send commands to the GemFire Manager via HTTP for procsessing
   * along with the base URL to the GemFire Manager's embedded HTTP service hosting the HTTP (REST) interface.
   * @param gfsh a reference to the instance of the GemFire shell (Gfsh) using this HTTP-based OperationInvoker for
   * command processing.
   * @param baseUrl a String specifying the base URL to the GemFire Manager's embedded HTTP service hosting the REST
   * interface.
   * @throws AssertionError if the reference to the Gfsh instance is null.
   * @see com.gemstone.gemfire.management.internal.cli.shell.Gfsh
   */
  public AbstractHttpOperationInvoker(final Gfsh gfsh, final String baseUrl, Map<String, String> securityProperties) {
    assertNotNull(gfsh, "The reference to the GemFire shell (Gfsh) cannot be null!");

    this.gfsh = gfsh;
    this.baseUrl = StringUtils.defaultIfBlank(baseUrl, REST_API_URL);
    this.securityProperties = securityProperties;

    // constructs an instance of a single-threaded, scheduled Executor to send periodic HTTP requests to the Manager's
    // HTTP service or Web Service to assess the "alive" state
    this.executorService = Executors.newSingleThreadScheduledExecutor();

    // constructs an instance of the Spring RestTemplate for M&M REST API (interface) operations
    this.restTemplate = new RestTemplate(new SimpleClientHttpRequestFactory());

    // add our custom HttpMessageConverter for serializing DTO Objects into the HTTP request message body
    // and de-serializing HTTP response message body content back into DTO Objects
    this.restTemplate.getMessageConverters().add(new SerializableObjectHttpMessageConverter());

    // set the ResponseErrorHandler handling any errors originating from our HTTP request
    this.restTemplate.setErrorHandler(new ResponseErrorHandler() {
      @Override
      public boolean hasError(final ClientHttpResponse response) throws IOException {
        final HttpStatus status = response.getStatusCode();

        switch (status) {
          case BAD_REQUEST: // 400 *
          case UNAUTHORIZED: // 401
          case FORBIDDEN: // 403
          case NOT_FOUND: // 404 *
          case METHOD_NOT_ALLOWED: // 405 *
          case NOT_ACCEPTABLE: // 406 *
          case REQUEST_TIMEOUT: // 408
          case CONFLICT: // 409
          case REQUEST_ENTITY_TOO_LARGE: // 413
          case REQUEST_URI_TOO_LONG: // 414
          case UNSUPPORTED_MEDIA_TYPE: // 415 *
          case TOO_MANY_REQUESTS: // 429
          case INTERNAL_SERVER_ERROR: // 500 *
          case NOT_IMPLEMENTED: // 501
          case BAD_GATEWAY: // 502 ?
          case SERVICE_UNAVAILABLE: // 503
            return true;
          default:
            return false;
        }
      }

      @Override
      public void handleError(final ClientHttpResponse response) throws IOException {
        String body = readBody(response);
        final String message = String.format("The HTTP request failed with: %1$d - %2$s.", response.getRawStatusCode(), body);

        if (gfsh.getDebug()) {
          gfsh.logSevere(body, null);
        }

        if (response.getRawStatusCode() == 401) {
          throw new AuthenticationFailedException(message);
        }
        else if (response.getRawStatusCode() == 403) {
          throw new NotAuthorizedException(message);
        }
        else {
          throw new RuntimeException(message);
        }
      }

      private String readBody(final ClientHttpResponse response) throws IOException {
        BufferedReader responseBodyReader = null;

        try {
          responseBodyReader = new BufferedReader(new InputStreamReader(response.getBody()));

          final StringBuilder buffer = new StringBuilder();
          String line;

          while ((line = responseBodyReader.readLine()) != null) {
            buffer.append(line).append(StringUtils.LINE_SEPARATOR);
          }

          return buffer.toString().trim();
        }
        finally {
          IOUtils.close(responseBodyReader);
        }
      }
    });
  }

  /**
   * Asserts the argument is valid, as determined by the caller passing the result of an evaluated expression to this
   * assertion.
   * @param validArg a boolean value indicating the evaluation of the expression validating the argument.
   * @param message a String value used as the message when constructing an IllegalArgumentException.
   * @param args Object arguments used to populate placeholder's in the message.
   * @throws IllegalArgumentException if the argument is not valid.
   * @see java.lang.String#format(String, Object...)
   */
  protected static void assertArgument(final boolean validArg, final String message, final Object... args) {
    if (!validArg) {
      throw new IllegalArgumentException(String.format(message, args));
    }
  }

  /**
   * Asserts the Object reference is not null!
   * @param obj the reference to the Object.
   * @param message the String value used as the message when constructing and throwing a NullPointerException.
   * @param args Object arguments used to populate placeholder's in the message.
   * @throws NullPointerException if the Object reference is null.
   * @see java.lang.String#format(String, Object...)
   */
  protected static void assertNotNull(final Object obj, final String message, final Object... args) {
    if (obj == null) {
      throw new NullPointerException(String.format(message, args));
    }
  }

  /**
   * Asserts whether state, based on the evaluation of a conditional expression, passed to this assertion is valid.
   * @param validState a boolean value indicating the evaluation of the expression from which the conditional state
   * is based.  For example, a caller might use an expression of the form (initableObj.isInitialized()).
   * @param message a String values used as the message when constructing an IllegalStateException.
   * @param args Object arguments used to populate placeholder's in the message.
   * @throws IllegalStateException if the conditional state is not valid.
   * @see java.lang.String#format(String, Object...)
   */
  protected static void assertState(final boolean validState, final String message, final Object... args) {
    if (!validState) {
      throw new IllegalStateException(String.format(message, args));
    }
  }

  /**
   * Gets a list of acceptable content/media types supported by Gfsh.
   * @return a List of acceptable content/media types supported by Gfsh.
   * @see org.springframework.http.MediaType
   */
  protected List<MediaType> getAcceptableMediaTypes() {
    return acceptableMediaTypes;
  }

  /**
   * Returns the base URL to GemFire's REST interface hosted in the GemFire Manager's embedded HTTP service
   * (Tomcat server).
   * @return a String value specifying the base URL to the GemFire REST interface.
   */
  protected String getBaseUrl() {
    return this.baseUrl;
  }

  /**
   * Determines whether Gfsh is in debug mode (or whether the user enabled debugging in Gfsh).
   * @return a boolean value indicating if debugging has been turned on in Gfsh.
   * @see com.gemstone.gemfire.management.internal.cli.shell.Gfsh#getDebug()
   */
  protected boolean isDebugEnabled() {
    return getGfsh().getDebug();
  }

  /**
   * Gets the ExecutorService used by this HTTP OperationInvoker to scheduled periodic or delayed tasks.
   * @return an instance of the ScheduledExecutorService for scheduling periodic or delayed tasks.
   * @see java.util.concurrent.ScheduledExecutorService
   */
  protected final ScheduledExecutorService getExecutorService() {
    assertState(this.executorService != null,
      "The ExecutorService for this HTTP OperationInvoker (%1$s) was not properly initialized!",
      getClass().getName());
    return this.executorService;
  }

  /**
   * Returns the reference to the GemFire shell (Gfsh) instance using this HTTP-based OperationInvoker to send commands
   * to the GemFire Manager for remote execution and processing.
   * @return a reference to the instance of the GemFire shell (Gfsh) using this HTTP-based OperationInvoker to process
   * commands.
   * @see com.gemstone.gemfire.management.internal.cli.shell.Gfsh
   */
  protected final Gfsh getGfsh() {
    return this.gfsh;
  }

  /**
   * Returns a reference to the Spring RestTemplate used by this HTTP-based OperationInvoker to send HTTP requests to
   * GemFire's REST interface, making REST API calls.
   * @return an instance of the Spring RestTemplate used to make REST API web service calls.
   * @see org.springframework.web.client.RestTemplate
   */
  protected final RestTemplate getRestTemplate() {
    return this.restTemplate;
  }

  /**
   * Creates an instance of a client HTTP request with the specified Link targeting the resource as well as the intended
   * operation on the resource.
   * @param link a Link with the URI targeting and identifying the resource as well as the method of operation on the
   * resource.
   * @return a client HTTP request with the details of the request.
   * @see com.gemstone.gemfire.management.internal.web.http.ClientHttpRequest
   * @see com.gemstone.gemfire.management.internal.web.domain.Link
   */
  protected ClientHttpRequest createHttpRequest(final Link link) {
    final ClientHttpRequest request = new ClientHttpRequest(link);
    request.addHeaderValues(HttpHeader.USER_AGENT.getName(), USER_AGENT_HTTP_REQUEST_HEADER_VALUE);
    request.getHeaders().setAccept(getAcceptableMediaTypes());

    if (this.securityProperties != null) {
      Iterator<Entry<String, String>> it = this.securityProperties.entrySet().iterator();
      while (it.hasNext()) {
        Entry<String, String> entry = it.next();
        request.addHeaderValues(entry.getKey(), entry.getValue());
      }
    }
    return request;
  }

  /**
   * Creates a Link with the specified relation and URI of the remote resource.
   * @param relation a String indicating the link relation, or relative state transition, operation.
   * @param href the URI identifying the resource and it's location.
   * @return a Link with the providing relation and URI.
   * @see com.gemstone.gemfire.management.internal.web.domain.Link
   * @see java.net.URI
   */
  protected Link createLink(final String relation, final URI href) {
    return new Link(relation, href);
  }

  /**
   * Creates a Link with the specified relation and URI of the remote resource along with the method of the operation.
   * @param relation a String indicating the link relation, or relative state transition, operation.
   * @param href the URI identifying the resource and it's location.
   * @param method the HTTP method for the operation of the request.
   * @return a Link with the providing relation and URI.
   * @see com.gemstone.gemfire.management.internal.web.http.HttpMethod
   * @see com.gemstone.gemfire.management.internal.web.domain.Link
   * @see java.net.URI
   */
  protected Link createLink(final String relation, final URI href, final HttpMethod method) {
    return new Link(relation, href, method);
  }

  /**
   * Decodes the encoded String value using the default encoding UTF-8.  It is assumed the String value was encoded
   * with the URLEncoder using the UTF-8 encoding.  This method handles UnsupportedEncodingException by just returning
   * the encodedValue.
   * @param encodedValue the encoded String value to decode.
   * @return the decoded value of the String or encodedValue if the UTF-8 encoding is unsupported.
   * @see com.gemstone.gemfire.management.internal.web.util.UriUtils#decode(String)
   */
  protected String decode(final String encodedValue) {
    return UriUtils.decode(encodedValue);
  }

  /**
   * Decodes the encoded String value using the specified encoding (such as UTF-8).  It is assumed the String value
   * was encoded with the URLEncoder using the specified encoding.  This method handles UnsupportedEncodingException
   * by just returning the encodedValue.
   * @param encodedValue a String value encoded in the encoding.
   * @param encoding a String value specifying the encoding.
   * @return the decoded value of the String or encodedValue if the specified encoding is unsupported.
   * @see com.gemstone.gemfire.management.internal.web.util.UriUtils#decode(String, String)
   */
  protected String decode(final String encodedValue, String encoding) {
    return UriUtils.decode(encodedValue, encoding);
  }

  /**
   * Encode the String value using the default encoding UTF-8.
   * @param value the String value to encode.
   * @return an encoded value of the String using the default encoding UTF-8 or value if the UTF-8 encoding
   * is unsupported.
   * @see com.gemstone.gemfire.management.internal.web.util.UriUtils#encode(String)
   */
  protected String encode(final String value) {
    return UriUtils.encode(value);
  }

  /**
   * Encode the String value using the specified encoding (such as UTF-8).
   * @param value the String value to encode.
   * @param encoding a String value indicating the encoding.
   * @return an encoded value of the String using the specified encoding or value if the specified encoding
   * is unsupported.
   * @see com.gemstone.gemfire.management.internal.web.util.UriUtils#encode(String, String)
   */
  protected String encode(final String value, final String encoding) {
    return UriUtils.encode(value, encoding);
  }

  /**
   * Finds a Link containing the HTTP request URI for the relational operation (state transition) on the resource.
   * @param relation a String describing the relational operation, or state transition on the resource.
   * @return an instance of Link containing the HTTP request URI used to perform the intended operation on the resource.
   * @see com.gemstone.gemfire.management.internal.web.domain.Link
   */
  protected Link findLink(final String relation) {
    return null;
  }

  /**
   * Handles resource access errors such as ConnectExceptions when the server-side process/service is not listening
   * for client connections, or the connection to the server/service fails.
   * @param e the ResourceAccessException resulting in some sort of I/O error.
   * @return a user-friendly String message describing the problem and appropriate action/response by the user.
   * @see #stop()
   * @see org.springframework.web.client.ResourceAccessException
   */
  protected String handleResourceAccessException(final ResourceAccessException e) {
    stop();

    return String.format("The connection to the GemFire Manager's HTTP service @ %1$s failed with: %2$s. "
        + "Please try reconnecting or see the GemFire Manager's log file for further details.",
      getBaseUrl(), e.getMessage());
  }

  /**
   * Displays the message inside GemFire shell at debug level.
   * @param message the String containing the message to display inside Gfsh.
   * @see #isDebugEnabled()
   * @see #printInfo(String, Object...)
   */
  protected void printDebug(final String message, final Object... args) {
    if (isDebugEnabled()) {
      printInfo(message, args);
    }
  }

  /**
   * Displays the message inside GemFire shell at info level.
   * @param message the String containing the message to display inside Gfsh.
   * @see com.gemstone.gemfire.management.internal.cli.shell.Gfsh#printAsInfo(String)
   */
  protected void printInfo(final String message, final Object... args) {
    getGfsh().printAsInfo(String.format(message, args));
  }

  /**
   * Displays the message inside GemFire shell at warning level.
   * @param message the String containing the message to display inside Gfsh.
   * @see com.gemstone.gemfire.management.internal.cli.shell.Gfsh#printAsWarning(String)
   */
  protected void printWarning(final String message, final Object... args) {
    getGfsh().printAsWarning(String.format(message, args));
  }

  /**
   * Displays the message inside GemFire shell at severe level.
   * @param message the String containing the message to display inside Gfsh.
   * @see com.gemstone.gemfire.management.internal.cli.shell.Gfsh#printAsSevere(String)
   */
  protected void printSevere(final String message, final Object... args) {
    getGfsh().printAsSevere(String.format(message, args));
  }

  /**
   * Sends the HTTP request, using Spring's RestTemplate, to the GemFire REST API web service endpoint, expecting the
   * specified response type from the server in return.
   * @param <T> the response type.
   * @param request the client HTTP request to send.
   * @param responseType the expected Class type of the return value in the server's response.
   * @return a ResponseEntity encapsulating the details of the server's response to the client's HTTP request.
   * @see #send(com.gemstone.gemfire.management.internal.web.http.ClientHttpRequest, Class, java.util.Map)
   * @see com.gemstone.gemfire.management.internal.web.http.ClientHttpRequest
   * @see org.springframework.http.ResponseEntity
   */
  protected <T> ResponseEntity<T> send(final ClientHttpRequest request, final Class<T> responseType) {
    return send(request, responseType, Collections.<String, Object>emptyMap());
  }

  /**
   * Sends the HTTP request, using Spring's RestTemplate, to the GemFire REST API web service endpoint, expecting the
   * specified response type from the server in return.
   * @param request the client HTTP request to send.
   * @param responseType the expected Class type of the return value in the server's response.
   * @param uriVariables a Mapping of URI template path variables to values.
   * @return a ResponseEntity encapsulating the details of the server's response to the client's HTTP request.
   * @see java.net.URI
   * @see com.gemstone.gemfire.management.internal.web.http.ClientHttpRequest
   * @see org.springframework.http.ResponseEntity
   * @see org.springframework.web.client.RestTemplate#exchange(java.net.URI, org.springframework.http.HttpMethod, org.springframework.http.HttpEntity, Class)
   */
  protected <T> ResponseEntity<T> send(final ClientHttpRequest request, final Class<T> responseType, final Map<String, ?> uriVariables) {
    final URI url = request.getURL(uriVariables);

    if (isDebugEnabled()) {
      printInfo("Link: %1$s", request.getLink().toHttpRequestLine());
      printInfo("HTTP URL: %1$s", url);
      printInfo("HTTP request headers: %1$s", request.getHeaders());
      printInfo("HTTP request parameters: %1$s", request.getParameters());
    }

    final ResponseEntity<T> response = getRestTemplate().exchange(url, request.getMethod(),
      request.createRequestEntity(), responseType);

    if (isDebugEnabled()) {
      printInfo("------------------------------------------------------------------------");
      printInfo("HTTP response headers: %1$s", response.getHeaders());
      printInfo("HTTP response status: %1$d - %2$s", response.getStatusCode().value(),
        response.getStatusCode().getReasonPhrase());

      printInfo("HTTP response body: ", response.getBody());
    }

    return response;
  }

  /**
   * Determines whether this HTTP-based OperationInvoker is successfully connected to the remote GemFire Manager's
   * HTTP service in order to send commands for execution/processing.
   * @return a boolean value indicating the connection state of the HTTP-based OperationInvoker.
   */
  @Override
  public boolean isConnected() {
    return (getRestTemplate() != null);
  }

  /**
   * Determines whether this HTTP-based OperationInvoker is ready to send commands to the GemFire Manager for remote
   * execution/processing.
   * @return a boolean value indicating whether this HTTP-based OperationInvoker is ready for command invocations.
   * @see #isConnected()
   */
  @Override
  public boolean isReady() {
    return isConnected();
  }

  // TODO research the use of Jolokia instead

  /**
   * Read the attribute identified by name from a remote resource identified by name.  The intent of this method
   * is to return the value of an attribute on an MBean located in the remote MBeanServer.
   * @param resourceName name/url of the remote resource from which to fetch the attribute value.
   * @param attributeName name of the attribute who's value will be fetched.
   * @return the value of the named attribute for the named resource (typically an MBean).
   * @throws MBeanAccessException if an MBean access error occurs.
   * @throws RestApiCallForCommandNotFoundException if the REST API web service endpoint for accessing an attribute on
   * an MBean does not exists!
   * @see #createHttpRequest(com.gemstone.gemfire.management.internal.web.domain.Link)
   * @see #findLink(String)
   * @see #send(com.gemstone.gemfire.management.internal.web.http.ClientHttpRequest, Class)
   */
  @Override
  public Object getAttribute(final String resourceName, final String attributeName) {
    final Link link = findLink(MBEAN_ATTRIBUTE_LINK_RELATION);

    if (link != null) {
      final ClientHttpRequest request = createHttpRequest(link);

      request.addParameterValues("resourceName", resourceName);
      request.addParameterValues("attributeName", attributeName);

      final ResponseEntity<byte[]> response = send(request, byte[].class);

      try {
        return IOUtils.deserializeObject(response.getBody());
      }
      catch (IOException e) {
        throw new MBeanAccessException(String.format(
          "De-serializing the result of accessing attribute (%1$s) on MBean (%2$s) failed!",
          resourceName, attributeName), e);
      }
      catch (ClassNotFoundException e) {
        throw new MBeanAccessException(String.format(
          "The Class type of the result when accessing attribute (%1$s) on MBean (%2$s) was not found!",
          resourceName, attributeName), e);
      }
    }
    else {
      printSevere("Getting the value of attribute (%1$s) on MBean (%2$s) is currently an unsupported operation!",
        attributeName, resourceName);
      throw new RestApiCallForCommandNotFoundException(MBEAN_ATTRIBUTE_LINK_RELATION);
    }
  }

  /**
   * Gets the identifier of the GemFire cluster.
   * @return an integer value indicating the identifier of the GemFire cluster.
   * @see #initClusterId()
   */
  @Override
  public int getClusterId() {
    return clusterId;
  }

  protected void initClusterId() {
    if (isReady()) {
      try {
        clusterId = (Integer) getAttribute(ManagementConstants.OBJECTNAME__DISTRIBUTEDSYSTEM_MXBEAN, "DistributedSystemId");
        printDebug("Cluster ID (%1$s)", clusterId);
      }
      catch (Exception ignore) {
        printDebug("Failed to determine cluster ID: %1$s", ignore.getMessage());
      }
    }
  }

  /**
   * Gets a proxy to the remote DistributedSystem MXBean to access attributes and invoke operations on the distributed
   * system, or the GemFire cluster.
   * @return a proxy instance of the GemFire Manager's DistributedSystem MXBean.
   * @see #getMBeanProxy(javax.management.ObjectName, Class)
   * @see com.gemstone.gemfire.management.DistributedSystemMXBean
   * @see com.gemstone.gemfire.management.internal.MBeanJMXAdapter#getDistributedSystemName()
   */
  public DistributedSystemMXBean getDistributedSystemMXBean() {
    return getMBeanProxy(MBeanJMXAdapter.getDistributedSystemName(), DistributedSystemMXBean.class);
  }

  /**
   * Gets a proxy to an MXBean on a remote MBeanServer using HTTP for remoting.
   * @param <T> the class type of the remote MXBean.
   * @param objectName the JMX ObjectName uniquely identifying the remote MXBean.
   * @param mbeanInterface the interface of the remote MXBean to proxy for attribute/operation access.
   * @return a proxy using HTTP remoting to access the specified, remote MXBean.
   * @see javax.management.ObjectName
   * @see com.gemstone.gemfire.management.internal.web.shell.support.HttpMBeanProxyFactory
   */
  public <T> T getMBeanProxy(final ObjectName objectName, final Class<T> mbeanInterface) {
    return HttpMBeanProxyFactory.createMBeanProxy(this, objectName, mbeanInterface);
  }

  /**
   * Invoke an operation identified by name on a remote resource identified by name with the given arguments.
   * The intent of this method is to invoke an arbitrary operation on an MBean located in the remote MBeanServer.
   * @param resourceName name/url (object name) of the remote resource (MBea) on which operation is to be invoked.
   * @param operationName name of the operation to be invoked.
   * @param params an array of arguments for the parameters to be set when the operation is invoked.
   * @param signature an array containing the signature of the operation.
   * @return result of the operation invocation.
   * @throws MBeanAccessException if an MBean access error occurs.
   * @throws RestApiCallForCommandNotFoundException if the REST API web service endpoint for invoking an operation on
   * an MBean does not exists!
   * @see #createHttpRequest(com.gemstone.gemfire.management.internal.web.domain.Link)
   * @see #findLink(String)
   * @see #send(com.gemstone.gemfire.management.internal.web.http.ClientHttpRequest, Class)
   */
  // TODO research the use of Jolokia instead
  @Override
  public Object invoke(final String resourceName, final String operationName, final Object[] params, final String[] signature) {
    final Link link = findLink(MBEAN_OPERATION_LINK_RELATION);

    if (link != null) {
      final ClientHttpRequest request = createHttpRequest(link);

      request.addParameterValues("resourceName", resourceName);
      request.addParameterValues("operationName", operationName);
      request.addParameterValues("signature", (Object[]) signature);
      request.addParameterValues("parameters", params); // TODO may need to convert method parameter arguments

      final ResponseEntity<byte[]> response = send(request, byte[].class);

      try {
        return IOUtils.deserializeObject(response.getBody());
      }
      catch (IOException e) {
        throw new MBeanAccessException(String.format(
          "De-serializing the result from invoking operation (%1$s) on MBean (%2$s) failed!",
          resourceName, operationName), e);
      }
      catch (ClassNotFoundException e) {
        throw new MBeanAccessException(String.format(
          "The Class type of the result from invoking operation (%1$s) on MBean (%2$s) was not found!",
          resourceName, operationName), e);
      }
    }
    else {
      printSevere("Invoking operation (%1$s) on MBean (%2$s) is currently an unsupported operation!",
        operationName, resourceName);
      throw new RestApiCallForCommandNotFoundException(MBEAN_OPERATION_LINK_RELATION);
    }
  }

  /**
   * This method searches the MBean server, based on the OperationsInvoker's JMX-based or remoting capable MBean server
   * connection, for MBeans matching a specific ObjectName or matching an ObjectName pattern along with satisfying
   * criteria from the Query expression.
   * @param objectName the ObjectName or pattern for which matching MBeans in the target MBean server will be returned.
   * @param queryExpression the JMX-based query expression used to filter matching MBeans.
   * @return a set of ObjectName's matching MBeans in the MBean server matching the ObjectName and Query expression
   * criteria.
   * @see #createHttpRequest(com.gemstone.gemfire.management.internal.web.domain.Link)
   * @see #findLink(String)
   * @see #send(com.gemstone.gemfire.management.internal.web.http.ClientHttpRequest, Class)
   * @see javax.management.ObjectName
   * @see javax.management.QueryExp
   */
  @Override
  @SuppressWarnings("unchecked")
  public Set<ObjectName> queryNames(final ObjectName objectName, final QueryExp queryExpression) {
    final Link link = findLink(MBEAN_QUERY_LINK_RELATION);

    if (link != null) {
      final ClientHttpRequest request = createHttpRequest(link);

      request.setContent(new QueryParameterSource(objectName, queryExpression));

      final ResponseEntity<byte[]> response = send(request, byte[].class);

      try {
        return (Set<ObjectName>) IOUtils.deserializeObject(response.getBody());
      }
      catch (Exception e) {
        throw new MBeanAccessException(String.format(
          "An error occurred while querying for MBean names using ObjectName pattern (%1$s) and Query expression (%2$s)!",
          objectName, queryExpression), e);
      }
    }
    else {
      printSevere(
        "Running a query to get the ObjectNames of all MBeans matching the ObjectName pattern (%1$s) and Query expression (%2$s) is currently unsupported!",
        objectName, queryExpression);
      throw new RestApiCallForCommandNotFoundException(MBEAN_QUERY_LINK_RELATION);
    }
  }

  /**
   * Stops communication with and closes all connections to the remote HTTP server (service).
   */
  @Override
  public void stop() {
    if (executorService != null) {
      executorService.shutdown();
    }

    restTemplate = null;
  }

  @Override
  public String toString() {
    return String.format("GemFire Manager HTTP service @ %1$s", getBaseUrl());
  }

}
