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

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;

import com.gemstone.gemfire.internal.lang.Filter;
import com.gemstone.gemfire.internal.lang.Initable;
import com.gemstone.gemfire.internal.lang.StringUtils;
import com.gemstone.gemfire.internal.logging.LogService;
import com.gemstone.gemfire.internal.util.CollectionUtils;
import com.gemstone.gemfire.management.internal.cli.CommandRequest;
import com.gemstone.gemfire.management.internal.cli.i18n.CliStrings;
import com.gemstone.gemfire.management.internal.cli.shell.Gfsh;
import com.gemstone.gemfire.management.internal.web.domain.Link;
import com.gemstone.gemfire.management.internal.web.domain.LinkIndex;
import com.gemstone.gemfire.management.internal.web.http.ClientHttpRequest;
import com.gemstone.gemfire.management.internal.web.http.HttpHeader;
import com.gemstone.gemfire.management.internal.web.util.ConvertUtils;

import org.apache.logging.log4j.Logger;
import org.springframework.http.HttpMethod;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.http.client.ClientHttpResponse;
import org.springframework.web.client.ResourceAccessException;
import org.springframework.web.util.UriTemplate;

/**
 * The RestHttpOperationInvoker class is an implementation of the OperationInvoker interface that translates (adapts) 
 * GemFire shell command invocations into HTTP requests to a corresponding REST API call hosted by the GemFire Manager's
 * HTTP service using the Spring RestTemplate.
 * 
 * @see com.gemstone.gemfire.internal.lang.Initable
 * @see com.gemstone.gemfire.management.internal.cli.shell.Gfsh
 * @see com.gemstone.gemfire.management.internal.cli.shell.OperationInvoker
 * @see com.gemstone.gemfire.management.internal.web.shell.AbstractHttpOperationInvoker
 * @see com.gemstone.gemfire.management.internal.web.shell.HttpOperationInvoker
 * @see com.gemstone.gemfire.management.internal.web.shell.SimpleHttpOperationInvoker
 * @since GemFire 8.0
 */
@SuppressWarnings("unused")
public class RestHttpOperationInvoker extends AbstractHttpOperationInvoker implements Initable {

  private static final Logger logger = LogService.getLogger();
  
  protected static final String ENVIRONMENT_VARIABLE_REQUEST_PARAMETER_PREFIX = "vf.gf.env.";
  protected static final String RESOURCES_REQUEST_PARAMETER = "resources";

  // the HttpOperationInvoker used when this RestHttpOperationInvoker is unable to resolve the correct REST API
  // web service endpoint (URI) for a command
  private final HttpOperationInvoker httpOperationInvoker;

  // the LinkIndex containing Links to all GemFire REST API web service endpoints
  private final LinkIndex linkIndex;

  /**
   * Constructs an instance of the RestHttpOperationInvoker class initialized with the given link index containing links
   * referencing all REST API web service endpoints.  This constructor should only be used for testing purposes.
   * 
   * @param linkIndex the LinkIndex containing Links to all REST API web service endpoints in GemFire's REST interface.
   * @see com.gemstone.gemfire.management.internal.web.domain.LinkIndex
   */
  RestHttpOperationInvoker(final LinkIndex linkIndex) {
    super(REST_API_URL);
    assertNotNull(linkIndex, "The Link Index resolving commands to REST API web service endpoints cannot be null!");
    this.linkIndex = linkIndex;
    this.httpOperationInvoker = new SimpleHttpOperationInvoker();
  }

  /**
   * Constructs an instance of the RestHttpOperationInvoker class initialized with the given link index containing links
   * referencing all REST API web service endpoints.  In addition, a reference to the instance of GemFire shell (Gfsh)
   * using this RestHttpOperationInvoker to send command invocations to the GemFire Manager's HTTP service via HTTP
   * for processing is required in order to interact with the shell and provide feedback to the user.
   * 
   * @param linkIndex the LinkIndex containing Links to all REST API web service endpoints in GemFire' REST interface.
   * @param gfsh a reference to the instance of the GemFire shell using this OperationInvoker to process commands.
   * @see #RestHttpOperationInvoker(com.gemstone.gemfire.management.internal.web.domain.LinkIndex, com.gemstone.gemfire.management.internal.cli.shell.Gfsh,  Map)
   * @see com.gemstone.gemfire.management.internal.cli.shell.Gfsh
   * @see com.gemstone.gemfire.management.internal.web.domain.LinkIndex
   */
  public RestHttpOperationInvoker(final LinkIndex linkIndex, final Gfsh gfsh, Map<String,String> securityProperties) {
    this(linkIndex, gfsh, CliStrings.CONNECT__DEFAULT_BASE_URL, securityProperties);
  }

  /**
   * Constructs an instance of the RestHttpOperationInvoker class initialized with the given link index containing links
   * referencing all REST API web service endpoints.  In addition, a reference to the instance of GemFire shell (Gfsh)
   * using this RestHttpOperationInvoker to send command invocations to the GemFire Manager's HTTP service via HTTP
   * for processing is required in order to interact with the shell and provide feedback to the user.  Finally, a URL
   * to the HTTP service running in the GemFire Manager is specified as the base location for all HTTP requests.
   * 
   * @param linkIndex the LinkIndex containing Links to all REST API web service endpoints in GemFire's REST interface.
   * @param gfsh a reference to the instance of the GemFire shell using this OperationInvoker to process commands.
   * @param baseUrl the String specifying the base URL to the GemFire Manager's HTTP service, REST interface.
   * @see com.gemstone.gemfire.management.internal.web.domain.LinkIndex
   * @see com.gemstone.gemfire.management.internal.cli.shell.Gfsh
   */
  public RestHttpOperationInvoker(final LinkIndex linkIndex, final Gfsh gfsh, final String baseUrl, Map<String,String> securityProperties) {
    super(gfsh, baseUrl, securityProperties);
    assertNotNull(linkIndex, "The Link Index resolving commands to REST API web service endpoints cannot be null!");
    this.linkIndex = linkIndex;
    this.httpOperationInvoker = new SimpleHttpOperationInvoker(gfsh, baseUrl, securityProperties);

  }

  /**
   * Initializes the RestHttpOperationInvokers scheduled and periodic monitoring task to assess the availibity of the
   * targeted GemFire Manager's HTTP service.
   * 
   * @see com.gemstone.gemfire.internal.lang.Initable#init()
   * @see org.springframework.http.client.ClientHttpRequest
   */
  @SuppressWarnings("null")
  public void init() {
    final Link pingLink = getLinkIndex().find(PING_LINK_RELATION);

    if (pingLink != null) {
      if (logger.isDebugEnabled()) {
        logger.debug("Scheduling periodic HTTP ping requests to monitor the availability of the GemFire Manager HTTP service @ ({})",
            getBaseUrl());
      }

      getExecutorService().scheduleAtFixedRate(new Runnable() {
        public void run() {
          try {
            org.springframework.http.client.ClientHttpRequest httpRequest = getRestTemplate().getRequestFactory()
              .createRequest(pingLink.getHref(), HttpMethod.HEAD);

            httpRequest.getHeaders().set(HttpHeader.USER_AGENT.getName(), USER_AGENT_HTTP_REQUEST_HEADER_VALUE);
            httpRequest.getHeaders().setAccept(getAcceptableMediaTypes());
            httpRequest.getHeaders().setContentLength(0l);

            if(securityProperties != null){
              Iterator<Entry<String, String>> it = securityProperties.entrySet().iterator();
              while(it.hasNext()){
                Entry<String,String> entry= it.next();
                httpRequest.getHeaders().add(entry.getKey(), entry.getValue());
              }
            }

            ClientHttpResponse httpResponse = httpRequest.execute();

            if (HttpStatus.NOT_FOUND.equals(httpResponse.getStatusCode())) {
              throw new IOException(String.format("The HTTP service at URL (%1$s) could not be found!",
                pingLink.getHref()));
            }
            else if (!HttpStatus.OK.equals(httpResponse.getStatusCode())) {
              printDebug("Received unexpected HTTP status code (%1$d - %2$s) for HTTP request (%3$s).",
                httpResponse.getRawStatusCode(), httpResponse.getStatusText(), pingLink.getHref());
            }
          }
          catch (IOException e) {
            printDebug("An error occurred while connecting to the Manager's HTTP service: %1$s: ", e.getMessage());
            getGfsh().notifyDisconnect(RestHttpOperationInvoker.this.toString());
            stop();
          }
        }
      }, DEFAULT_INITIAL_DELAY, DEFAULT_PERIOD, DEFAULT_TIME_UNIT);
    }
    else {
      if (logger.isDebugEnabled()) {
        logger.debug("The Link to the GemFire Manager web service endpoint @ ({}) to monitor availability was not found!",
            getBaseUrl());
      }
    }

    initClusterId();
  }

  /**
   * Returns a reference to an implementation of HttpOperationInvoker used as the fallback by this
   * RestHttpOperationInvoker for processing commands via HTTP requests.
   * 
   * @return an instance of HttpOperationInvoker used by this RestHttpOperationInvoker as a fallback to process commands
   * via HTTP requests.
   * @see com.gemstone.gemfire.management.internal.web.shell.HttpOperationInvoker
   */
  protected HttpOperationInvoker getHttpOperationInvoker() {
    return httpOperationInvoker;
  }

  /**
   * Returns the LinkIndex resolving Gfsh commands to GemFire REST API web service endpoints.  The corresponding
   * web service endpoint is a URI/URL uniquely identifying the resource on which the command was invoked.
   * 
   * @return the LinkIndex containing Links for all GemFire REST API web service endpoints.
   * @see com.gemstone.gemfire.management.internal.web.domain.LinkIndex
   */
  protected LinkIndex getLinkIndex() {
    return linkIndex;
  }

  /**
   * Creates an HTTP request from the specified command invocation encapsulated by the CommandRequest object.
   * The CommandRequest identifies the resource targeted by the command invocation along with any parameters to be sent
   * as part of the HTTP request.
   * 
   * @param command the CommandRequest object encapsulating details of the command invocation.
   * @return a client HTTP request detailing the operation to be performed on the remote resource targeted by the
   * command invocation.
   * @see AbstractHttpOperationInvoker#createHttpRequest(com.gemstone.gemfire.management.internal.web.domain.Link)
   * @see com.gemstone.gemfire.management.internal.cli.CommandRequest
   * @see com.gemstone.gemfire.management.internal.web.http.ClientHttpRequest
   * @see com.gemstone.gemfire.management.internal.web.util.ConvertUtils#convert(byte[][])
   */
  protected ClientHttpRequest createHttpRequest(final CommandRequest command) {
    ClientHttpRequest request = createHttpRequest(findLink(command));

    Map<String, String> commandParameters = command.getParameters();

    for (Map.Entry<String, String> entry : commandParameters.entrySet()) {
      if (NullValueFilter.INSTANCE.accept(entry)) {
        request.addParameterValues(entry.getKey(), entry.getValue());
      }
    }

    Map<String, String> environmentVariables = command.getEnvironment();

    for (Map.Entry<String, String> entry : environmentVariables.entrySet()) {
      if (EnvironmentVariableFilter.INSTANCE.accept(entry)) {
        request.addParameterValues(ENVIRONMENT_VARIABLE_REQUEST_PARAMETER_PREFIX + entry.getKey(), entry.getValue());
      }
    }

    if (command.getFileData() != null) {
      request.addParameterValues(RESOURCES_REQUEST_PARAMETER, (Object[]) ConvertUtils.convert(command.getFileData()));
    }

    return request;
  }

  /**
   * Finds a Link from the Link Index containing the HTTP request URI to the web service endpoint for the relative
   * operation on the resource.
   * 
   * @param relation a String describing the relative operation (state transition) on the resource.
   * @return an instance of Link containing the HTTP request URI used to perform the intended operation on the resource.
   * @see #getLinkIndex()
   * @see com.gemstone.gemfire.management.internal.web.domain.Link
   * @see com.gemstone.gemfire.management.internal.web.domain.LinkIndex#find(String)
   */
  @Override
  protected Link findLink(final String relation) {
    return getLinkIndex().find(relation);
  }

  /**
   * Finds a Link from the Link Index corresponding to the command invocation.  The CommandRequest indicates the
   * intended function on the target resource so the proper Link based on it's relation (the state transition of the
   * corresponding function), along with it's method of operation and corresponding REST API web service endpoint (URI),
   * can be identified.
   * 
   * @param command the CommandRequest object encapsulating the details of the command invocation.
   * @return a Link referencing the correct REST API web service endpoint (URI) and method for the command invocation.
   * @see #getLinkIndex()
   * @see #resolveLink(com.gemstone.gemfire.management.internal.cli.CommandRequest, java.util.List)
   * @see com.gemstone.gemfire.management.internal.cli.CommandRequest
   * @see com.gemstone.gemfire.management.internal.web.domain.Link
   * @see com.gemstone.gemfire.management.internal.web.domain.LinkIndex
   */
  protected Link findLink(final CommandRequest command) {
    List<Link> linksFound = new ArrayList<>(getLinkIndex().size());

    for (Link link : getLinkIndex()) {
      if (command.getInput().startsWith(link.getRelation())) {
        linksFound.add(link);
      }
    }

    if (linksFound.isEmpty()) {
      throw new RestApiCallForCommandNotFoundException(String.format("No REST API call for command (%1$s) was found!",
        command.getInput()));
    }

    return (linksFound.size() > 1 ? resolveLink(command, linksFound) : linksFound.get(0));
  }

  /**
   * Resolves one Link from a Collection of Links based on the command invocation matching multiple relations from
   * the Link Index.
   * 
   * @param command the CommandRequest object encapsulating details of the command invocation.
   * @param links a Collection of Links for the command matching the relation.
   * @return the resolved Link matching the command exactly as entered by the user.
   * @see #findLink(com.gemstone.gemfire.management.internal.cli.CommandRequest)
   * @see com.gemstone.gemfire.management.internal.cli.CommandRequest
   * @see com.gemstone.gemfire.management.internal.web.domain.Link
   * @see org.springframework.web.util.UriTemplate
   */
  // Find and use the Link with the greatest number of path variables that can be expanded!
  protected Link resolveLink(final CommandRequest command, final List<Link> links) {
    // NOTE, Gfsh's ParseResult contains a Map entry for all command options whether or not the user set the option
    // with a value on the command-line, argh!
    Map<String, String> commandParametersCopy = CollectionUtils.removeKeys(
      new HashMap<>(command.getParameters()), NoValueFilter.INSTANCE);

    Link resolvedLink = null;

    int pathVariableCount = 0;

    for (Link link : links) {
      final List<String> pathVariables = new UriTemplate(decode(link.getHref().toString())).getVariableNames();

      // first, all path variables in the URL/URI template must be resolvable/expandable for this Link
      // to even be considered...
      if (commandParametersCopy.keySet().containsAll(pathVariables)) {
        // then, either we have not found a Link for the command yet, or the number of resolvable/expandable
        // path variables in this Link has to be greater than the number of resolvable/expandable path variables
        // for the last Link
        if (resolvedLink == null || (pathVariables.size() > pathVariableCount)) {
          resolvedLink = link;
          pathVariableCount = pathVariables.size();
        }
      }
    }

    if (resolvedLink == null) {
      throw new RestApiCallForCommandNotFoundException(String.format("No REST API call for command (%1$s) was found!",
        command.getInput()));
    }

    return resolvedLink;
  }

  /**
   * Processes the requested command.  Sends the command to the GemFire Manager for remote processing (execution).
   * 
   * @param command the command requested/entered by the user to be processed.
   * @return the result of the command execution.
   * @see #createHttpRequest(com.gemstone.gemfire.management.internal.cli.CommandRequest)
   * @see #handleResourceAccessException(org.springframework.web.client.ResourceAccessException)
   * @see #isConnected()
   * @see #send(com.gemstone.gemfire.management.internal.web.http.ClientHttpRequest, Class, java.util.Map)
   * @see #simpleProcessCommand(com.gemstone.gemfire.management.internal.cli.CommandRequest, RestApiCallForCommandNotFoundException)
   * @see com.gemstone.gemfire.management.internal.cli.CommandRequest
   * @see org.springframework.http.ResponseEntity
   */
  @Override
  public String processCommand(final CommandRequest command) {
    assertState(isConnected(), "Gfsh must be connected to the GemFire Manager in order to process commands remotely!");

    try {
      ResponseEntity<String> response = send(createHttpRequest(command), String.class, command.getParameters());

      return response.getBody();
    }
    catch (RestApiCallForCommandNotFoundException e) {
      return simpleProcessCommand(command, e);
    }
    catch (ResourceAccessException e) {
      return handleResourceAccessException(e);
    }
  }

  /**
   * A method to process the command by sending an HTTP request to the simple URL/URI web service endpoint, where all
   * details of the request and command invocation are encoded in the URL/URI.
   * 
   * @param command the CommandRequest encapsulating the details of the command invocation.
   * @param e the RestApiCallForCommandNotFoundException indicating the standard REST API web service endpoint
   * could not be found.
   * @return the result of the command execution.
   * @see #getHttpOperationInvoker()
   * @see com.gemstone.gemfire.management.internal.web.shell.HttpOperationInvoker#processCommand(com.gemstone.gemfire.management.internal.cli.CommandRequest)
   * @see com.gemstone.gemfire.management.internal.cli.CommandRequest
   */
  protected String simpleProcessCommand(final CommandRequest command, final RestApiCallForCommandNotFoundException e) {
    if (getHttpOperationInvoker() != null) {
      printWarning("WARNING - No REST API web service endpoint (URI) exists for command (%1$s); using the non-RESTful, simple URI.",
        command.getName());

      return String.valueOf(getHttpOperationInvoker().processCommand(command));
    }

    throw e;
  }

  protected static class EnvironmentVariableFilter extends NoValueFilter {

    protected static final EnvironmentVariableFilter INSTANCE = new EnvironmentVariableFilter();

    @Override
    public boolean accept(final Map.Entry<String, String> entry) {
      return (!entry.getKey().startsWith("SYS") && super.accept(entry));
    }
  }

  protected static class NoValueFilter implements Filter<Map.Entry<String, String>> {

    protected static final NoValueFilter INSTANCE = new NoValueFilter();

    @Override
    public boolean accept(final Map.Entry<String, String> entry) {
      return !StringUtils.isBlank(entry.getValue());
    }
  }

  protected static class NullValueFilter implements Filter<Map.Entry<String, ?>> {

    protected static final NullValueFilter INSTANCE = new NullValueFilter();

    @Override
    public boolean accept(final Map.Entry<String, ?> entry) {
      return (entry.getValue() != null);
    }
  }

}
