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

import java.net.URI;
import java.util.Map;

import com.gemstone.gemfire.management.internal.cli.CommandRequest;
import com.gemstone.gemfire.management.internal.cli.shell.Gfsh;
import com.gemstone.gemfire.management.internal.web.domain.Link;
import com.gemstone.gemfire.management.internal.web.http.ClientHttpRequest;
import com.gemstone.gemfire.management.internal.web.http.HttpMethod;

import org.springframework.http.ResponseEntity;
import org.springframework.web.client.ResourceAccessException;
import org.springframework.web.util.UriComponentsBuilder;

/**
 * The SimpleHttpOperationInvoker class is an implementation of the OperationInvoker interface that issues commands
 * to the GemFire Manager via HTTP.  The SimpleHttpOperationInvoker uses a single URL web service endpoint to process
 * commands and return responses.
 * 
 * @see java.net.URI
 * @see com.gemstone.gemfire.management.internal.cli.shell.Gfsh
 * @see com.gemstone.gemfire.management.internal.cli.shell.OperationInvoker
 * @see com.gemstone.gemfire.management.internal.web.shell.AbstractHttpOperationInvoker
 * @see com.gemstone.gemfire.management.internal.web.shell.HttpOperationInvoker
 * @see com.gemstone.gemfire.management.internal.web.shell.RestHttpOperationInvoker
 * @since GemFire 8.0
 */
@SuppressWarnings("unused")
public class SimpleHttpOperationInvoker extends AbstractHttpOperationInvoker {

  protected static final String CMD_QUERY_PARAMETER = "cmd";
  protected static final String LINK_RELATION = "simple";
  protected static final String REST_API_MANAGEMENT_COMMANDS_URI = "/management/commands";

  /**
   * Default no-arg constructor to create an instance of the SimpleHttpOperationInvoker class for testing purposes.
   */
  SimpleHttpOperationInvoker() {
    super(REST_API_URL);
  }

  /**
   * Constructs an instance of the SimpleHttpOperationInvoker class initialized with a reference to the GemFire shell
   * (Gfsh) using this HTTP-based OperationInvoker to send command invocations to the GemFire Manager's HTTP service
   * using HTTP processing.
   * 
   * @param gfsh a reference to the instance of the GemFire shell using this OperationInvoker to process commands.
   * @see #SimpleHttpOperationInvoker(com.gemstone.gemfire.management.internal.cli.shell.Gfsh, String, Map)
   * @see com.gemstone.gemfire.management.internal.cli.shell.Gfsh
   */
  public SimpleHttpOperationInvoker(final Gfsh gfsh, Map<String,String> securityProperties) {
    this(gfsh, REST_API_URL, securityProperties);
  }

  /**
   * Constructs an instance of the SimpleHttpOperationInvoker class initialized with a reference to the GemFire shell
   * (Gfsh) using this HTTP-based OperationInvoker to send command invocations to the GemFire Manager's HTTP service
   * using HTTP for processing.  In addition, the base URL to the HTTP service running in the GemFire Manager is
   * specified as the base location for all HTTP requests.
   * 
   * @param gfsh a reference to the instance of the GemFire shell using this OperationInvoker to process commands.
   * @param baseUrl the base URL to the GemFire Manager's HTTP service.
   * @see com.gemstone.gemfire.management.internal.cli.shell.Gfsh
   */
  public SimpleHttpOperationInvoker(final Gfsh gfsh, final String baseUrl, Map<String,String> securityProperties) {
    super(gfsh, baseUrl, securityProperties);
  }

  /**
   * Creates an HTTP request from a command invocation encapsulated in a CommandRequest object.  The CommandRequest
   * identifies the resource targeted by the command invocation along with any parameters to be sent as part of the
   * HTTP request.
   * 
   * @param command a CommandRequest object encapsulating the details of the command invocation.
   * @return a client HTTP request detailing the operation to be performed on the remote resource targeted by the
   * command invocation.
   * @see #createLink(com.gemstone.gemfire.management.internal.cli.CommandRequest)
   * @see AbstractHttpOperationInvoker#createHttpRequest(com.gemstone.gemfire.management.internal.web.domain.Link)
   * @see com.gemstone.gemfire.management.internal.cli.CommandRequest
   * @see com.gemstone.gemfire.management.internal.web.http.ClientHttpRequest
   */
  protected ClientHttpRequest createHttpRequest(final CommandRequest command) {
    return createHttpRequest(createLink(command));
  }

  /**
   * Creates a Link based on the resource being targeted by the command invocation.  The Link will contain the URI
   * uniquely identifying the resource along with the HTTP GET operation specifying the method of processing.
   * 
   * @param command a CommandRequest object encapsulating the details of the command invocation.
   * @return a Link identifying the resource and the operation on the resource.
   * @see AbstractHttpOperationInvoker#createLink(String, java.net.URI, com.gemstone.gemfire.management.internal.web.http.HttpMethod)
   * @see com.gemstone.gemfire.management.internal.cli.CommandRequest
   * @see com.gemstone.gemfire.management.internal.web.domain.Link
   */
  protected Link createLink(final CommandRequest command) {
    return createLink(LINK_RELATION, getHttpRequestUrl(command), HttpMethod.POST);
  }

  /**
   * Gets HTTP request URL (URI) locating the proper resource along with details for the request.
   * 
   * @param command a CommandRequest object encapsulating the details of the command invocation.
   * @return a URI identifying the resource, it's location as well as details of the HTTP request.
   * @see com.gemstone.gemfire.management.internal.cli.CommandRequest
   * @see java.net.URI
   * @see com.gemstone.gemfire.management.internal.cli.CommandRequest
   * @see org.springframework.web.util.UriComponentsBuilder
   */
  protected URI getHttpRequestUrl(final CommandRequest command) {
    return UriComponentsBuilder.fromHttpUrl(getBaseUrl())
      .path(REST_API_MANAGEMENT_COMMANDS_URI)
      .queryParam(CMD_QUERY_PARAMETER, command.getInput())
      .build().encode().toUri();
  }

  /**
   * Processes the requested command.  Sends the command to the GemFire Manager for remote processing (execution).
   * 
   * @param command the command requested/entered by the user to be processed.
   * @return the result of the command execution.
   * @see #isConnected()
   * @see #createHttpRequest(com.gemstone.gemfire.management.internal.cli.CommandRequest)
   * @see AbstractHttpOperationInvoker#handleResourceAccessException(org.springframework.web.client.ResourceAccessException)
   * @see AbstractHttpOperationInvoker#send(com.gemstone.gemfire.management.internal.web.http.ClientHttpRequest, Class)
   * @see com.gemstone.gemfire.management.internal.cli.CommandRequest
   * @see org.springframework.http.ResponseEntity
   */
  @Override
  public String processCommand(final CommandRequest command) {
    assertState(isConnected(), "Gfsh must be connected to the GemFire Manager in order to process commands remotely!");

    try {
      final ResponseEntity<String> response = send(createHttpRequest(command), String.class);

      return response.getBody();
    }
    catch (ResourceAccessException e) {
      return handleResourceAccessException(e);
    }
  }

}
