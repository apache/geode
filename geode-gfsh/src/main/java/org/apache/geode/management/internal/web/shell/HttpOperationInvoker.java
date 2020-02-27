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
package org.apache.geode.management.internal.web.shell;

import java.io.File;
import java.io.IOException;
import java.net.URI;
import java.util.Properties;
import java.util.Set;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

import javax.management.ObjectName;
import javax.management.QueryExp;

import org.apache.logging.log4j.Logger;
import org.springframework.core.io.FileSystemResource;
import org.springframework.util.LinkedMultiValueMap;
import org.springframework.util.MultiValueMap;

import org.apache.geode.annotations.Immutable;
import org.apache.geode.internal.util.IOUtils;
import org.apache.geode.logging.internal.log4j.api.LogService;
import org.apache.geode.management.DistributedSystemMXBean;
import org.apache.geode.management.internal.MBeanJMXAdapter;
import org.apache.geode.management.internal.ManagementConstants;
import org.apache.geode.management.internal.cli.CommandRequest;
import org.apache.geode.management.internal.cli.shell.Gfsh;
import org.apache.geode.management.internal.cli.shell.OperationInvoker;
import org.apache.geode.management.internal.web.domain.QueryParameterSource;
import org.apache.geode.management.internal.web.http.support.HttpRequester;
import org.apache.geode.management.internal.web.shell.support.HttpMBeanProxyFactory;

/**
 * The HttpOperationInvoker class is an abstract base class encapsulating common functionality for
 * all HTTP-based OperationInvoker implementations.
 *
 * @see org.apache.geode.management.internal.cli.shell.Gfsh
 * @see org.apache.geode.management.internal.cli.shell.OperationInvoker
 * @see org.apache.geode.management.internal.web.shell.HttpOperationInvoker
 * @since GemFire 8.0
 */
@SuppressWarnings("unused")
public class HttpOperationInvoker implements OperationInvoker {

  protected static final long DEFAULT_INITIAL_DELAY = TimeUnit.SECONDS.toMillis(1);
  protected static final long DEFAULT_PERIOD = TimeUnit.MILLISECONDS.toMillis(2000);
  @Immutable
  protected static final TimeUnit DEFAULT_TIME_UNIT = TimeUnit.MILLISECONDS;
  protected static final String CMD_QUERY_PARAMETER = "cmd";
  protected static final String COMMANDS_URI = "/management/commands";
  protected static final String RESOURCES_REQUEST_PARAMETER = "resources";

  // the ID of the GemFire distributed system (cluster)
  private Integer clusterId = CLUSTER_ID_WHEN_NOT_CONNECTED;

  // Executor for scheduling periodic Runnable task to assess the state of the Manager's HTTP
  // service or Web Service
  private final ScheduledExecutorService executorService;

  // a reference to the GemFire shell (Gfsh) instance using this HTTP-based OperationInvoker for
  // command execution
  // and processing
  private final Gfsh gfsh;

  private final String baseUrl;

  // a Java Logger used to log severe, warning, informational and debug messages during the
  // operation of this invoker
  private final Logger logger = LogService.getLogger();

  // the Spring RestTemplate used to executeRequest HTTP requests and make REST API calls
  private volatile HttpRequester httpRequester;

  private boolean connected = false;

  /**
   * Constructs an instance of the HttpOperationInvoker class with a reference to the GemFire shell
   * (Gfsh) instance using this HTTP-based OperationInvoker to send commands to the GemFire Manager
   * via HTTP for procsessing along with the base URL to the GemFire Manager's embedded HTTP service
   * hosting the HTTP (REST) interface.
   *
   * @param gfsh a reference to the instance of the GemFire shell (Gfsh) using this HTTP-based
   *        OperationInvoker for command processing.
   * @param baseUrl a String specifying the base URL to the GemFire Manager's embedded HTTP service
   *        hosting the REST interface.
   * @throws AssertionError if the reference to the Gfsh instance is null.
   * @see org.apache.geode.management.internal.cli.shell.Gfsh
   */
  public HttpOperationInvoker(final Gfsh gfsh, final String baseUrl,
      Properties securityProperties) {
    this.gfsh = gfsh;
    this.baseUrl = baseUrl;
    this.httpRequester = new HttpRequester(securityProperties);

    // request ping and then schedule the ping to access the "alive" state, this will trigger
    // authentication check
    httpRequester.get(HttpRequester.createURI(baseUrl, "/ping"), String.class);
    connected = true;

    // constructs an instance of a single-threaded, scheduled Executor to send periodic HTTP
    // requests to the Manager's HTTP service or Web Service to assess the "alive" state
    this.executorService = Executors.newSingleThreadScheduledExecutor();
    executorService.scheduleAtFixedRate(() -> {
      try {
        httpRequester.get(HttpRequester.createURI(baseUrl, "/ping"), String.class);
      } catch (Exception e) {
        printDebug("An error occurred while connecting to the Manager's HTTP service: %1$s: ",
            e.getMessage());
        getGfsh().notifyDisconnect(HttpOperationInvoker.this.toString());
        stop();
      }
    }, DEFAULT_INITIAL_DELAY, DEFAULT_PERIOD, DEFAULT_TIME_UNIT);

    // initialize cluster id
    clusterId = (Integer) getAttribute(ManagementConstants.OBJECTNAME__DISTRIBUTEDSYSTEM_MXBEAN,
        "DistributedSystemId");
  }

  /**
   * Asserts whether state, based on the evaluation of a conditional expression, passed to this
   * assertion is valid.
   *
   * @param validState a boolean value indicating the evaluation of the expression from which the
   *        conditional state is based. For example, a caller might use an expression of the form
   *        (initableObj.isInitialized()).
   * @param message a String values used as the message when constructing an IllegalStateException.
   * @param args Object arguments used to populate placeholder's in the message.
   * @throws IllegalStateException if the conditional state is not valid.
   * @see java.lang.String#format(String, Object...)
   */
  protected static void assertState(final boolean validState, final String message,
      final Object... args) {
    if (!validState) {
      throw new IllegalStateException(String.format(message, args));
    }
  }

  /**
   * Determines whether Gfsh is in debug mode (or whether the user enabled debugging in Gfsh).
   *
   * @return a boolean value indicating if debugging has been turned on in Gfsh.
   * @see org.apache.geode.management.internal.cli.shell.Gfsh#getDebug()
   */
  protected boolean isDebugEnabled() {
    return getGfsh().getDebug();
  }

  /**
   * Gets the ExecutorService used by this HTTP OperationInvoker to scheduled periodic or delayed
   * tasks.
   *
   * @return an instance of the ScheduledExecutorService for scheduling periodic or delayed tasks.
   * @see java.util.concurrent.ScheduledExecutorService
   */
  protected ScheduledExecutorService getExecutorService() {
    return this.executorService;
  }

  /**
   * Returns the reference to the GemFire shell (Gfsh) instance using this HTTP-based
   * OperationInvoker to send commands to the GemFire Manager for remote execution and processing.
   *
   * @return a reference to the instance of the GemFire shell (Gfsh) using this HTTP-based
   *         OperationInvoker to process commands.
   * @see org.apache.geode.management.internal.cli.shell.Gfsh
   */
  protected Gfsh getGfsh() {
    return this.gfsh;
  }

  /**
   * Displays the message inside GemFire shell at debug level.
   *
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
   *
   * @param message the String containing the message to display inside Gfsh.
   * @see org.apache.geode.management.internal.cli.shell.Gfsh#printAsInfo(String)
   */
  protected void printInfo(final String message, final Object... args) {
    getGfsh().printAsInfo(String.format(message, args));
  }

  /**
   * Displays the message inside GemFire shell at warning level.
   *
   * @param message the String containing the message to display inside Gfsh.
   * @see org.apache.geode.management.internal.cli.shell.Gfsh#printAsWarning(String)
   */
  protected void printWarning(final String message, final Object... args) {
    getGfsh().printAsWarning(String.format(message, args));
  }

  /**
   * Displays the message inside GemFire shell at severe level.
   *
   * @param message the String containing the message to display inside Gfsh.
   * @see org.apache.geode.management.internal.cli.shell.Gfsh#printAsSevere(String)
   */
  protected void printSevere(final String message, final Object... args) {
    getGfsh().printAsSevere(String.format(message, args));
  }



  /**
   * Determines whether this HTTP-based OperationInvoker is successfully connected to the remote
   * GemFire Manager's HTTP service in order to send commands for execution/processing.
   *
   * @return a boolean value indicating the connection state of the HTTP-based OperationInvoker.
   */
  @Override
  public boolean isConnected() {
    return connected;
  }

  /**
   * Determines whether this HTTP-based OperationInvoker is ready to send commands to the GemFire
   * Manager for remote execution/processing.
   *
   * @return a boolean value indicating whether this HTTP-based OperationInvoker is ready for
   *         command invocations.
   * @see #isConnected()
   */
  @Override
  public boolean isReady() {
    return isConnected();
  }

  /**
   * Read the attribute identified by name from a remote resource identified by name. The intent of
   * this method is to return the value of an attribute on an MBean located in the remote
   * MBeanServer.
   *
   * @param resourceName name/url of the remote resource from which to fetch the attribute value.
   * @param attributeName name of the attribute who's value will be fetched.
   * @return the value of the named attribute for the named resource (typically an MBean).
   * @throws MBeanAccessException if an MBean access error occurs.
   */
  @Override
  public Object getAttribute(final String resourceName, final String attributeName) {
    final URI link = HttpRequester.createURI(baseUrl, "/mbean/attribute", "resourceName",
        resourceName, "attributeName", attributeName);

    try {
      return IOUtils.deserializeObject(httpRequester.get(link, byte[].class));
    } catch (IOException e) {
      throw new MBeanAccessException(String.format(
          "De-serializing the result of accessing attribute (%1$s) on MBean (%2$s) failed!",
          resourceName, attributeName), e);
    } catch (ClassNotFoundException e) {
      throw new MBeanAccessException(String.format(
          "The Class type of the result when accessing attribute (%1$s) on MBean (%2$s) was not found!",
          resourceName, attributeName), e);
    }
  }

  @Override
  public int getClusterId() {
    return clusterId;
  }

  /**
   * Gets a proxy to the remote DistributedSystem MXBean to access attributes and invoke operations
   * on the distributed system, or the GemFire cluster.
   *
   * @return a proxy instance of the GemFire Manager's DistributedSystem MXBean.
   * @see #getMBeanProxy(javax.management.ObjectName, Class)
   */
  @Override
  public DistributedSystemMXBean getDistributedSystemMXBean() {
    return getMBeanProxy(MBeanJMXAdapter.getDistributedSystemName(), DistributedSystemMXBean.class);
  }

  /**
   * Gets a proxy to an MXBean on a remote MBeanServer using HTTP for remoting.
   *
   * @param <T> the class type of the remote MXBean.
   * @param objectName the JMX ObjectName uniquely identifying the remote MXBean.
   * @param mbeanInterface the interface of the remote MXBean to proxy for attribute/operation
   *        access.
   * @return a proxy using HTTP remoting to access the specified, remote MXBean.
   * @see javax.management.ObjectName
   * @see org.apache.geode.management.internal.web.shell.support.HttpMBeanProxyFactory
   */
  @Override
  public <T> T getMBeanProxy(final ObjectName objectName, final Class<T> mbeanInterface) {
    return HttpMBeanProxyFactory.createMBeanProxy(this, objectName, mbeanInterface);
  }

  /**
   * Invoke an operation identified by name on a remote resource identified by name with the given
   * arguments. The intent of this method is to invoke an arbitrary operation on an MBean located in
   * the remote MBeanServer.
   *
   * @param resourceName name/url (object name) of the remote resource (MBea) on which operation is
   *        to be invoked.
   * @param operationName name of the operation to be invoked.
   * @param params an array of arguments for the parameters to be set when the operation is invoked.
   * @param signatures an array containing the signature of the operation.
   * @return result of the operation invocation.
   * @throws MBeanAccessException if an MBean access error occurs.
   */
  @Override
  public Object invoke(final String resourceName, final String operationName, final Object[] params,
      final String[] signatures) {
    final URI link = HttpRequester.createURI(baseUrl, "/mbean/operation");

    MultiValueMap<String, Object> content = new LinkedMultiValueMap<>();

    content.add("resourceName", resourceName);
    content.add("operationName", operationName);
    if (params != null) {
      for (Object param : params) {
        content.add("parameters", param);
      }
    }
    if (signatures != null) {
      for (String signature : signatures) {
        content.add("signature", signature);
      }
    }

    try {
      return IOUtils.deserializeObject(
          httpRequester.post(link, content, byte[].class));
    } catch (IOException e) {
      throw new MBeanAccessException(String.format(
          "De-serializing the result from invoking operation (%1$s) on MBean (%2$s) failed!",
          resourceName, operationName), e);
    } catch (ClassNotFoundException e) {
      throw new MBeanAccessException(String.format(
          "The Class type of the result from invoking operation (%1$s) on MBean (%2$s) was not found!",
          resourceName, operationName), e);
    }

  }

  /**
   * This method searches the MBean server, based on the OperationsInvoker's JMX-based or remoting
   * capable MBean server connection, for MBeans matching a specific ObjectName or matching an
   * ObjectName pattern along with satisfying criteria from the Query expression.
   *
   * @param objectName the ObjectName or pattern for which matching MBeans in the target MBean
   *        server will be returned.
   * @param queryExpression the JMX-based query expression used to filter matching MBeans.
   * @return a set of ObjectName's matching MBeans in the MBean server matching the ObjectName and
   *         Query expression criteria.
   */
  @Override
  @SuppressWarnings("unchecked")
  public Set<ObjectName> queryNames(final ObjectName objectName, final QueryExp queryExpression) {
    final URI link = HttpRequester.createURI(baseUrl, "/mbean/query");

    Object content = new QueryParameterSource(objectName, queryExpression);
    try {
      return (Set<ObjectName>) IOUtils
          .deserializeObject(httpRequester.post(link, content, byte[].class));
    } catch (Exception e) {
      throw new MBeanAccessException(String.format(
          "An error occurred while querying for MBean names using ObjectName pattern (%1$s) and Query expression (%2$s)!",
          objectName, queryExpression), e);
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
    httpRequester = null;
    connected = false;
  }

  @Override
  public String toString() {
    return String.format("GemFire Manager HTTP service @ %1$s", baseUrl);
  }

  @Override
  public String getRemoteVersion() {
    final URI link = HttpRequester.createURI(baseUrl, "/version/release");
    return httpRequester.get(link, String.class);
  }

  @Override
  public String getRemoteGeodeSerializationVersion() {
    final URI link = HttpRequester.createURI(baseUrl, "/version/geodeSerializationVersion");
    return httpRequester.get(link, String.class);
  }

  /**
   * Processes the requested command. Sends the command to the GemFire Manager for remote processing
   * (execution).
   *
   * @param command the command requested/entered by the user to be processed.
   * @return the result of the command execution, either a json string of ResultModel or a Path
   */
  @Override
  public Object processCommand(final CommandRequest command) {
    URI link =
        HttpRequester.createURI(baseUrl, COMMANDS_URI, CMD_QUERY_PARAMETER, command.getUserInput());
    if (command.hasFileList()) {
      MultiValueMap<String, Object> content = new LinkedMultiValueMap<String, Object>();

      for (File file : command.getFileList()) {
        content.add(RESOURCES_REQUEST_PARAMETER, new FileSystemResource(file));
      }
      return httpRequester.post(link, content, String.class);
    }

    // when no file data to upload, this handles file download over http
    return httpRequester.executeWithResponseExtractor(link);
  }
}
