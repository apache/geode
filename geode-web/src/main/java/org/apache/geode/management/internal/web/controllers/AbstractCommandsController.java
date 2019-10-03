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
package org.apache.geode.management.internal.web.controllers;

import java.io.File;
import java.io.IOException;
import java.io.PrintWriter;
import java.io.StringWriter;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import javax.management.InstanceNotFoundException;
import javax.management.JMX;
import javax.management.MBeanServer;
import javax.management.MalformedObjectNameException;
import javax.management.ObjectName;

import org.apache.commons.io.FileUtils;
import org.apache.logging.log4j.Logger;
import org.springframework.beans.propertyeditors.StringArrayPropertyEditor;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.WebDataBinder;
import org.springframework.web.bind.annotation.ExceptionHandler;
import org.springframework.web.bind.annotation.InitBinder;
import org.springframework.web.multipart.MultipartFile;

import org.apache.geode.internal.cache.GemFireCacheImpl;
import org.apache.geode.internal.cache.InternalCache;
import org.apache.geode.logging.internal.LogService;
import org.apache.geode.management.DistributedSystemMXBean;
import org.apache.geode.management.ManagementService;
import org.apache.geode.management.MemberMXBean;
import org.apache.geode.management.internal.MBeanJMXAdapter;
import org.apache.geode.management.internal.ManagementAgent;
import org.apache.geode.management.internal.SystemManagementService;
import org.apache.geode.management.internal.beans.FileUploader;
import org.apache.geode.management.internal.cli.shell.Gfsh;
import org.apache.geode.management.internal.web.controllers.support.LoginHandlerInterceptor;
import org.apache.geode.management.internal.web.util.UriUtils;
import org.apache.geode.security.AuthenticationFailedException;
import org.apache.geode.security.NotAuthorizedException;

/**
 * The AbstractCommandsController class is the abstract base class encapsulating common
 * functionality across all Management Controller classes that expose REST API web service endpoints
 * (URLs/URIs) for GemFire shell (Gfsh) commands.
 *
 * @see org.apache.geode.management.internal.cli.shell.Gfsh
 * @see org.springframework.http.ResponseEntity
 * @see org.springframework.stereotype.Controller
 * @see org.springframework.web.bind.annotation.ExceptionHandler
 * @see org.springframework.web.bind.annotation.InitBinder
 * @see org.springframework.web.bind.annotation.ResponseBody
 * @since GemFire 8.0
 */
@SuppressWarnings("unused")
public abstract class AbstractCommandsController {
  private static final Logger logger = LogService.getLogger();

  protected static final String DEFAULT_ENCODING = UriUtils.DEFAULT_ENCODING;
  protected static final String REST_API_VERSION = "/v1";

  private MemberMXBean managingMemberMXBeanProxy;

  private Class accessControlKlass;

  @ExceptionHandler(Exception.class)
  public ResponseEntity<String> internalError(final Exception e) {
    final String stackTrace = getPrintableStackTrace(e);
    logger.fatal(stackTrace);
    return new ResponseEntity<>(stackTrace, HttpStatus.INTERNAL_SERVER_ERROR);
  }

  @ExceptionHandler(AuthenticationFailedException.class)
  public ResponseEntity<String> unauthorized(AuthenticationFailedException e) {
    return new ResponseEntity<>(e.getMessage(), HttpStatus.UNAUTHORIZED);
  }

  @ExceptionHandler({NotAuthorizedException.class, java.lang.SecurityException.class})
  public ResponseEntity<String> forbidden(Exception e) {
    return new ResponseEntity<>(e.getMessage(), HttpStatus.FORBIDDEN);
  }

  @ExceptionHandler(MalformedObjectNameException.class)
  public ResponseEntity<String> badRequest(final MalformedObjectNameException e) {
    logger.info(e);
    return new ResponseEntity<>(getPrintableStackTrace(e), HttpStatus.BAD_REQUEST);
  }

  @ExceptionHandler(InstanceNotFoundException.class)
  public ResponseEntity<String> notFound(final InstanceNotFoundException e) {
    logger.info(e);
    return new ResponseEntity<>(getPrintableStackTrace(e), HttpStatus.NOT_FOUND);
  }


  /**
   * Writes the stack trace of the Throwable to a String.
   *
   * @param t a Throwable object who's stack trace will be written to a String.
   * @return a String containing the stack trace of the Throwable.
   * @see java.io.StringWriter
   * @see java.lang.Throwable#printStackTrace(java.io.PrintWriter)
   */
  private static String getPrintableStackTrace(final Throwable t) {
    final StringWriter stackTraceWriter = new StringWriter();
    t.printStackTrace(new PrintWriter(stackTraceWriter));
    return stackTraceWriter.toString();
  }

  /**
   * Initializes data bindings for various HTTP request handler method parameter Java class types.
   *
   * @param dataBinder the DataBinder implementation used for Web transactions.
   * @see org.springframework.web.bind.WebDataBinder
   * @see org.springframework.web.bind.annotation.InitBinder
   */
  @InitBinder
  public void initBinder(final WebDataBinder dataBinder) {
    dataBinder.registerCustomEditor(String[].class,
        new StringArrayPropertyEditor(StringArrayPropertyEditor.DEFAULT_SEPARATOR, false));
  }


  /**
   * Gets a reference to the platform MBeanServer running in this JVM process. The MBeanServer
   * instance constitutes a connection to the MBeanServer. This method returns a security-wrapped
   * MBean if integrated security is active.
   *
   * @return a reference to the platform MBeanServer for this JVM process.
   * @see java.lang.management.ManagementFactory#getPlatformMBeanServer()
   * @see javax.management.MBeanServer
   */
  protected MBeanServer getMBeanServer() {
    InternalCache cache = getInternalCache();
    SystemManagementService service =
        (SystemManagementService) ManagementService.getExistingManagementService(cache);
    ManagementAgent managementAgent = service.getManagementAgent();
    return managementAgent.getJmxConnectorServer().getMBeanServer();
  }

  @SuppressWarnings("deprecation")
  private InternalCache getInternalCache() {
    return GemFireCacheImpl.getInstance();
  }

  /**
   * Lookup operation for the MemberMXBean representing the Manager in the GemFire cluster. This
   * method gets an instance of the Platform MBeanServer for this JVM process and uses it to lookup
   * the MemberMXBean for the GemFire Manager based on the ObjectName declared in the
   * DistributedSystemMXBean.getManagerObjectName() operation.
   *
   * @return a proxy instance to the MemberMXBean of the GemFire Manager.
   * @see #getMBeanServer()
   * @see #createMemberMXBeanForManagerUsingProxy(javax.management.MBeanServer,
   *      javax.management.ObjectName)
   */
  private synchronized MemberMXBean getManagingMemberMXBean() {
    if (managingMemberMXBeanProxy == null) {
      MBeanServer mbs = getMBeanServer();
      DistributedSystemMXBean distributedSystemMXBean = JMX.newMXBeanProxy(mbs,
          MBeanJMXAdapter.getDistributedSystemName(), DistributedSystemMXBean.class);
      managingMemberMXBeanProxy = createMemberMXBeanForManagerUsingProxy(mbs,
          distributedSystemMXBean.getMemberObjectName());
    }

    return managingMemberMXBeanProxy;
  }

  protected synchronized ObjectName getMemberObjectName() {
    MBeanServer platformMBeanServer = getMBeanServer();
    DistributedSystemMXBean distributedSystemMXBean = JMX.newMXBeanProxy(platformMBeanServer,
        MBeanJMXAdapter.getDistributedSystemName(), DistributedSystemMXBean.class);
    return distributedSystemMXBean.getMemberObjectName();
  }

  /**
   * Creates a Proxy using the Platform MBeanServer and ObjectName in order to access attributes and
   * invoke operations on the GemFire Manager's MemberMXBean.
   *
   * @param server a reference to this JVM's Platform MBeanServer.
   * @param managingMemberObjectName the ObjectName of the GemFire Manager's MemberMXBean registered
   *        in the Platform MBeanServer.
   * @return a Proxy for accessing attributes and invoking operations on the GemFire Manager's
   *         MemberMXBean.
   * @see javax.management.JMX#newMXBeanProxy(javax.management.MBeanServerConnection,
   *      javax.management.ObjectName, Class)
   */
  private MemberMXBean createMemberMXBeanForManagerUsingProxy(final MBeanServer server,
      final ObjectName managingMemberObjectName) {
    return JMX.newMXBeanProxy(server, managingMemberObjectName, MemberMXBean.class);
  }

  /**
   * Gets the environment setup during this HTTP/command request for the current command process
   * execution.
   *
   * @return a mapping of environment variables to values.
   * @see LoginHandlerInterceptor#getEnvironment()
   */
  protected Map<String, String> getEnvironment() {
    final Map<String, String> environment = new HashMap<>();

    environment.putAll(LoginHandlerInterceptor.getEnvironment());
    environment.put(Gfsh.ENV_APP_NAME, Gfsh.GFSH_APP_NAME);

    return environment;
  }

  /**
   * Executes the specified command as entered by the user using the GemFire Shell (Gfsh). Note,
   * Gfsh performs validation of the command during parsing before sending the command to the
   * Manager for processing.
   *
   * @param command a String value containing a valid command String as would be entered by the user
   *        in Gfsh.
   * @param environment a Map containing any environment configuration settings to be used by the
   *        Manager during command execution. For example, when executing commands originating from
   *        Gfsh, the key/value pair (APP_NAME=gfsh) is a specified mapping in the "environment.
   *        Note, it is common for the REST API to act as a bridge, or an adapter between Gfsh and
   *        the Manager, and thus need to specify this key/value pair mapping.
   * @param multipartFiles uploaded files
   * @return a result of the command execution as a String, typically marshalled in JSON to be
   *         serialized back to Gfsh.
   */
  protected String processCommand(final String command, final Map<String, String> environment,
      final MultipartFile[] multipartFiles) throws IOException {
    List<String> filePaths = null;
    Path tempDir = null;
    if (multipartFiles != null) {
      tempDir = FileUploader.createSecuredTempDirectory("uploaded-");
      // staging the files to local
      filePaths = new ArrayList<>();
      for (MultipartFile multipartFile : multipartFiles) {
        File dest = new File(tempDir.toFile(), multipartFile.getOriginalFilename());
        multipartFile.transferTo(dest);
        filePaths.add(dest.getAbsolutePath());
      }
    }

    MemberMXBean manager = getManagingMemberMXBean();
    try {
      return manager.processCommand(command, environment, filePaths);
    } finally {
      if (tempDir != null) {
        FileUtils.deleteDirectory(tempDir.toFile());
      }
    }
  }
}
