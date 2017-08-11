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

import static org.apache.geode.management.internal.web.controllers.AbstractMultiPartCommandsController.RESOURCES_REQUEST_PARAMETER;

import java.io.FileInputStream;
import java.io.IOException;
import java.nio.file.Path;
import java.util.Set;

import javax.management.AttributeNotFoundException;
import javax.management.InstanceNotFoundException;
import javax.management.MalformedObjectNameException;
import javax.management.ObjectName;

import org.apache.commons.io.FileUtils;
import org.apache.commons.lang.ArrayUtils;
import org.springframework.core.io.InputStreamResource;
import org.springframework.http.HttpHeaders;
import org.springframework.http.HttpStatus;
import org.springframework.http.MediaType;
import org.springframework.http.ResponseEntity;
import org.springframework.stereotype.Controller;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestMethod;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.ResponseBody;
import org.springframework.web.multipart.MultipartFile;

import org.apache.geode.internal.GemFireVersion;
import org.apache.geode.internal.lang.ObjectUtils;
import org.apache.geode.internal.util.IOUtils;
import org.apache.geode.management.cli.Result;
import org.apache.geode.management.internal.cli.result.CommandResult;
import org.apache.geode.management.internal.cli.result.ResultBuilder;
import org.apache.geode.management.internal.web.domain.QueryParameterSource;
import org.apache.geode.management.internal.web.util.ConvertUtils;

/**
 * The ShellCommandsController class implements GemFire REST API calls for Gfsh Shell Commands.
 * 
 * @see org.apache.geode.management.internal.cli.commands.ConnectCommand
 * @see org.apache.geode.management.internal.cli.commands.DebugCommand
 * @see org.apache.geode.management.internal.cli.commands.DescribeConnectionCommand
 * @see org.apache.geode.management.internal.cli.commands.DisconnectCommand
 * @see org.apache.geode.management.internal.cli.commands.EchoCommand
 * @see org.apache.geode.management.internal.cli.commands.ExecuteScriptCommand
 * @see org.apache.geode.management.internal.cli.commands.ExitCommand
 * @see org.apache.geode.management.internal.cli.commands.HistoryCommand
 * @see org.apache.geode.management.internal.cli.commands.SetVariableCommand
 * @see org.apache.geode.management.internal.cli.commands.ShCommand
 * @see org.apache.geode.management.internal.cli.commands.SleepCommand
 * @see org.apache.geode.management.internal.cli.commands.VersionCommand
 * @see org.apache.geode.management.internal.web.controllers.AbstractCommandsController
 * @see org.springframework.stereotype.Controller
 * @see org.springframework.web.bind.annotation.RequestBody
 * @see org.springframework.web.bind.annotation.RequestMapping
 * @see org.springframework.web.bind.annotation.RequestMethod
 * @see org.springframework.web.bind.annotation.RequestParam
 * @see org.springframework.web.bind.annotation.ResponseBody
 * @since GemFire 8.0
 */
@Controller("shellController")
@RequestMapping(AbstractCommandsController.REST_API_VERSION)
@SuppressWarnings("unused")
public class ShellCommandsController extends AbstractCommandsController {

  protected static final String MBEAN_ATTRIBUTE_LINK_RELATION = "mbean-attribute";
  protected static final String MBEAN_OPERATION_LINK_RELATION = "mbean-operation";
  protected static final String MBEAN_QUERY_LINK_RELATION = "mbean-query";
  protected static final String PING_LINK_RELATION = "ping";

  @RequestMapping(method = {RequestMethod.GET, RequestMethod.POST}, value = "/management/commands")
  public ResponseEntity<InputStreamResource> command(@RequestParam(value = "cmd") String command,
      @RequestParam(value = RESOURCES_REQUEST_PARAMETER,
          required = false) MultipartFile[] fileResource)
      throws IOException {
    String result =
        processCommand(decode(command), getEnvironment(), ConvertUtils.convert(fileResource));
    return getResponse(result);
  }

  ResponseEntity<InputStreamResource> getResponse(String result) {
    // the result is json string from CommandResult
    CommandResult commandResult = ResultBuilder.fromJson(result);

    if (commandResult.getStatus().equals(Result.Status.OK) && commandResult.hasFileToDownload()) {
      return getFileDownloadResponse(commandResult);
    } else {
      return getJsonResponse(result);
    }
  }

  private ResponseEntity<InputStreamResource> getJsonResponse(String result) {
    HttpHeaders respHeaders = new HttpHeaders();
    InputStreamResource isr;// if the command is successful, the output is the filepath,
    // else we need to send the orignal result back so that the receiver will know to turn it
    // into a Result object
    try {
      isr = new InputStreamResource(org.apache.commons.io.IOUtils.toInputStream(result, "UTF-8"));
      respHeaders.set(HttpHeaders.CONTENT_TYPE, MediaType.APPLICATION_JSON_VALUE);
      return new ResponseEntity<>(isr, respHeaders, HttpStatus.OK);
    } catch (Exception e) {
      throw new RuntimeException("IO Error writing file to output stream", e);
    }
  }

  private ResponseEntity<InputStreamResource> getFileDownloadResponse(CommandResult commandResult) {
    HttpHeaders respHeaders = new HttpHeaders();
    InputStreamResource isr;// if the command is successful, the output is the filepath,

    Path filePath = commandResult.getFileToDownload();
    try {
      isr = new InputStreamResource(new FileInputStream(filePath.toFile()));
      respHeaders.set(HttpHeaders.CONTENT_TYPE, MediaType.APPLICATION_OCTET_STREAM_VALUE);
      return new ResponseEntity<>(isr, respHeaders, HttpStatus.OK);
    } catch (Exception e) {
      throw new RuntimeException("IO Error writing file to output stream", e);
    } finally {
      FileUtils.deleteQuietly(filePath.toFile());
    }
  }

  @RequestMapping(method = RequestMethod.GET, value = "/mbean/attribute")
  public ResponseEntity<?> getAttribute(@RequestParam("resourceName") final String resourceName,
      @RequestParam("attributeName") final String attributeName) {
    try {
      final Object attributeValue = getMBeanServer()
          .getAttribute(ObjectName.getInstance(decode(resourceName)), decode(attributeName));

      return new ResponseEntity<>(IOUtils.serializeObject(attributeValue), HttpStatus.OK);
    } catch (AttributeNotFoundException | MalformedObjectNameException e) {
      return new ResponseEntity<>(printStackTrace(e), HttpStatus.BAD_REQUEST);
    } catch (InstanceNotFoundException e) {
      return new ResponseEntity<>(printStackTrace(e), HttpStatus.NOT_FOUND);
    } catch (Exception e) {
      return new ResponseEntity<>(printStackTrace(e), HttpStatus.INTERNAL_SERVER_ERROR);
    }
  }

  @RequestMapping(method = RequestMethod.POST, value = "/mbean/operation")
  public ResponseEntity<?> invoke(@RequestParam("resourceName") final String resourceName,
      @RequestParam("operationName") final String operationName,
      @RequestParam(value = "signature", required = false) String[] signature,
      @RequestParam(value = "parameters", required = false) Object[] parameters) {
    signature = (signature != null ? signature : ArrayUtils.EMPTY_STRING_ARRAY);
    parameters = (parameters != null ? parameters : ObjectUtils.EMPTY_OBJECT_ARRAY);

    try {
      final Object result = getMBeanServer().invoke(ObjectName.getInstance(decode(resourceName)),
          decode(operationName), parameters, signature);

      return new ResponseEntity<>(IOUtils.serializeObject(result), HttpStatus.OK);
    } catch (InstanceNotFoundException e) {
      return new ResponseEntity<>(printStackTrace(e), HttpStatus.NOT_FOUND);
    } catch (MalformedObjectNameException e) {
      return new ResponseEntity<>(printStackTrace(e), HttpStatus.BAD_REQUEST);
    } catch (Exception e) {
      return new ResponseEntity<>(printStackTrace(e), HttpStatus.INTERNAL_SERVER_ERROR);
    }
  }

  @RequestMapping(method = RequestMethod.POST, value = "/mbean/query")
  public ResponseEntity<?> queryNames(@RequestBody final QueryParameterSource query) {
    try {
      final Set<ObjectName> objectNames =
          getMBeanServer().queryNames(query.getObjectName(), query.getQueryExpression());

      return new ResponseEntity<>(IOUtils.serializeObject(objectNames), HttpStatus.OK);
    } catch (IOException e) {
      return new ResponseEntity<>(printStackTrace(e), HttpStatus.INTERNAL_SERVER_ERROR);
    }
  }

  @RequestMapping(method = {RequestMethod.GET, RequestMethod.HEAD}, value = "/ping")
  public ResponseEntity<String> ping() {
    return new ResponseEntity<>("pong", HttpStatus.OK);
  }

  @RequestMapping(method = RequestMethod.GET, value = "/version")
  @ResponseBody
  public String version() {
    return GemFireVersion.getProductName().concat("/").concat(GemFireVersion.getGemFireVersion());
  }

  @RequestMapping(method = RequestMethod.GET, value = "/version/full")
  @ResponseBody
  public String versionSimple() {
    return GemFireVersion.asString();
  }

}
