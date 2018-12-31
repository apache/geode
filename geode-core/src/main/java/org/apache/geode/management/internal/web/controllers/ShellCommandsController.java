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

import static org.apache.commons.io.IOUtils.toInputStream;
import static org.apache.geode.management.internal.web.util.UriUtils.decode;

import java.io.FileInputStream;
import java.io.IOException;
import java.nio.file.Path;
import java.util.Optional;
import java.util.Set;

import javax.management.AttributeNotFoundException;
import javax.management.InstanceNotFoundException;
import javax.management.MBeanException;
import javax.management.MBeanServer;
import javax.management.MalformedObjectNameException;
import javax.management.ObjectName;
import javax.management.ReflectionException;

import org.apache.commons.io.FileUtils;
import org.apache.commons.lang3.ArrayUtils;
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

import org.apache.geode.cache.RegionShortcut;
import org.apache.geode.internal.GemFireVersion;
import org.apache.geode.internal.util.IOUtils;
import org.apache.geode.management.cli.Result;
import org.apache.geode.management.internal.cli.i18n.CliStrings;
import org.apache.geode.management.internal.cli.result.CommandResult;
import org.apache.geode.management.internal.cli.result.ResultBuilder;
import org.apache.geode.management.internal.cli.util.CommandStringBuilder;
import org.apache.geode.management.internal.web.domain.QueryParameterSource;

/**
 * The ShellCommandsController class implements GemFire REST API calls for Gfsh Shell Commands.
 *
 * @see org.springframework.stereotype.Controller
 * @see org.springframework.web.bind.annotation.RequestBody
 * @see org.springframework.web.bind.annotation.RequestMapping
 * @see org.springframework.web.bind.annotation.RequestMethod
 * @see org.springframework.web.bind.annotation.RequestParam
 * @see org.springframework.web.bind.annotation.ResponseBody
 * @since GemFire 8.0
 */
@Controller("shellController")
@SuppressWarnings("unused")
public class ShellCommandsController extends AbstractCommandsController {

  private static final MultipartFile[] DEFAULT_MULTIPART_FILE = null;

  private static final String DEFAULT_INDEX_TYPE = "range";

  @RequestMapping(method = {RequestMethod.GET, RequestMethod.POST}, value = "/management/commands")
  public ResponseEntity<InputStreamResource> command(@RequestParam(value = "cmd") String command,
      @RequestParam(value = "resources", required = false) MultipartFile[] fileResource)
      throws IOException {
    String result = processCommand(decode(command), getEnvironment(), fileResource);
    return getResponse(result);
  }

  @RequestMapping(method = RequestMethod.POST, value = "/indexes")
  public ResponseEntity<?> createIndex(@RequestParam(CliStrings.CREATE_INDEX__NAME) String name,
      @RequestParam(CliStrings.CREATE_INDEX__EXPRESSION) String expression,
      @RequestParam(CliStrings.CREATE_INDEX__REGION) String region,
      @RequestParam(value = CliStrings.CREATE_INDEX__TYPE,
          defaultValue = DEFAULT_INDEX_TYPE) String type)
      throws IOException {

    CommandStringBuilder command = new CommandStringBuilder(CliStrings.CREATE_INDEX);

    command.addOption(CliStrings.CREATE_INDEX__NAME, name);
    command.addOption(CliStrings.CREATE_INDEX__EXPRESSION, expression);
    command.addOption(CliStrings.CREATE_INDEX__REGION, region);
    command.addOption(CliStrings.CREATE_INDEX__TYPE, type);

    return command(command.toString(), DEFAULT_MULTIPART_FILE);
  }

  @RequestMapping(method = RequestMethod.POST, value = "/regions")
  public ResponseEntity<?> createRegion(
      @RequestParam(CliStrings.CREATE_REGION__REGION) String namePath,
      @RequestParam(value = CliStrings.CREATE_REGION__SKIPIFEXISTS,
          defaultValue = "false") Boolean skipIfExists,
      @RequestParam(value = CliStrings.CREATE_REGION__REGIONSHORTCUT,
          defaultValue = "PARTITION") RegionShortcut type)
      throws IOException {

    CommandStringBuilder command = new CommandStringBuilder(CliStrings.CREATE_REGION);

    command.addOption(CliStrings.CREATE_REGION__REGION, namePath);
    command.addOption(CliStrings.CREATE_REGION__REGIONSHORTCUT, type.name());

    Optional.ofNullable(skipIfExists)
        .ifPresent(it -> command.addOption(CliStrings.CREATE_REGION__SKIPIFEXISTS, it.toString()));

    return command(command.toString(), DEFAULT_MULTIPART_FILE);
  }

  @RequestMapping(method = RequestMethod.GET, value = "/mbean/attribute")
  public ResponseEntity<?> getAttribute(@RequestParam("resourceName") final String resourceName,
      @RequestParam("attributeName") final String attributeName)
      throws AttributeNotFoundException, MBeanException, ReflectionException,
      InstanceNotFoundException, IOException, MalformedObjectNameException {
    // Exceptions are caught by the @ExceptionHandler AbstractCommandsController.handleAppException
    MBeanServer mBeanServer = getMBeanServer();
    ObjectName objectName = ObjectName.getInstance(decode(resourceName));
    final Object attributeValue = mBeanServer.getAttribute(objectName, decode(attributeName));
    byte[] serializedResult = IOUtils.serializeObject(attributeValue);
    return new ResponseEntity<>(serializedResult, HttpStatus.OK);
  }

  @RequestMapping(method = RequestMethod.POST, value = "/mbean/operation")
  public ResponseEntity<?> invoke(@RequestParam("resourceName") final String resourceName,
      @RequestParam("operationName") final String operationName,
      @RequestParam(value = "signature", required = false) String[] signature,
      @RequestParam(value = "parameters", required = false) Object[] parameters)
      throws MalformedObjectNameException, MBeanException, InstanceNotFoundException,
      ReflectionException, IOException {
    // Exceptions are caught by the @ExceptionHandler AbstractCommandsController.handleAppException
    signature = (signature != null ? signature : ArrayUtils.EMPTY_STRING_ARRAY);
    parameters = (parameters != null ? parameters : ArrayUtils.EMPTY_OBJECT_ARRAY);
    MBeanServer mBeanServer = getMBeanServer();
    ObjectName objectName = ObjectName.getInstance(decode(resourceName));
    final Object result =
        mBeanServer.invoke(objectName, decode(operationName), parameters, signature);
    byte[] serializedResult = IOUtils.serializeObject(result);
    return new ResponseEntity<>(serializedResult, HttpStatus.OK);
  }

  @RequestMapping(method = RequestMethod.POST, value = "/mbean/query")
  public ResponseEntity<?> queryNames(@RequestBody final QueryParameterSource query)
      throws IOException {
    // Exceptions are caught by the @ExceptionHandler AbstractCommandsController.handleAppException
    final Set<ObjectName> objectNames =
        getMBeanServer().queryNames(query.getObjectName(), query.getQueryExpression());
    return new ResponseEntity<>(IOUtils.serializeObject(objectNames), HttpStatus.OK);
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
  public String fullVersion() {
    return GemFireVersion.asString();
  }

  @RequestMapping(method = RequestMethod.GET, value = "/version/release")
  @ResponseBody
  public String releaseVersion() {
    return GemFireVersion.getGemFireVersion();
  }


  private ResponseEntity<InputStreamResource> getResponse(String result) {
    CommandResult commandResult = ResultBuilder.fromJson(result);
    if (commandResult.getStatus().equals(Result.Status.OK) && commandResult.hasFileToDownload()) {
      return getFileDownloadResponse(commandResult);
    } else {
      return getJsonResponse(result);
    }
  }

  private ResponseEntity<InputStreamResource> getJsonResponse(String result) {
    HttpHeaders respHeaders = new HttpHeaders();
    try {
      InputStreamResource isr = new InputStreamResource(toInputStream(result, "UTF-8"));
      respHeaders.set(HttpHeaders.CONTENT_TYPE, MediaType.APPLICATION_JSON_VALUE);
      return new ResponseEntity<>(isr, respHeaders, HttpStatus.OK);
    } catch (Exception e) {
      throw new RuntimeException("IO Error writing file to output stream", e);
    }
  }

  private ResponseEntity<InputStreamResource> getFileDownloadResponse(CommandResult commandResult) {
    HttpHeaders respHeaders = new HttpHeaders();
    Path filePath = commandResult.getFileToDownload();
    try {
      InputStreamResource isr = new InputStreamResource(new FileInputStream(filePath.toFile()));
      respHeaders.set(HttpHeaders.CONTENT_TYPE, MediaType.APPLICATION_OCTET_STREAM_VALUE);
      return new ResponseEntity<>(isr, respHeaders, HttpStatus.OK);
    } catch (Exception e) {
      throw new RuntimeException("IO Error writing file to output stream", e);
    } finally {
      FileUtils.deleteQuietly(filePath.toFile());
    }
  }
}
