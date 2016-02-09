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
package com.gemstone.gemfire.management.internal.web.controllers;

import static org.junit.Assert.*;

import java.lang.reflect.Method;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

import com.gemstone.gemfire.management.cli.CliMetaData;
import com.gemstone.gemfire.management.internal.cli.util.ClasspathScanLoadHelper;
import com.gemstone.gemfire.management.internal.web.domain.Link;
import com.gemstone.gemfire.management.internal.web.domain.LinkIndex;
import com.gemstone.gemfire.management.internal.web.util.UriUtils;
import com.gemstone.gemfire.test.junit.categories.UnitTest;

import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.springframework.mock.web.MockHttpServletRequest;
import org.springframework.shell.core.CommandMarker;
import org.springframework.shell.core.annotation.CliCommand;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.context.request.RequestAttributes;
import org.springframework.web.context.request.RequestContextHolder;
import org.springframework.web.context.request.ServletRequestAttributes;

import javax.servlet.http.HttpServletRequest;

/**
 * The ShellCommandsControllerJUnitTest class is a test suite of test cases testing the contract and functionality of the
 * ShellCommandsController class, and specifically ensuring that all GemFire Gfsh commands have a corresponding
 * Management REST API call and web service endpoint in the GemFire Management REST Interface.
 * <p/>
 * @author John Blum
 * @see org.junit.Test
 * @see com.gemstone.gemfire.management.internal.web.controllers.ShellCommandsController
 * @since 8.0
 */
@Category(UnitTest.class)
public class ShellCommandsControllerJUnitTest {

  private static ShellCommandsController controller;

  @BeforeClass
  public static void setupBeforeClass() {
    controller = new ShellCommandsController();
    MockHttpServletRequest request = new MockHttpServletRequest();
    request.setContextPath("gemfire");
    RequestContextHolder.setRequestAttributes(new ServletRequestAttributes(request));
  }

  @AfterClass
  public static void tearDownAfterClass() {
    controller = null;
  }

  protected List<String> getCliCommands() {
    try {
      Set<Class<?>> commandClasses = ClasspathScanLoadHelper.loadAndGet(
        "com.gemstone.gemfire.management.internal.cli.commands", CommandMarker.class, true);

      List<String> commands = new ArrayList<>(commandClasses.size());

      for (Class<?> commandClass : commandClasses) {
        for (Method method : commandClass.getMethods()) {
          if (method.isAnnotationPresent(CliCommand.class)) {
            if (!(method.isAnnotationPresent(CliMetaData.class)
              && method.getAnnotation(CliMetaData.class).shellOnly()))
            {
              CliCommand commandAnnotation = method.getAnnotation(CliCommand.class);
              commands.addAll(Arrays.asList(commandAnnotation.value()));
            }
          }
        }
      }

      return commands;
    }
    catch (Exception e) {
      throw new RuntimeException(e);
    }
  }

  protected List<String> getControllerWebServiceEndpoints() {
    RequestAttributes requestAttrs = RequestContextHolder.getRequestAttributes();
    HttpServletRequest servletRequest = ((ServletRequestAttributes) requestAttrs).getRequest();
    String scheme = servletRequest.getScheme();

    try {
      Set<Class<?>> controllerClasses = ClasspathScanLoadHelper.loadAndGet(
        "com.gemstone.gemfire.management.internal.web.controllers", AbstractCommandsController.class, true);

      List<String> controllerWebServiceEndpoints = new ArrayList<>(controllerClasses.size());

      for (Class<?> controllerClass : controllerClasses) {
        if (!AbstractCommandsController.class.equals(controllerClass)) {
          for (Method method : controllerClass.getMethods()) {
            if (method.isAnnotationPresent(RequestMapping.class)) {
              RequestMapping requestMappingAnnotation = method.getAnnotation(RequestMapping.class);

              String webServiceEndpoint = String.format("%1$s %2$s", requestMappingAnnotation.method()[0],
                UriUtils.decode(controller.toUri(requestMappingAnnotation.value()[0], scheme).toString()));

              String[] requestParameters = requestMappingAnnotation.params();

              if (requestParameters.length > 0) {
                webServiceEndpoint += "?".concat(
                  com.gemstone.gemfire.internal.lang.StringUtils.concat(requestParameters, "&amp;"));
              }

              controllerWebServiceEndpoints.add(webServiceEndpoint);
            }
          }
        }
      }

      return controllerWebServiceEndpoints;
    }
    catch (Exception e) {
      throw new RuntimeException(e);
    }
  }

  @Test
  public void testUniqueIndex() {
    LinkIndex linkIndex = controller.index("https");

    List<String> conflicts = new ArrayList<>();
    Map<String, String> uriRelationMapping = new HashMap<>(linkIndex.size());

    for (Link link : linkIndex) {
      if (uriRelationMapping.containsKey(link.toHttpRequestLine())) {
        conflicts.add(String.format("REST API endpoint (%1$s) for (%2$s) conflicts with the REST API endpoint for (%3$s)",
          link.toHttpRequestLine(), link.getRelation(), uriRelationMapping.get(link.toHttpRequestLine())));
      }
      else {
        uriRelationMapping.put(link.toHttpRequestLine(), link.getRelation());
      }
    }

    assertTrue(String.format("Conflicts: %1$s!", conflicts), conflicts.isEmpty());
  }

  @Test
  public void testIndex() {
    List<String> commands = getCliCommands();

    assertNotNull(commands);
    assertFalse(commands.isEmpty());

    LinkIndex linkIndex = controller.index("https");

    assertNotNull(linkIndex);
    assertFalse(linkIndex.isEmpty());

    List<String> linkCommands = new ArrayList<>(linkIndex.size());

    for (Link link : linkIndex) {
      linkCommands.add(link.getRelation());
    }

    assertEquals(linkIndex.size(), linkCommands.size());

    List<String> missingLinkCommands = new ArrayList<>(commands);

    missingLinkCommands.removeAll(linkCommands);

    assertTrue(String.format(
      "The GemFire Management REST API Link Index is missing Link(s) for the following command(s): %1$s",
        missingLinkCommands), missingLinkCommands.isEmpty());
  }

  @Test
  public void testCommandHasRestApiControllerWebServiceEndpoint() {
    List<String> controllerWebServiceEndpoints = getControllerWebServiceEndpoints();

    assertNotNull(controllerWebServiceEndpoints);
    assertFalse(controllerWebServiceEndpoints.isEmpty());

    LinkIndex linkIndex = controller.index("http");

    assertNotNull(linkIndex);
    assertFalse(linkIndex.isEmpty());

    List<String> linkWebServiceEndpoints = new ArrayList<>(linkIndex.size());

    for (Link link : linkIndex) {
      linkWebServiceEndpoints.add(link.toHttpRequestLine());
    }

    assertEquals(linkIndex.size(), linkWebServiceEndpoints.size());

    List<String> missingControllerWebServiceEndpoints = new ArrayList<>(linkWebServiceEndpoints);

    missingControllerWebServiceEndpoints.removeAll(controllerWebServiceEndpoints);

    assertTrue(String.format(
        "The Management REST API Web Service Controllers in (%1$s) are missing the following REST API Web Service Endpoint(s): %2$s!",
        getClass().getPackage().getName(), missingControllerWebServiceEndpoints), missingControllerWebServiceEndpoints.isEmpty());
  }

  @Test
  public void testIndexUrisHaveCorrectScheme() {
    String versionCmd = "version";
    List<String> controllerWebServiceEndpoints = getControllerWebServiceEndpoints();

    assertNotNull(controllerWebServiceEndpoints);
    assertFalse(controllerWebServiceEndpoints.isEmpty());

    String testScheme = "xyz";
    LinkIndex linkIndex = controller.index(testScheme);

    assertNotNull(linkIndex);
    assertFalse(linkIndex.isEmpty());

    assertTrue(String.format("Link does not have correct scheme %1$s", linkIndex.find(versionCmd)),
        testScheme.equals(linkIndex.find(versionCmd).getHref().getScheme()));
  }
}
