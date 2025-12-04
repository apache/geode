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
package org.apache.geode.management.internal.cli;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import java.lang.reflect.Method;

import org.junit.Before;
import org.junit.Test;
import org.springframework.shell.standard.ShellOption;

import org.apache.geode.management.internal.cli.result.model.ResultModel;

/**
 * Tests for {@link MandatoryParameterValidationInterceptor}.
 *
 * Spring Shell 3.x migration: This interceptor validates mandatory parameters at execution time
 * because Shell 3.x removed built-in parse-time validation for required parameters.
 * Shell 1.x validated @CliOption(mandatory=true) during parsing, but Shell 3.x @ShellOption
 * only supports optional parameters with defaultValue - parameters without a defaultValue
 * need runtime validation to provide proper error messages.
 */
public class MandatoryParameterValidationInterceptorTest {

  private MandatoryParameterValidationInterceptor interceptor;
  private GfshParseResult parseResult;

  @Before
  public void setUp() {
    interceptor = new MandatoryParameterValidationInterceptor();
    parseResult = mock(GfshParseResult.class);
  }

  @Test
  public void testValidationPassesWithNonNullRequiredParameter() throws Exception {
    // Setup: method with required parameter (no defaultValue)
    Method method = TestCommands.class.getMethod("commandWithRequiredParam", String.class);
    when(parseResult.getMethod()).thenReturn(method);
    when(parseResult.getArguments()).thenReturn(new Object[] {"value"});

    // Execute
    ResultModel result = interceptor.preExecution(parseResult);

    // Verify: validation passes (empty info message)
    assertThat(result.getStatus()).isEqualTo(org.apache.geode.management.cli.Result.Status.OK);
  }

  @Test
  public void testValidationFailsWithNullRequiredParameter() throws Exception {
    // Setup: method with required parameter (no defaultValue)
    Method method = TestCommands.class.getMethod("commandWithRequiredParam", String.class);
    when(parseResult.getMethod()).thenReturn(method);
    when(parseResult.getArguments()).thenReturn(new Object[] {null});

    // Execute
    ResultModel result = interceptor.preExecution(parseResult);

    // Verify: returns "Invalid command"
    assertThat(result.getStatus()).isEqualTo(org.apache.geode.management.cli.Result.Status.ERROR);
    assertThat(result.toString()).contains("Invalid command");
  }

  @Test
  public void testValidationFailsWithEmptyStringRequiredParameter() throws Exception {
    // Setup: method with required String parameter
    Method method = TestCommands.class.getMethod("commandWithRequiredParam", String.class);
    when(parseResult.getMethod()).thenReturn(method);
    when(parseResult.getArguments()).thenReturn(new Object[] {""});

    // Execute
    ResultModel result = interceptor.preExecution(parseResult);

    // Verify: returns "Invalid command" (empty string = missing)
    assertThat(result.getStatus()).isEqualTo(org.apache.geode.management.cli.Result.Status.ERROR);
    assertThat(result.toString()).contains("Invalid command");
  }

  @Test
  public void testValidationPassesWithNullOptionalParameter() throws Exception {
    // Setup: method with optional parameter (has defaultValue)
    Method method =
        TestCommands.class.getMethod("commandWithOptionalParam", String.class);
    when(parseResult.getMethod()).thenReturn(method);
    when(parseResult.getArguments()).thenReturn(new Object[] {null});

    // Execute
    ResultModel result = interceptor.preExecution(parseResult);

    // Verify: validation passes (optional parameter can be null)
    assertThat(result.getStatus()).isEqualTo(org.apache.geode.management.cli.Result.Status.OK);
  }

  @Test
  public void testValidationPassesWithNonNullOptionalParameter() throws Exception {
    // Setup: method with optional parameter
    Method method =
        TestCommands.class.getMethod("commandWithOptionalParam", String.class);
    when(parseResult.getMethod()).thenReturn(method);
    when(parseResult.getArguments()).thenReturn(new Object[] {"value"});

    // Execute
    ResultModel result = interceptor.preExecution(parseResult);

    // Verify: validation passes
    assertThat(result.getStatus()).isEqualTo(org.apache.geode.management.cli.Result.Status.OK);
  }

  @Test
  public void testValidationPassesWithMultipleValidParameters() throws Exception {
    // Setup: method with multiple parameters (1 required, 1 optional)
    Method method = TestCommands.class.getMethod("commandWithMixedParams", String.class,
        Integer.class);
    when(parseResult.getMethod()).thenReturn(method);
    when(parseResult.getArguments()).thenReturn(new Object[] {"required-value", null});

    // Execute
    ResultModel result = interceptor.preExecution(parseResult);

    // Verify: validation passes (required param has value, optional can be null)
    assertThat(result.getStatus()).isEqualTo(org.apache.geode.management.cli.Result.Status.OK);
  }

  @Test
  public void testValidationFailsWithNullInMixedParameters() throws Exception {
    // Setup: method with multiple parameters (1 required, 1 optional)
    Method method = TestCommands.class.getMethod("commandWithMixedParams", String.class,
        Integer.class);
    when(parseResult.getMethod()).thenReturn(method);
    when(parseResult.getArguments()).thenReturn(new Object[] {null, 100});

    // Execute
    ResultModel result = interceptor.preExecution(parseResult);

    // Verify: validation fails (required param is null)
    assertThat(result.getStatus()).isEqualTo(org.apache.geode.management.cli.Result.Status.ERROR);
    assertThat(result.toString()).contains("Invalid command");
  }

  @Test
  public void testValidationPassesWithNullMarkerDefaultValue() throws Exception {
    // Setup: method with ShellOption.NULL as defaultValue (still required)
    Method method = TestCommands.class.getMethod("commandWithNullDefaultValue", String.class);
    when(parseResult.getMethod()).thenReturn(method);
    when(parseResult.getArguments()).thenReturn(new Object[] {null});

    // Execute
    ResultModel result = interceptor.preExecution(parseResult);

    // Verify: validation fails (ShellOption.NULL means required)
    assertThat(result.getStatus()).isEqualTo(org.apache.geode.management.cli.Result.Status.ERROR);
    assertThat(result.toString()).contains("Invalid command");
  }

  @Test
  public void testValidationPassesWithNoShellOptionParameters() throws Exception {
    // Setup: method with no @ShellOption annotations
    Method method = TestCommands.class.getMethod("commandWithNoAnnotations", String.class);
    when(parseResult.getMethod()).thenReturn(method);
    when(parseResult.getArguments()).thenReturn(new Object[] {null});

    // Execute
    ResultModel result = interceptor.preExecution(parseResult);

    // Verify: validation passes (no @ShellOption = not validated)
    assertThat(result.getStatus()).isEqualTo(org.apache.geode.management.cli.Result.Status.OK);
  }

  /**
   * Test command class with various parameter configurations
   */
  static class TestCommands {

    // Required parameter (no defaultValue)
    public void commandWithRequiredParam(@ShellOption("param") String param) {}

    // Optional parameter (has defaultValue)
    public void commandWithOptionalParam(
        @ShellOption(value = "param", defaultValue = "default") String param) {}

    // Mixed parameters
    public void commandWithMixedParams(@ShellOption("required") String required,
        @ShellOption(value = "optional", defaultValue = "0") Integer optional) {}

    // Parameter with ShellOption.NULL defaultValue (still required)
    public void commandWithNullDefaultValue(
        @ShellOption(value = "param", defaultValue = ShellOption.NULL) String param) {}

    // No @ShellOption annotation
    public void commandWithNoAnnotations(String param) {}
  }
}
