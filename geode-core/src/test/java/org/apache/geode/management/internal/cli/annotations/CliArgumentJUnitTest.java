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
package com.gemstone.gemfire.management.internal.cli.annotations;

import static org.junit.Assert.*;

import org.junit.Test;
import org.junit.experimental.categories.Category;

import com.gemstone.gemfire.management.internal.cli.annotation.CliArgument;
import com.gemstone.gemfire.test.junit.categories.UnitTest;

/**
 * Includes tests for checking assignment of {@link CliArgument}
 */
@Category(UnitTest.class)
public class CliArgumentJUnitTest {

  private static final String ARGUMENT_NAME = "ARGUMENT_NAME";
  private static final String ARGUMENT_HELP = "ARGUMENT_HELP";
  private static final boolean ARGUMENT_MANDATORY = true;
  private static final String ARGUMENT_CONTEXT = "ARGUMENT_CONTEXT";
  private static final boolean SYSTEM_PROVIDED = true;
  private static final String ARGUMENT_UNSPECIFIED_DEFAULT_VALUE = "ARGUMENT_UNSPECIFIED_DEFAULT_VALUE";
  private static final String MESSAGE_FOR_DEFAULT_ARGUMENT = "Testing for argument with defaults";
  private static final String MESSAGE_FOR_ARGUMENT = "Testing for argument without defaults";

  /**
   * Test for {@link CliArgument#name()}
   */
  @Test
  public void testName() throws Exception {
    String name = ((CliArgument) (ArgumentTestingClass.class.getMethod("defaultArgumentTestingMethod", String.class).getParameterAnnotations()[0][0])).name();
    assertNotNull(name);
    assertEquals(MESSAGE_FOR_DEFAULT_ARGUMENT, name, ARGUMENT_NAME);
    name = ((CliArgument) (ArgumentTestingClass.class.getMethod("argumentTestingMethod", String.class).getParameterAnnotations()[0][0])).name();
    assertNotNull(name);
    assertEquals(MESSAGE_FOR_ARGUMENT, name, ARGUMENT_NAME);
  }

  /**
   * Test for {@link CliArgument#help()}
   */
  @Test
  public void testHelp() throws Exception {
    String help = ((CliArgument) (ArgumentTestingClass.class.getMethod("defaultArgumentTestingMethod", String.class).getParameterAnnotations()[0][0])).help();
    assertNotNull(help);
    assertEquals(MESSAGE_FOR_DEFAULT_ARGUMENT, help, "");
    help = ((CliArgument) (ArgumentTestingClass.class.getMethod("argumentTestingMethod", String.class).getParameterAnnotations()[0][0])).help();
    assertNotNull(help);
    assertEquals(MESSAGE_FOR_ARGUMENT, help, ARGUMENT_HELP);
  }

  /**
   * Test for {@link CliArgument#mandatory()}
   */
  @Test
  public void testMandatory() throws Exception {
    boolean mandatory = ((CliArgument) (ArgumentTestingClass.class.getMethod("defaultArgumentTestingMethod", String.class).getParameterAnnotations()[0][0])).mandatory();
    assertEquals(MESSAGE_FOR_DEFAULT_ARGUMENT, mandatory, false);
    mandatory = ((CliArgument) (ArgumentTestingClass.class.getMethod("argumentTestingMethod", String.class).getParameterAnnotations()[0][0])).mandatory();
    assertEquals(MESSAGE_FOR_ARGUMENT, mandatory, ARGUMENT_MANDATORY);
  }

  /**
   * Test for {@link CliArgument#argumentContext()}
   */
  @Test
  public void testArgumentContext() throws Exception {
    String argumentContext = ((CliArgument) (ArgumentTestingClass.class.getMethod("defaultArgumentTestingMethod", String.class).getParameterAnnotations()[0][0])).argumentContext();
    assertNotNull(argumentContext);
    assertEquals(MESSAGE_FOR_DEFAULT_ARGUMENT, argumentContext, "");
    argumentContext = ((CliArgument) (ArgumentTestingClass.class.getMethod("argumentTestingMethod", String.class).getParameterAnnotations()[0][0])).argumentContext();
    assertNotNull(argumentContext);
    assertEquals(MESSAGE_FOR_ARGUMENT, argumentContext, ARGUMENT_CONTEXT);
  }

  /**
   * Test for {@link CliArgument#systemProvided()}
   */
  @Test
  public void testSystemProvided() throws Exception {
    boolean systemProvided = ((CliArgument) (ArgumentTestingClass.class.getMethod("defaultArgumentTestingMethod", String.class).getParameterAnnotations()[0][0])).systemProvided();
    assertEquals(MESSAGE_FOR_DEFAULT_ARGUMENT, systemProvided, false);
    systemProvided = ((CliArgument) (ArgumentTestingClass.class.getMethod("argumentTestingMethod", String.class).getParameterAnnotations()[0][0])).systemProvided();
    assertEquals(MESSAGE_FOR_ARGUMENT, systemProvided, SYSTEM_PROVIDED);
  }

  /**
   * Test for {@link CliArgument#unspecifiedDefaultValue()}
   */
  @Test
  public void testUnspecifiedDefaultValue() throws Exception {
    String unspecifiedDefaultValue = ((CliArgument) (ArgumentTestingClass.class.getMethod("defaultArgumentTestingMethod", String.class).getParameterAnnotations()[0][0])).unspecifiedDefaultValue();
    assertEquals(MESSAGE_FOR_DEFAULT_ARGUMENT, unspecifiedDefaultValue, "__NULL__");
    unspecifiedDefaultValue = ((CliArgument) (ArgumentTestingClass.class.getMethod("argumentTestingMethod", String.class).getParameterAnnotations()[0][0])).unspecifiedDefaultValue();
    assertEquals(MESSAGE_FOR_ARGUMENT, unspecifiedDefaultValue, ARGUMENT_UNSPECIFIED_DEFAULT_VALUE);
  }

  /**
   * Class used by the tests
   */
  private static class ArgumentTestingClass {

    @SuppressWarnings("unused")
    public static Object defaultArgumentTestingMethod(@CliArgument(name = ARGUMENT_NAME) String defaultArgument) {
      return null;
    }

    @SuppressWarnings("unused")
    public static Object argumentTestingMethod(@CliArgument(name = ARGUMENT_NAME, help = ARGUMENT_HELP, mandatory = ARGUMENT_MANDATORY, argumentContext = ARGUMENT_CONTEXT, systemProvided = SYSTEM_PROVIDED, unspecifiedDefaultValue = ARGUMENT_UNSPECIFIED_DEFAULT_VALUE) String argument) {
      return null;
    }
  }
}
