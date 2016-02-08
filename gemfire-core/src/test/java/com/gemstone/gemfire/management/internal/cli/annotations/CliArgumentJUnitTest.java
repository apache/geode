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

import org.junit.experimental.categories.Category;

import junit.framework.TestCase;

import com.gemstone.gemfire.management.internal.cli.annotation.CliArgument;
import com.gemstone.gemfire.test.junit.categories.UnitTest;

/**
 * Includes tests for checking assignment of {@link CliArgument}
 * 
 * @author njadhav
 * 
 */
@Category(UnitTest.class)
public class CliArgumentJUnitTest extends TestCase {

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
  public void testName() {
    try {
      String name = ((CliArgument) (ArgumentTestingClass.class.getMethod(
          "defaultArgumentTestingMethod", String.class)
          .getParameterAnnotations()[0][0])).name();
      assertNotNull(name);
      assertEquals(MESSAGE_FOR_DEFAULT_ARGUMENT, name, ARGUMENT_NAME);
      name = ((CliArgument) (ArgumentTestingClass.class.getMethod(
          "argumentTestingMethod", String.class).getParameterAnnotations()[0][0]))
          .name();
      assertNotNull(name);
      assertEquals(MESSAGE_FOR_ARGUMENT, name, ARGUMENT_NAME);
    } catch (SecurityException e) {
      // TODO Auto-generated catch block
      e.printStackTrace();
    } catch (NoSuchMethodException e) {
      // TODO Auto-generated catch block
      e.printStackTrace();
    }
  }

  /**
   * Test for {@link CliArgument#help()}
   */
  public void testHelp() {
    try {
      String help = ((CliArgument) (ArgumentTestingClass.class.getMethod(
          "defaultArgumentTestingMethod", String.class)
          .getParameterAnnotations()[0][0])).help();
      assertNotNull(help);
      assertEquals(MESSAGE_FOR_DEFAULT_ARGUMENT, help, "");
      help = ((CliArgument) (ArgumentTestingClass.class.getMethod(
          "argumentTestingMethod", String.class).getParameterAnnotations()[0][0]))
          .help();
      assertNotNull(help);
      assertEquals(MESSAGE_FOR_ARGUMENT, help, ARGUMENT_HELP);
    } catch (SecurityException e) {
      // TODO Auto-generated catch block
      e.printStackTrace();
    } catch (NoSuchMethodException e) {
      // TODO Auto-generated catch block
      e.printStackTrace();
    }
  }

  /**
   * Test for {@link CliArgument#mandatory()}
   */
  public void testMandatory() {
    try {
      boolean mandatory = ((CliArgument) (ArgumentTestingClass.class.getMethod(
          "defaultArgumentTestingMethod", String.class)
          .getParameterAnnotations()[0][0])).mandatory();
      assertEquals(MESSAGE_FOR_DEFAULT_ARGUMENT, mandatory, false);
      mandatory = ((CliArgument) (ArgumentTestingClass.class.getMethod(
          "argumentTestingMethod", String.class).getParameterAnnotations()[0][0]))
          .mandatory();
      assertEquals(MESSAGE_FOR_ARGUMENT, mandatory, ARGUMENT_MANDATORY);
    } catch (SecurityException e) {
      // TODO Auto-generated catch block
      e.printStackTrace();
    } catch (NoSuchMethodException e) {
      // TODO Auto-generated catch block
      e.printStackTrace();
    }
  }

  /**
   * Test for {@link CliArgument#argumentContext()}
   */
  public void testArgumentContext() {
    try {
      String argumentContext = ((CliArgument) (ArgumentTestingClass.class
          .getMethod("defaultArgumentTestingMethod", String.class)
          .getParameterAnnotations()[0][0])).argumentContext();
      assertNotNull(argumentContext);
      assertEquals(MESSAGE_FOR_DEFAULT_ARGUMENT, argumentContext, "");
      argumentContext = ((CliArgument) (ArgumentTestingClass.class.getMethod(
          "argumentTestingMethod", String.class).getParameterAnnotations()[0][0]))
          .argumentContext();
      assertNotNull(argumentContext);
      assertEquals(MESSAGE_FOR_ARGUMENT, argumentContext, ARGUMENT_CONTEXT);
    } catch (SecurityException e) {
      // TODO Auto-generated catch block
      e.printStackTrace();
    } catch (NoSuchMethodException e) {
      // TODO Auto-generated catch block
      e.printStackTrace();
    }
  }

  /**
   * Test for {@link CliArgument#systemProvided()}
   */
  public void testSystemProvided() {
    try {
      boolean systemProvided = ((CliArgument) (ArgumentTestingClass.class
          .getMethod("defaultArgumentTestingMethod", String.class)
          .getParameterAnnotations()[0][0])).systemProvided();
      assertEquals(MESSAGE_FOR_DEFAULT_ARGUMENT, systemProvided, false);
      systemProvided = ((CliArgument) (ArgumentTestingClass.class.getMethod(
          "argumentTestingMethod", String.class).getParameterAnnotations()[0][0]))
          .systemProvided();
      assertEquals(MESSAGE_FOR_ARGUMENT, systemProvided, SYSTEM_PROVIDED);
    } catch (SecurityException e) {
      // TODO Auto-generated catch block
      e.printStackTrace();
    } catch (NoSuchMethodException e) {
      // TODO Auto-generated catch block
      e.printStackTrace();
    }
  }

  /**
   * Test for {@link CliArgument#unspecifiedDefaultValue()}
   */
  public void testUnspecifiedDefaultValue() {
    try {
      String unspecifiedDefaultValue = ((CliArgument) (ArgumentTestingClass.class
          .getMethod("defaultArgumentTestingMethod", String.class)
          .getParameterAnnotations()[0][0])).unspecifiedDefaultValue();
      assertEquals(MESSAGE_FOR_DEFAULT_ARGUMENT, unspecifiedDefaultValue,
          "__NULL__");
      unspecifiedDefaultValue = ((CliArgument) (ArgumentTestingClass.class
          .getMethod("argumentTestingMethod", String.class)
          .getParameterAnnotations()[0][0])).unspecifiedDefaultValue();
      assertEquals(MESSAGE_FOR_ARGUMENT, unspecifiedDefaultValue,
          ARGUMENT_UNSPECIFIED_DEFAULT_VALUE);
    } catch (SecurityException e) {
      // TODO Auto-generated catch block
      e.printStackTrace();
    } catch (NoSuchMethodException e) {
      // TODO Auto-generated catch block
      e.printStackTrace();
    }
  }

  /**
   * Class used by the tests
   * 
   * @author njadhav
   * 
   */
  public static class ArgumentTestingClass {

    /**
     * @param defaultargument
     * @return Object
     */
    @SuppressWarnings("unused")
    public static Object defaultArgumentTestingMethod(
        @CliArgument(name = ARGUMENT_NAME)
        String defaultargument) {
      return null;
    }

    /**
     * @param argument
     * @return Object
     */
    @SuppressWarnings("unused")
    public static Object argumentTestingMethod(
        @CliArgument(name = ARGUMENT_NAME, help = ARGUMENT_HELP, mandatory = ARGUMENT_MANDATORY, argumentContext = ARGUMENT_CONTEXT, systemProvided = SYSTEM_PROVIDED, unspecifiedDefaultValue = ARGUMENT_UNSPECIFIED_DEFAULT_VALUE)
        String argument) {
      return null;
    }
  }
}
