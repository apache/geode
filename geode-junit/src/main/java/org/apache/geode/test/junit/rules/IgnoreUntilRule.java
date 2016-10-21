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
package org.apache.geode.test.junit.rules;

import java.io.Serializable;
import java.text.DateFormat;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Calendar;

import org.junit.AssumptionViolatedException;
import org.junit.rules.TestRule;
import org.junit.runner.Description;
import org.junit.runners.model.Statement;
import org.apache.geode.test.junit.IgnoreUntil;
import org.apache.geode.test.junit.IgnoreCondition;
import org.apache.geode.test.junit.support.IgnoreConditionEvaluationException;

/**
 * The IgnoreUntilRule class...
 *
 * @see org.junit.rules.TestRule
 * @see org.junit.runner.Description
 * @see org.junit.runners.model.Statement
 * @see org.apache.geode.test.junit.IgnoreUntil
 * @see org.apache.geode.test.junit.IgnoreCondition
 */
@SuppressWarnings({"serial", "unused"})
public class IgnoreUntilRule implements TestRule, Serializable {

  protected static final String DATE_FORMAT_PATTERN = "yyyy-MM-dd";
  protected static final String DEFAULT_MESSAGE = "Ignoring test case (%1$s) of test class (%2$s)!";

  protected static final DateFormat DATE_FORMAT = new SimpleDateFormat(DATE_FORMAT_PATTERN);

  @Override
  public Statement apply(final Statement base, final Description description) {
    return new Statement() {
      @Override
      public void evaluate() throws Throwable {
        IgnoreUntilRule.this.evaluate(base, description);
      }
    };
  }

  public final void evaluate(Statement statement, Description description) throws Throwable {
    throwOnIgnoreTest(statement, description).evaluate();
  }

  protected Statement throwOnIgnoreTest(Statement statement, Description description) {
    if (isTest(description)) {
      boolean ignoreTest = false;
      String message = "";

      IgnoreUntil testCaseAnnotation = description.getAnnotation(IgnoreUntil.class);

      if (testCaseAnnotation != null) {
        ignoreTest = evaluate(testCaseAnnotation, description);
        message = testCaseAnnotation.value();
      } else if (description.getTestClass().isAnnotationPresent(IgnoreUntil.class)) {
        IgnoreUntil testClassAnnotation =
            description.getTestClass().getAnnotation(IgnoreUntil.class);

        ignoreTest = evaluate(testClassAnnotation, description);
        message = testClassAnnotation.value();
      }

      if (ignoreTest) {
        throw new AssumptionViolatedException(format(message, description));
      }
    }

    return statement;
  }

  protected boolean isTest(final Description description) {
    return (description.isSuite() || description.isTest());
  }

  protected String format(String message, Description description) {
    message = (!message.isEmpty() ? message : DEFAULT_MESSAGE);
    return String.format(message, description.getMethodName(), description.getClassName());
  }

  protected boolean evaluate(IgnoreUntil conditionalIgnoreAnnotation, Description description) {
    return (evaluateCondition(conditionalIgnoreAnnotation.condition(), description)
        || evaluateUntil(conditionalIgnoreAnnotation.until()));
  }

  protected boolean evaluateCondition(Class<? extends IgnoreCondition> ignoreConditionType,
      Description description) {
    try {
      return ignoreConditionType.newInstance().evaluate(description);
    } catch (Exception e) {
      throw new IgnoreConditionEvaluationException(
          String.format("failed to evaluate IgnoreCondition: %1$s", ignoreConditionType.getName()),
          e);
    }
  }

  protected boolean evaluateUntil(String timestamp) {
    try {
      return DATE_FORMAT.parse(timestamp).after(Calendar.getInstance().getTime());
    } catch (ParseException e) {
      return false;
    }
  }

}
