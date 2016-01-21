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
package com.gemstone.gemfire.test.junit.rules;

import java.io.Serializable;

import org.junit.rules.TestRule;
import org.junit.runner.Description;
import org.junit.runners.model.Statement;
import com.gemstone.gemfire.test.junit.Repeat;

/**
 * The RepeatRule class is a JUnit TestRule that enables an appropriately @Repeat annotated test case method
 * to be repeated a specified number of times.
 *
 * @author John Blum
 * @see org.junit.rules.TestRule
 * @see org.junit.runner.Description
 * @see org.junit.runners.model.Statement
 */
@SuppressWarnings({ "serial", "unused" })
public class RepeatRule implements TestRule, Serializable {

  protected static final int DEFAULT_REPETITIONS = 1;

  @Override
  public Statement apply(final Statement statement, final Description description) {
    return new Statement() {
      @Override public void evaluate() throws Throwable {
        RepeatRule.this.evaluate(statement, description);
      }
    };
  }

  protected void evaluate(final Statement statement, final Description description) throws Throwable {
    if (isTest(description)) {
      Repeat repeat = description.getAnnotation(Repeat.class);

      for (int count = 0, repetitions = getRepetitions(repeat); count < repetitions; count++) {
        statement.evaluate();
      }
    }
  }

  private int getRepetitions(final Repeat repeat) {
    int repetitions = DEFAULT_REPETITIONS;

    if (repeat != null) {
      if (!"".equals(repeat.property())) {
        repetitions = Integer.getInteger(repeat.property(), DEFAULT_REPETITIONS);
      } else {
        repetitions = repeat.value();
      }
    }
    
    if (repetitions < 1) {
      throw new IllegalArgumentException("Repeat value must be a positive integer");
    }

    return repetitions;
  }

  private boolean isTest(final Description description) {
    return (description.isSuite() || description.isTest());
  }

}
