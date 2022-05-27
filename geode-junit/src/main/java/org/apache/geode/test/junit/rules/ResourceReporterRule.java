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

import static java.util.Collections.synchronizedList;

import java.nio.file.Path;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.function.Consumer;

import org.junit.rules.TestRule;
import org.junit.runner.Description;
import org.junit.runners.model.MultipleFailureException;
import org.junit.runners.model.Statement;


public class ResourceReporterRule implements TestRule {

  private final List<Throwable> errors = synchronizedList(new ArrayList<>());
  private final List<Runnable> actions = synchronizedList(new ArrayList<>());

  @Override
  public Statement apply(Statement base, Description description) {
    return new Statement() {
      @Override
      public void evaluate() throws Throwable {
        try {
          // initializeFolder();
          base.evaluate();
        } catch (MultipleFailureException e) {
          // errors.addAll(e.getFailures());
        } catch (Throwable e) {
          // errors.add(e);
        } finally {
          try {
            // cleanupGfshContexts();
          } catch (Throwable e) {
            // errors.add(e);
          }
        }

        MultipleFailureException.assertEmpty(errors);
      }
    };
  }

  private enum Policy {
    BEFORE,
    AFTER,
    BOTH
  }

  public ResourceReporterRule.Builder builder() {
    return null; // return new ResourceReporterRule.Builder(e - > errors.add(e));
  }

  public static class Builder {

    private final Map<Policy, List<Runnable>> actions = new HashMap<>();
    private final Consumer<Throwable> thrown;

    private Path dir;

    Builder(Consumer<Throwable> thrown) {
      this.thrown = thrown;
    }

    public Builder doBefore(Runnable action) {
      return this;
    }

    public Builder doAfter(Runnable action) {
      return this;
    }

    public Builder doBeforeAndAfter(Runnable action) {
      return this;
    }

    public ResourceReporterRule build(Path dir) {
      this.dir = dir;
      return new ResourceReporterRule();
    }
  }
}
