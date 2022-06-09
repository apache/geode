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
package org.apache.geode.test.junit.rules.gfsh;

import static java.util.Collections.synchronizedList;
import static org.apache.geode.test.junit.rules.gfsh.GfshContext.Builder;

import java.io.File;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;
import java.util.function.Function;
import java.util.function.Supplier;

import org.junit.rules.TestRule;
import org.junit.runner.Description;
import org.junit.runners.model.MultipleFailureException;
import org.junit.runners.model.Statement;

import org.apache.geode.test.junit.rules.Folder;
import org.apache.geode.test.junit.rules.FolderFactory;

public class GfshRule implements TestRule, GfshExecutor {

  private final List<GfshContext> gfshContexts = synchronizedList(new ArrayList<>());
  private final List<Throwable> errors = synchronizedList(new ArrayList<>());

  private final Function<Description, Folder> folderProvider;

  private Folder folder;
  private GfshExecutor defaultExecutor;

  /**
   * Returns a builder for a {@link GfshExecutor} that uses this {@code GfshRule}'s folder as the
   * working directory for invoked processes.
   *
   * @return the builder
   */
  public Builder executor() {
    return new Builder(gfshContexts::add, errors::add, folder.toPath());
  }

  /**
   * Creates a new {@code GfshRule}.
   *
   * <p>
   * Use {@link #GfshRule(Supplier)} with a Folder Supplier instead.
   */
  public GfshRule() {
    this(description -> FolderFactory.create(Folder.Policy.DELETE_ON_PASS, description));
  }

  /**
   * Creates a new {@code GfshRule} that uses the supplied {@code Folder}.
   *
   * <p>
   * Rules should be ordered to ensure that {@code FolderRule} is set up before and torn down after
   * the {@code GfshRule}.
   *
   * <pre>
   * {@code @Rule(order = 0)}
   * {@code public FolderRule folderRule = new FolderRule();}
   * {@code @Rule(order = 1)}
   * {@code public GfshRule gfshRule = new GfshRule(folderRule::getFolder);}
   * </pre>
   *
   * @param folderSupplier supplies the Folder for GfshRule to use
   */
  public GfshRule(Supplier<Folder> folderSupplier) {
    this(description -> folderSupplier.get());
  }

  private GfshRule(Function<Description, Folder> folderProvider) {
    this.folderProvider = folderProvider;
  }

  @Override
  public Statement apply(Statement base, Description description) {
    return new Statement() {
      @Override
      public void evaluate() throws Throwable {
        folder = folderProvider.apply(description);
        defaultExecutor = executor().build();

        try {
          base.evaluate();
        } catch (MultipleFailureException e) {
          errors.addAll(e.getFailures());
        } catch (Throwable e) {
          errors.add(e);
        } finally {
          try {
            gfshContexts.forEach(GfshContext::killProcesses);
          } catch (Throwable e) {
            errors.add(e);
          }
        }

        if (errors.isEmpty()) {
          try {
            folder.testPassed();
          } catch (Throwable e) {
            errors.add(e);
          }
        }

        MultipleFailureException.assertEmpty(errors);
      }
    };
  }

  @Override
  public GfshExecution execute(String... commands) {
    return defaultExecutor.execute(commands);
  }

  @Override
  public GfshExecution execute(GfshScript gfshScript) {
    return defaultExecutor.execute(gfshScript);
  }

  @Override
  public GfshExecution execute(File workingDir, String... commands) {
    return defaultExecutor.execute(workingDir, commands);
  }

  @Override
  public GfshExecution execute(Path workingDir, String... commands) {
    return defaultExecutor.execute(workingDir, commands);
  }

  @Override
  public GfshExecution execute(Path workingDir, GfshScript gfshScript) {
    return defaultExecutor.execute(workingDir, gfshScript);
  }

  @Override
  public GfshExecution execute(File workingDir, GfshScript gfshScript) {
    return defaultExecutor.execute(workingDir, gfshScript);
  }
}
