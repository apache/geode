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

import java.io.File;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;
import java.util.Objects;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Supplier;

import org.junit.rules.TestRule;
import org.junit.runner.Description;
import org.junit.runners.model.MultipleFailureException;
import org.junit.runners.model.Statement;

import org.apache.geode.test.junit.rules.Folder;

public class GfshRule implements TestRule, GfshExecutor {

  private final List<GfshContext> gfshContexts = synchronizedList(new ArrayList<>());
  private final List<Throwable> errors = synchronizedList(new ArrayList<>());

  private final AtomicReference<Folder> suppliedFolder = new AtomicReference<>();
  private final AtomicReference<GfshContext> defaultContext = new AtomicReference<>();

  private final Supplier<Folder> folderSupplier;

  public GfshExecutor.Builder executor() {
    return new GfshContext.Builder(gfshContexts::add, errors::add);
  }

  public GfshRule() {
    this(null);
  }

  public GfshRule(Supplier<Folder> folderSupplier) {
    this.folderSupplier = folderSupplier;
  }

  public GfshRule setFolder(Folder folder) {
    suppliedFolder.set(folder);
    return this;
  }

  @Override
  public Statement apply(Statement base, Description description) {
    return new Statement() {
      @Override
      public void evaluate() throws Throwable {
        try {
          initializeFolder();
          base.evaluate();
        } catch (MultipleFailureException e) {
          errors.addAll(e.getFailures());
        } catch (Throwable e) {
          errors.add(e);
        } finally {
          try {
            cleanupGfshContexts();
          } catch (Throwable e) {
            errors.add(e);
          }
        }

        MultipleFailureException.assertEmpty(errors);
      }
    };
  }

  private void initializeFolder() {
    synchronized (defaultContext) {
      if (defaultContext.get() != null || suppliedFolder.get() != null) {
        return;
      }

      if (folderSupplier != null) {
        suppliedFolder.set(folderSupplier.get());
      }
    }
  }

  private void cleanupGfshContexts() {
    gfshContexts.forEach(GfshContext::killProcesses);
  }

  private GfshExecutor defaultExecutor() {
    synchronized (defaultContext) {
      GfshContext context = defaultContext.get();
      if (context != null) {
        return context;
      }

      Folder folder = suppliedFolder.get();
      Objects.requireNonNull(folder,
          "Construction of GfshRule with Supplier<Folder> is required in order to use defaultContext()");
      return executor().build(suppliedFolder.get().toPath());
    }
  }

  @Override
  public GfshExecution execute(String... commands) {
    return defaultExecutor().execute(commands);
  }

  @Override
  public GfshExecution execute(GfshScript gfshScript) {
    return defaultExecutor().execute(gfshScript);
  }

  @Override
  public GfshExecution execute(File workingDir, String... commands) {
    return defaultExecutor().execute(workingDir, commands);
  }

  @Override
  public GfshExecution execute(Path workingDir, String... commands) {
    return defaultExecutor().execute(workingDir, commands);
  }

  @Override
  public GfshExecution execute(Path workingDir, GfshScript gfshScript) {
    return defaultExecutor().execute(workingDir, gfshScript);
  }

  @Override
  public GfshExecution execute(File workingDir, GfshScript gfshScript) {
    return defaultExecutor().execute(workingDir, gfshScript);
  }
}
