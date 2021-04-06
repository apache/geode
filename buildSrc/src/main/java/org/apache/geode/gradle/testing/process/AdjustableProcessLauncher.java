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
 *
 */

package org.apache.geode.gradle.testing.process;

import static java.util.stream.Collectors.toMap;

import java.io.IOException;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.function.Consumer;
import java.util.stream.Collectors;

import org.gradle.api.UncheckedIOException;

/**
 * A process launcher that applies an adjustment to the  {@link ProcessBuilder} before launching the
 * process.
 */
public class AdjustableProcessLauncher implements ProcessLauncher {
  private final Consumer<ProcessBuilder> adjustment;

  public AdjustableProcessLauncher(Consumer<ProcessBuilder> adjustment) {
    this.adjustment = adjustment;
  }

  @Override
  public Process start(ProcessBuilder processBuilder) {
    adjustment.accept(processBuilder);
    try {
      return processBuilder.start();
    } catch (IOException e) {
      throw new UncheckedIOException("Cannot launch process", e);
    }
  }
}
