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
package org.apache.geode.internal.serialization.filter;

import static java.util.Objects.nonNull;
import static java.util.Objects.requireNonNull;
import static org.apache.commons.lang3.exception.ExceptionUtils.getRootCause;

import java.util.function.Consumer;

import org.apache.logging.log4j.Logger;

import org.apache.geode.annotations.VisibleForTesting;
import org.apache.geode.logging.internal.log4j.api.LogService;

public class GlobalSerialFilterConfiguration implements FilterConfiguration {

  private static final Logger logger = LogService.getLogger();

  private final GlobalSerialFilter globalSerialFilter;
  private final Consumer<String> infoLogger;

  public GlobalSerialFilterConfiguration(GlobalSerialFilter globalSerialFilter) {
    this(globalSerialFilter, logger::info);
  }

  @VisibleForTesting
  GlobalSerialFilterConfiguration(GlobalSerialFilter globalSerialFilter,
      Consumer<String> infoLogger) {
    this.globalSerialFilter = requireNonNull(globalSerialFilter, "globalSerialFilter is required");
    this.infoLogger = infoLogger;
  }

  @Override
  public void configure() {
    new SetSerialFilter(globalSerialFilter, infoLogger).execute();
  }

  private static class SetSerialFilter {

    private final GlobalSerialFilter globalSerialFilter;
    private final Consumer<String> infoLogger;

    private SetSerialFilter(GlobalSerialFilter globalSerialFilter, Consumer<String> infoLogger) {
      this.globalSerialFilter = globalSerialFilter;
      this.infoLogger = infoLogger;
    }

    public void execute() {
      try {
        globalSerialFilter.setFilter();
      } catch (UnsupportedOperationException e) {
        if (hasRootCauseWithMessage(e, IllegalStateException.class,
            "Serial filter can only be set once")) {
          infoLogger.accept("Global serial filter is already configured.");
        }
      }
    }

    private static boolean hasRootCauseWithMessage(Throwable throwable,
        Class<? extends Throwable> causeClass, String message) {
      Throwable rootCause = getRootCause(throwable);
      return isInstanceOf(rootCause, causeClass) && hasMessage(rootCause, message);
    }

    private static boolean isInstanceOf(Throwable throwable,
        Class<? extends Throwable> causeClass) {
      return nonNull(throwable) && throwable.getClass().equals(causeClass);
    }

    private static boolean hasMessage(Throwable throwable, String message) {
      return nonNull(throwable) && throwable.getMessage().equalsIgnoreCase(message);
    }
  }
}
