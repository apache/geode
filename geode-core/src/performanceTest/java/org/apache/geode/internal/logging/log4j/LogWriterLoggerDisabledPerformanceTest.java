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
package org.apache.geode.internal.logging.log4j;

import java.io.IOException;

import org.apache.logging.log4j.Level;
import org.apache.logging.log4j.Logger;
import org.junit.Ignore;
import org.junit.experimental.categories.Category;

import org.apache.geode.test.junit.categories.PerformanceTest;

@Category(PerformanceTest.class)
@Ignore("Tests have no assertions")
public class LogWriterLoggerDisabledPerformanceTest extends LogWriterLoggerPerformanceTest {

  @Override
  protected PerformanceLogger createPerformanceLogger() throws IOException {
    final Logger logger = createLogger();

    final PerformanceLogger perfLogger = new PerformanceLogger() {
      @Override
      public void log(String message) {
        logger.debug(message);
      }

      @Override
      public boolean isEnabled() {
        return logger.isEnabled(Level.DEBUG);
      }
    };

    return perfLogger;
  }
}
