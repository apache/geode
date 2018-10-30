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
package org.apache.geode.test.assertj;

import org.apache.logging.log4j.Level;

import org.apache.geode.test.assertj.internal.AbstractLogLevelAssert;

/**
 * {@code AssertJ} assertions for {@code Level}.
 */
public class LogLevelAssert extends AbstractLogLevelAssert<LogLevelAssert> {

  public static LogLevelAssert assertThat(Level actual) {
    return new LogLevelAssert(actual);
  }

  public LogLevelAssert(Level actual) {
    super(actual, LogLevelAssert.class);
  }
}
