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
package org.apache.geode.internal.logging;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.logging.log4j.util.StackLocator;

import org.apache.geode.internal.logging.log4j.FastLogger;
import org.apache.geode.internal.logging.log4j.message.GemFireParameterizedMessage;
import org.apache.geode.internal.logging.log4j.message.GemFireParameterizedMessageFactory;

/**
 * Provides Log4J2 Loggers with customized optimizations for Geode:
 *
 * <p>
 * Returned Logger is wrapped inside an instance of {@link FastLogger} which skips expensive
 * filtering, debug and trace handling with a volatile boolean. This optimization is turned on only
 * when using the default Geode {@code log4j2.xml} by checking for the existence of this property:
 *
 * <pre>
 * &lt;Property name="geode-default"&gt;true&lt;/Property&gt;
 * </pre>
 *
 * <p>
 * Returned Logger uses {@link GemFireParameterizedMessageFactory} to create
 * {@link GemFireParameterizedMessage} which excludes {@link Region}s from being handled as a
 * {@code Map} and {@link EntriesSet} from being handled as a {@code Collection}. Without this
 * change, using a {@code Region} or {@code EntriesSet} in a log statement can result in an
 * expensive operation or even a hang in the case of a {@code PartitionedRegion}.
 *
 * <p>
 * {@code LogService} only uses Log4J2 API so that any logging backend may be used.
 */
public class LogService extends LogManager {

  private LogService() {
    // do not instantiate
  }

  /**
   * Returns a Logger with the name of the calling class.
   *
   * @return The Logger for the calling class.
   */
  public static Logger getLogger() {
    String name = StackLocator.getInstance().getCallerClass(2).getName();
    return new FastLogger(
        LogManager.getLogger(name, GemFireParameterizedMessageFactory.INSTANCE));
  }

  public static Logger getLogger(final String name) {
    return new FastLogger(LogManager.getLogger(name, GemFireParameterizedMessageFactory.INSTANCE));
  }

}
