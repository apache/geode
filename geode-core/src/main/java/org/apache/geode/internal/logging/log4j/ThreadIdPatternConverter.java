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

import org.apache.logging.log4j.core.LogEvent;
import org.apache.logging.log4j.core.config.plugins.Plugin;
import org.apache.logging.log4j.core.pattern.ConverterKeys;
import org.apache.logging.log4j.core.pattern.LogEventPatternConverter;

/**
 * Formats the event thread id.
 */
@Plugin(name = "ThreadIdPatternConverter", category = "Converter")
@ConverterKeys({"tid", "threadId"})
public final class ThreadIdPatternConverter extends LogEventPatternConverter {
  /**
   * Singleton.
   */
  private static final ThreadIdPatternConverter INSTANCE = new ThreadIdPatternConverter();

  /**
   * Private constructor.
   */
  private ThreadIdPatternConverter() {
    super("ThreadId", "threadId");
  }

  /**
   * Obtains an instance of ThreadPatternConverter.
   *
   * @param options options, currently ignored, may be null.
   * @return instance of ThreadPatternConverter.
   */
  public static ThreadIdPatternConverter newInstance(final String[] options) {
    return INSTANCE;
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public void format(final LogEvent event, final StringBuilder toAppendTo) {
    toAppendTo.append("0x").append(Long.toHexString(Thread.currentThread().getId()));
  }
}
