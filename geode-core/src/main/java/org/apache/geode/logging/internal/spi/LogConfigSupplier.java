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
package org.apache.geode.logging.internal.spi;

import org.apache.geode.internal.statistics.StatisticsConfig;

/**
 * Provides logging configuration and change notification services to {@link LogConfigListener}s.
 */
public interface LogConfigSupplier {

  LogConfig getLogConfig();

  /**
   * {@link StatisticsConfig} is used only to make logging and statistics behave similarly if both
   * are configured. It would be nice to break this dependency so we can just use {@link LogConfig}.
   */
  StatisticsConfig getStatisticsConfig();

  void addLogConfigListener(LogConfigListener logConfigListener);

  void removeLogConfigListener(LogConfigListener logConfigListener);
}
