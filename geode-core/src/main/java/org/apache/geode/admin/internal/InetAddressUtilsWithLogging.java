/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.geode.admin.internal;

import static org.apache.geode.admin.internal.InetAddressUtils.validateHostOrThrow;

import java.net.InetAddress;
import java.net.UnknownHostException;

import org.apache.logging.log4j.Logger;

import org.apache.geode.logging.internal.log4j.api.LogService;

/**
 * @deprecated Please use {@link InetAddressUtils} instead and log any thrown errors within the
 *             calling code instead of these utils.
 */
@Deprecated
public class InetAddressUtilsWithLogging {

  private static final Logger logger = LogService.getLogger();

  private InetAddressUtilsWithLogging() {
    // prevent construction
  }

  /**
   * Invokes {@link InetAddressUtils#toInetAddressOrThrow(String)} and logs
   * {@code UnknownHostException} call stack if no IP address for the {@code host} could be found
   *
   * @deprecated Please use {@link InetAddressUtils} instead and log any thrown errors within the
   *             calling code instead of these utils.
   */
  @Deprecated
  public static InetAddress toInetAddress(String host) {
    try {
      return InetAddressUtils.toInetAddressOrThrow(host);
    } catch (UnknownHostException e) {
      logStackTrace(e);
      throw new AssertionError("Failed to get InetAddress: " + host, e);
    }
  }

  /**
   * Invokes {@link InetAddressUtils#validateHost(String)} and logs {@code UnknownHostException}
   * call stack if no IP address for the {@code host} could be found.
   *
   * @deprecated Please use {@link InetAddressUtils} instead and log any thrown errors within the
   *             calling code instead of these utils.
   */
  @Deprecated
  public static String validateHost(String host) {
    try {
      return validateHostOrThrow(host);
    } catch (UnknownHostException e) {
      logStackTrace(e);
      return null;
    }
  }

  /**
   * Logs the stack trace for the given Throwable if logger is initialized else prints the stack
   * trace using System.out. If logged the logs are logged at WARNING level.
   *
   * @param throwable Throwable to log stack trace for
   */
  private static void logStackTrace(Throwable throwable) {
    logger.warn(throwable.getMessage(), throwable);
  }
}
