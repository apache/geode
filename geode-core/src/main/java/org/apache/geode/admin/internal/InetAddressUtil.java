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
package org.apache.geode.admin.internal;

import java.net.InetAddress;

import org.apache.geode.internal.net.InetAddressUtils;

/**
 * Provides static utilities for manipulating, validating, and converting InetAddresses and host
 * strings.
 *
 * @since GemFire 3.5
 *
 * @deprecated Please use {@link InetAddressUtils} instead.
 */
@Deprecated
public class InetAddressUtil {

  /** Disallows InetAddressUtil instantiation. */
  private InetAddressUtil() {
    // nothing
  }

  /**
   * Returns a string version of InetAddress which can be converted back to an InetAddress later.
   * Essentially any leading slash is trimmed.
   *
   * @param val the InetAddress or String to return a formatted string of
   * @return string version the InetAddress minus any leading slash
   */
  public static String toString(Object val) {
    return InetAddressUtils.toString(val);
  }

  /**
   * Converts the string host to an instance of InetAddress. Returns null if the string is empty.
   * Fails Assertion if the conversion would result in <code>java.lang.UnknownHostException</code>.
   * <p>
   * Any leading slashes on host will be ignored.
   *
   * @param host string version the InetAddress
   * @return the host converted to InetAddress instance
   */
  public static InetAddress toInetAddress(String host) {
    return InetAddressUtils.toInetAddress(host);
  }

  /**
   * Validates the host by making sure it can successfully be used to get an instance of
   * InetAddress. If the host string is null, empty or would result in
   * <code>java.lang.UnknownHostException</code> then null is returned.
   * <p>
   * Any leading slashes on host will be ignored.
   *
   * @param host string version the InetAddress
   * @return the host converted to InetAddress instance
   */
  public static String validateHost(String host) {
    return InetAddressUtils.validateHost(host);
  }

  /** Returns true if host matches the LOCALHOST. */
  static boolean isLocalHost(Object host) {
    if (host instanceof InetAddress) {
      return InetAddressUtils.isLocalHost((InetAddress) host);
    }
    return InetAddressUtils.isLocalHost((String) host);
  }

  /** Returns true if host matches the LOOPBACK (127.0.0.1). */
  static boolean isLoopback(Object host) {
    if (host instanceof InetAddress) {
      return InetAddressUtils.isLoopback((InetAddress) host);
    }
    return InetAddressUtils.isLoopback((String) host);
  }
}
