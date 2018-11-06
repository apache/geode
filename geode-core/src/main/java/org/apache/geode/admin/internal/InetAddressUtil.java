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
import java.net.NetworkInterface;
import java.net.SocketException;
import java.util.Enumeration;

import org.apache.logging.log4j.Logger;

import org.apache.geode.GemFireIOException;
import org.apache.geode.internal.Assert;
import org.apache.geode.internal.logging.LogService;
import org.apache.geode.internal.net.SocketCreator;


/**
 * Provides static utilities for manipulating, validating, and converting InetAddresses and host
 * strings.
 *
 * @since GemFire 3.5
 */
@Deprecated
public class InetAddressUtil {

  private static final Logger logger = LogService.getLogger();

  /** InetAddress instance representing the local host */
  public static final InetAddress LOCALHOST = createLocalHost();

  public static final String LOOPBACK_ADDRESS =
      SocketCreator.preferIPv6Addresses() ? "::1" : "127.0.0.1";

  public static final InetAddress LOOPBACK = InetAddressUtil.toInetAddress(LOOPBACK_ADDRESS);

  /** Disallows InetAddressUtil instantiation. */
  private InetAddressUtil() {}

  /**
   * Returns a string version of InetAddress which can be converted back to an InetAddress later.
   * Essentially any leading slash is trimmed.
   *
   * @param val the InetAddress or String to return a formatted string of
   * @return string version the InetAddress minus any leading slash
   */
  public static String toString(Object val) {
    if (val instanceof String) {
      return trimLeadingSlash((String) val);

    } else if (val instanceof InetAddress) {
      return ((InetAddress) val).getHostAddress();

    } else {
      return trimLeadingSlash(val.toString());
    }
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
    if (host == null || host.length() == 0) {
      return null;
    }
    try {
      if (host.indexOf("/") > -1) {
        return InetAddress.getByName(host.substring(host.indexOf("/") + 1));
      } else {
        return InetAddress.getByName(host);
      }
    } catch (java.net.UnknownHostException e) {
      logStackTrace(e);
      Assert.assertTrue(false, "Failed to get InetAddress: " + host);
      return null; // will never happen since the Assert will fail
    }
  }

  /**
   * Creates an InetAddress representing the local host. The checked exception
   * <code>java.lang.UnknownHostException</code> is captured and results in an Assertion failure
   * instead.
   *
   * @return InetAddress instance representing the local host
   */
  public static InetAddress createLocalHost() {
    try {
      return SocketCreator.getLocalHost();
    } catch (java.net.UnknownHostException e) {
      logStackTrace(e);
      Assert.assertTrue(false, "Failed to get local host");
      return null; // will never happen
    }
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
    if (host == null || host.length() == 0) {
      return null;
    }
    try {
      InetAddress.getByName(trimLeadingSlash(host));
      return host;
    } catch (java.net.UnknownHostException e) {
      logStackTrace(e);
      return null;
    }
  }

  /** Returns true if host matches the LOCALHOST. */
  public static boolean isLocalHost(Object host) {
    if (host instanceof InetAddress) {
      if (LOCALHOST.equals(host)) {
        return true;
      } else {
        try {
          Enumeration en = NetworkInterface.getNetworkInterfaces();
          while (en.hasMoreElements()) {
            NetworkInterface i = (NetworkInterface) en.nextElement();
            for (Enumeration en2 = i.getInetAddresses(); en2.hasMoreElements();) {
              InetAddress addr = (InetAddress) en2.nextElement();
              if (host.equals(addr)) {
                return true;
              }
            }
          }
          return false;
        } catch (SocketException e) {
          throw new GemFireIOException(
              "Unable to query network interface",
              e);
        }
      }
    } else {
      return isLocalHost(InetAddressUtil.toInetAddress(host.toString()));
    }
  }

  /** Returns true if host matches the LOOPBACK (127.0.0.1). */
  public static boolean isLoopback(Object host) {
    if (host instanceof InetAddress) {
      return LOOPBACK.equals(host);
    } else {
      return isLoopback(InetAddressUtil.toInetAddress(host.toString()));
    }
  }

  /** Returns a version of the value after removing any leading slashes */
  private static String trimLeadingSlash(String value) {
    if (value == null)
      return "";
    while (value.indexOf("/") > -1) {
      value = value.substring(value.indexOf("/") + 1);
    }
    return value;
  }

  /**
   * Logs the stack trace for the given Throwable if logger is initialized else prints the stack
   * trace using System.out. If logged the logs are logged at WARNING level.
   *
   * @param throwable Throwable to log stack trace for
   */
  private static void logStackTrace(Throwable throwable) {
    AdminDistributedSystemImpl adminDS = AdminDistributedSystemImpl.getConnectedInstance();

    logger.warn(throwable.getMessage(), throwable);
  }

}
