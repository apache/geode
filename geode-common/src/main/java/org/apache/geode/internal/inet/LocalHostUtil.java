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
package org.apache.geode.internal.inet;

import java.net.Inet4Address;
import java.net.Inet6Address;
import java.net.InetAddress;
import java.net.NetworkInterface;
import java.net.SocketException;
import java.net.UnknownHostException;
import java.util.Enumeration;
import java.util.HashSet;
import java.util.Hashtable;
import java.util.Set;

import javax.naming.Context;
import javax.naming.NamingEnumeration;
import javax.naming.directory.Attribute;
import javax.naming.directory.Attributes;
import javax.naming.directory.DirContext;
import javax.naming.directory.InitialDirContext;

import org.apache.geode.annotations.internal.MakeNotStatic;

/**
 * LocalHostUtil provides lookup for the preferred local host InetAddress. It makes up
 * for deficiencies in /etc/hosts configuration and tries to ensure that, if available,
 * a non-loopback address is used.
 */

public class LocalHostUtil {
  /**
   * Optional system property to enable GemFire usage of link-local addresses
   */
  private static final String USE_LINK_LOCAL_ADDRESSES_PROPERTY =
      "gemfire.net.useLinkLocalAddresses";

  /**
   * True if GemFire should use link-local addresses
   */
  private static final boolean useLinkLocalAddresses =
      Boolean.getBoolean(USE_LINK_LOCAL_ADDRESSES_PROPERTY);

  /**
   * we cache localHost to avoid bug #40619, access-violation in native code
   */
  private static final InetAddress localHost;

  /**
   * all classes should use this variable to determine whether to use IPv4 or IPv6 addresses
   */
  @MakeNotStatic
  private static boolean useIPv6Addresses = !Boolean.getBoolean("java.net.preferIPv4Stack")
      && Boolean.getBoolean("java.net.preferIPv6Addresses");

  static {
    InetAddress inetAddress = null;
    try {
      inetAddress = InetAddress.getByAddress(InetAddress.getLocalHost().getAddress());
      if (inetAddress.isLoopbackAddress()) {
        InetAddress ipv4Fallback = null;
        InetAddress ipv6Fallback = null;
        // try to find a non-loopback address
        Set<InetAddress> myInterfaces = getMyAddresses();
        boolean preferIPv6 = useIPv6Addresses;
        String lhName = null;
        for (InetAddress addr : myInterfaces) {
          if (addr.isLoopbackAddress() || addr.isAnyLocalAddress() || lhName != null) {
            break;
          }
          boolean ipv6 = addr instanceof Inet6Address;
          boolean ipv4 = addr instanceof Inet4Address;
          if ((preferIPv6 && ipv6) || (!preferIPv6 && ipv4)) {
            String addrName = reverseDNS(addr);
            if (inetAddress.isLoopbackAddress()) {
              inetAddress = addr;
              lhName = addrName;
            } else if (addrName != null) {
              inetAddress = addr;
              lhName = addrName;
            }
          } else {
            if (preferIPv6 && ipv4 && ipv4Fallback == null) {
              ipv4Fallback = addr;
            } else if (!preferIPv6 && ipv6 && ipv6Fallback == null) {
              ipv6Fallback = addr;
            }
          }
        }
        // vanilla Ubuntu installations will have a usable IPv6 address when
        // running as a guest OS on an IPv6-enabled machine. We also look for
        // the alternative IPv4 configuration.
        if (inetAddress.isLoopbackAddress()) {
          if (ipv4Fallback != null) {
            inetAddress = ipv4Fallback;
            useIPv6Addresses = false;
          } else if (ipv6Fallback != null) {
            inetAddress = ipv6Fallback;
            useIPv6Addresses = true;
          }
        }
      }
    } catch (UnknownHostException ignored) {
    }
    localHost = inetAddress;
  }


  /**
   * returns a set of the non-loopback InetAddresses for this machine
   */
  public static Set<InetAddress> getMyAddresses() {
    Set<InetAddress> result = new HashSet<>();
    Set<InetAddress> locals = new HashSet<>();
    Enumeration<NetworkInterface> interfaces;
    try {
      interfaces = NetworkInterface.getNetworkInterfaces();
    } catch (SocketException e) {
      throw new IllegalArgumentException(
          "Unable to examine network interfaces",
          e);
    }
    while (interfaces.hasMoreElements()) {
      NetworkInterface face = interfaces.nextElement();
      boolean faceIsUp = false;
      try {
        faceIsUp = face.isUp();
      } catch (SocketException e) {
        // since it's not usable we'll skip this interface
      }
      if (faceIsUp) {
        Enumeration<InetAddress> addrs = face.getInetAddresses();
        while (addrs.hasMoreElements()) {
          InetAddress addr = addrs.nextElement();
          if (addr.isLoopbackAddress() || addr.isAnyLocalAddress()
              || (!useLinkLocalAddresses && addr.isLinkLocalAddress())) {
            locals.add(addr);
          } else {
            result.add(addr);
          }
        } // while
      }
    } // while
    // fix for bug #42427 - allow product to run on a standalone box by using
    // local addresses if there are no non-local addresses available
    if (result.size() == 0) {
      return locals;
    } else {
      return result;
    }
  }

  /**
   * This method uses JNDI to look up an address in DNS and return its name
   *
   *
   * @return the host name associated with the address or null if lookup isn't possible or there is
   *         no host name for this address
   */
  private static String reverseDNS(InetAddress addr) {
    byte[] addrBytes = addr.getAddress();
    // reverse the address suitable for reverse lookup
    StringBuilder lookup = new StringBuilder();
    for (int index = addrBytes.length - 1; index >= 0; index--) {
      lookup.append(addrBytes[index] & 0xff).append('.');
    }
    lookup.append("in-addr.arpa");

    try {
      Hashtable<String, String> env = new Hashtable<>();
      env.put(Context.INITIAL_CONTEXT_FACTORY, "com.sun.jndi.dns.DnsContextFactory");
      DirContext ctx = new InitialDirContext(env);
      Attributes attrs = ctx.getAttributes(lookup.toString(), new String[] {"PTR"});
      for (NamingEnumeration ae = attrs.getAll(); ae.hasMoreElements();) {
        Attribute attr = (Attribute) ae.next();
        for (Enumeration vals = attr.getAll(); vals.hasMoreElements();) {
          Object elem = vals.nextElement();
          if ("PTR".equals(attr.getID()) && elem != null) {
            return elem.toString();
          }
        }
      }
      ctx.close();
    } catch (Exception e) {
      // ignored
    }
    return null;
  }

  /**
   * All classes should use this instead of relying on the JRE system property
   */
  public static boolean preferIPv6Addresses() {
    return useIPv6Addresses;
  }

  /**
   * All GemFire code should use this method instead of InetAddress.getLocalHost(). See bug #40619
   */
  public static InetAddress getLocalHost() throws UnknownHostException {
    if (localHost == null) {
      throw new UnknownHostException();
    }
    return localHost;
  }

  /**
   * Returns true if host matches the LOCALHOST.
   */
  public static boolean isLocalHost(Object host) {
    if (host instanceof InetAddress) {
      InetAddress inetAddress = (InetAddress) host;
      if (isLocalHost(inetAddress)) {
        return true;
      } else if (inetAddress.isLoopbackAddress()) {
        return true;
      } else {
        try {
          Enumeration en = NetworkInterface.getNetworkInterfaces();
          while (en.hasMoreElements()) {
            NetworkInterface i = (NetworkInterface) en.nextElement();
            for (Enumeration en2 = i.getInetAddresses(); en2.hasMoreElements();) {
              InetAddress addr = (InetAddress) en2.nextElement();
              if (inetAddress.equals(addr)) {
                return true;
              }
            }
          }
          return false;
        } catch (SocketException e) {
          throw new IllegalArgumentException("Unable to query network interface", e);
        }
      }
    } else {
      return isLocalHost((Object) toInetAddress(host.toString()));
    }
  }

  private static boolean isLocalHost(InetAddress host) {
    try {
      return getLocalHost().equals(host);
    } catch (UnknownHostException ignored) {
      return false;
    }
  }

  /**
   * Converts the string host to an instance of InetAddress. Returns null if the string is empty.
   * Fails Assertion if the conversion would result in <code>java.lang.UnknownHostException</code>.
   * <p>
   * Any leading slashes on host will be ignored.
   *
   * @param host string version the InetAddress
   *
   * @return the host converted to InetAddress instance
   *
   * @throws IllegalArgumentException in lieu of UnknownHostException
   */
  public static InetAddress toInetAddress(String host) {
    if (host == null || host.length() == 0) {
      return null;
    }
    try {
      final int index = host.indexOf("/");
      if (index > -1) {
        return InetAddress.getByName(host.substring(index + 1));
      } else {
        return InetAddress.getByName(host);
      }
    } catch (UnknownHostException e) {
      throw new IllegalArgumentException(e.getMessage());
    }
  }

  public static String getLocalHostName() throws UnknownHostException {
    return getLocalHost().getHostName();
  }

  public static String getLocalHostString() throws UnknownHostException {
    return getLocalHost().toString();
  }

  public static String getCanonicalLocalHostName() throws UnknownHostException {
    return getLocalHost().getCanonicalHostName();

  }
}
