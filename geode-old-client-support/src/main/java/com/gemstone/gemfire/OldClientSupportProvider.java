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
package com.gemstone.gemfire;

import java.io.DataInput;
import java.io.DataOutput;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

import org.apache.geode.cache.Cache;
import org.apache.geode.internal.InternalDataSerializer;
import org.apache.geode.internal.cache.CacheService;
import org.apache.geode.internal.cache.InternalCache;
import org.apache.geode.internal.cache.tier.sockets.OldClientSupportService;
import org.apache.geode.internal.serialization.KnownVersion;
import org.apache.geode.internal.serialization.VersionedDataOutputStream;
import org.apache.geode.management.internal.beans.CacheServiceMBeanBase;
import org.apache.geode.security.AuthenticationExpiredException;
import org.apache.geode.security.AuthenticationRequiredException;
import org.apache.geode.util.internal.GeodeGlossary;

import com.gemstone.gemfire.cache.execute.EmtpyRegionFunctionException;

/**
 * Support for old GemFire clients
 */
public class OldClientSupportProvider implements OldClientSupportService {
  static final String GEODE = "org.apache.geode";
  static final String GEMFIRE = "com.gemstone.gemfire";

  static final String ALWAYS_CONVERT_CLASSES_NAME =
      GeodeGlossary.GEMFIRE_PREFIX + "old-client-support.convert-all";

  /** whether to always convert new package names to old on outgoing serialization */
  static final boolean ALWAYS_CONVERT_CLASSES = Boolean.getBoolean(ALWAYS_CONVERT_CLASSES_NAME);

  private final Map<String, String> oldClassNamesToNew = new ConcurrentHashMap<>();
  private final Map<String, String> newClassNamesToOld = new ConcurrentHashMap<>();

  /** returns the cache's OldClientSupportService */
  public static OldClientSupportService getService(Cache cache) {
    return (OldClientSupportService) ((InternalCache) cache)
        .getService(OldClientSupportService.class);
  }

  @Override
  public boolean init(final Cache cache) {
    InternalDataSerializer.setOldClientSupportService(this);
    return true;
  }

  @Override
  public Class<? extends CacheService> getInterface() {
    return OldClientSupportService.class;
  }

  @Override
  public CacheServiceMBeanBase getMBean() {
    // no mbean services provided
    return null;
  }

  @Override
  public String processIncomingClassName(String name) {
    // tcpserver was moved to a different package in Geode.
    String oldPackage = "com.gemstone.org.jgroups.stack.tcpserver";
    String newPackage = "org.apache.geode.distributed.internal.tcpserver";
    if (name.startsWith(oldPackage)) {
      String cached = oldClassNamesToNew.get(name);
      if (cached == null) {
        cached = newPackage + name.substring(oldPackage.length());
        oldClassNamesToNew.put(name, cached);
      }
      return cached;
    }
    return processClassName(name, GEMFIRE, GEODE, oldClassNamesToNew);
  }


  @Override
  public String processIncomingClassName(String name, DataInput in) {
    return processIncomingClassName(name);
  }


  @Override
  public String processOutgoingClassName(String name, DataOutput out) {
    // tcpserver was moved to a different package
    String oldPackage = "com.gemstone.org.jgroups.stack.tcpserver";
    String newPackage = "org.apache.geode.distributed.internal.tcpserver";
    if (name.startsWith(newPackage)) {
      return oldPackage + name.substring(newPackage.length());
    }
    if (ALWAYS_CONVERT_CLASSES) {
      return processClassName(name, GEODE, GEMFIRE, newClassNamesToOld);
    }
    // if the client is old then it needs com.gemstone.gemfire package names
    if (out instanceof VersionedDataOutputStream) {
      VersionedDataOutputStream vout = (VersionedDataOutputStream) out;
      KnownVersion version = vout.getVersion();
      if (version != null && version.isOlderThan(KnownVersion.GFE_90)) {
        return processClassName(name, GEODE, GEMFIRE, newClassNamesToOld);
      }
    }
    return name;
  }

  @Override
  public Throwable getThrowable(Throwable theThrowable, KnownVersion clientVersion) {

    if (theThrowable == null) {
      return theThrowable;
    }

    String className = theThrowable.getClass().getName();

    // GEODE-9452, backward compatibility for authentication expiration
    if (clientVersion.isOlderThan(KnownVersion.GEODE_1_14_0)) {
      if (className.equals(AuthenticationExpiredException.class.getName())) {
        return new AuthenticationRequiredException("User authorization attributes not found.");
      }
    }

    if (clientVersion.isNotOlderThan(KnownVersion.GFE_90)) {
      return theThrowable;
    }



    // this class has been renamed, so it cannot be automatically translated
    // during java deserialization
    if (className.equals("org.apache.geode.cache.execute.EmptyRegionFunctionException")) {
      return new EmtpyRegionFunctionException(theThrowable.getMessage(), theThrowable.getCause());
    }

    // other exceptions will be translated automatically by receivers
    return theThrowable;
  }


  private String processClassName(String p_className, String oldPackage, String newPackage,
      Map<String, String> cache) {
    String cached = cache.get(p_className);
    if (cached != null) {
      return cached;
    }

    String className = p_className;

    if (className.startsWith(oldPackage)) {
      className = newPackage + className.substring(oldPackage.length());

    } else if (className.startsWith("[") && className.contains("[L" + oldPackage)) {
      int idx = className.indexOf("[L") + 2;
      className =
          className.substring(0, idx) + newPackage + className.substring(idx, oldPackage.length());
    }

    if (className != p_className) {
      cache.put(p_className, className);
    }

    return className;
  }


}
