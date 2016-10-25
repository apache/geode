/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.gemstone.gemfire;

import java.io.DataInput;
import java.io.DataOutput;

import org.apache.geode.cache.Cache;
import org.apache.geode.internal.InternalDataSerializer;
import org.apache.geode.internal.Version;
import org.apache.geode.internal.VersionedDataOutputStream;
import org.apache.geode.internal.cache.CacheService;
import org.apache.geode.internal.cache.InternalCache;
import org.apache.geode.internal.cache.tier.sockets.OldClientSupportService;
import org.apache.geode.management.internal.beans.CacheServiceMBeanBase;

import com.gemstone.gemfire.cache.execute.EmtpyRegionFunctionException;

/**
 * Support for old GemFire clients
 */
public class OldClientSupportProvider implements OldClientSupportService {
  static final String GEODE = "org.apache.geode";
  static final String GEMFIRE = "com.gemstone.gemfire";

  /** returns the cache's OldClientSupportService */
  public static OldClientSupportService getService(Cache cache) {
    return (OldClientSupportService)((InternalCache)cache).getService(OldClientSupportService.class);
  }

  @Override
  public void init(final Cache cache) {
    InternalDataSerializer.setOldClientSupportService(this);
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
    if (name.startsWith(GEMFIRE)) {
      return GEODE + name.substring(GEMFIRE.length());
    }
    return name;
  }


  @Override
  public String processIncomingClassName(String name, DataInput in) {
    // tcpserver was moved to a different package in Geode.  
    String oldPackage = "com.gemstone.org.jgroups.stack.tcpserver";
    String newPackage = "org.apache.geode.distributed.internal.tcpserver";
    if (name.startsWith(oldPackage)) {
      return newPackage + name.substring(oldPackage.length());
    }
    if (name.startsWith(GEMFIRE)) {
      return GEODE + name.substring(GEMFIRE.length());
    }
    return name;
  }


  @Override
  public String processOutgoingClassName(String name, DataOutput out) {
    // tcpserver was moved to a different package in Geode
    String oldPackage = "com.gemstone.org.jgroups.stack.tcpserver";
    String newPackage = "org.apache.geode.distributed.internal.tcpserver";
    if (name.startsWith(newPackage)) {
      return oldPackage + name.substring(newPackage.length());
    }
    // if the client is old then it needs com.gemstone.gemfire package names
    if (out instanceof VersionedDataOutputStream) {
      VersionedDataOutputStream vout = (VersionedDataOutputStream)out;
      Version version = vout.getVersion();
      if (version != null && version.compareTo(Version.GFE_90) < 0) {
        if (name.startsWith(GEODE)) {
          name = GEMFIRE + name.substring(GEODE.length());
        }
      }
    }
    return name;
  }


  /**
   * translates the given exception into one that can be sent to an old GemFire client
   * @param theThrowable the exception to convert
   * @param clientVersion the version of the client
   * @return the exception to give the client
   */
  public Throwable getThrowable(Throwable theThrowable, Version clientVersion) {
    
    if (theThrowable == null) {
      return theThrowable;
    }
    if (clientVersion.compareTo(Version.GFE_90) >= 0) {
      return theThrowable;
    }
    
    String className = theThrowable.getClass().getName();
    
    // this class has been renamed, so it cannot be automatically translated
    // during java deserialization
    if (className.equals("org.apache.geode.cache.execute.EmptyRegionFunctionException")) {
      return new EmtpyRegionFunctionException(theThrowable.getMessage(), theThrowable.getCause());
    }
    
    // other exceptions will be translated automatically by receivers
    return theThrowable;
  }

}
