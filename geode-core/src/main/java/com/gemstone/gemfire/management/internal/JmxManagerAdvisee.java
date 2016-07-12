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
package com.gemstone.gemfire.management.internal;

import com.gemstone.gemfire.CancelCriterion;
import com.gemstone.gemfire.distributed.internal.*;
import com.gemstone.gemfire.distributed.internal.DistributionAdvisor.Profile;
import com.gemstone.gemfire.internal.net.SocketCreator;
import com.gemstone.gemfire.internal.cache.GemFireCacheImpl;
import com.gemstone.gemfire.management.ManagementService;
import com.gemstone.gemfire.management.internal.JmxManagerAdvisor.JmxManagerProfile;

import java.net.UnknownHostException;

/**
 * 
 * @since GemFire 7.0
 */
public class JmxManagerAdvisee implements DistributionAdvisee {

  private final int serialNumber;
  private final GemFireCacheImpl cache;
  private JmxManagerProfile myMostRecentProfile;
  
  public JmxManagerAdvisee(GemFireCacheImpl cache) {
    this.serialNumber = DistributionAdvisor.createSerialNumber();
    this.cache = cache;
  }
  @Override
  public DM getDistributionManager() {
    return this.cache.getDistributionManager();
  }

  @Override
  public CancelCriterion getCancelCriterion() {
    return this.cache.getCancelCriterion();
  }

  @Override
  public DistributionAdvisor getDistributionAdvisor() {
    return this.cache.getJmxManagerAdvisor();
  }

  @Override
  public Profile getProfile() {
    return this.cache.getJmxManagerAdvisor().createProfile();
  }

  @Override
  public DistributionAdvisee getParentAdvisee() {
    return null;
  }

  @Override
  public InternalDistributedSystem getSystem() {
    return this.cache.getSystem();
  }

  @Override
  public String getName() {
    return this.cache.getName();
  }

  @Override
  public String getFullPath() {
    return "JmxManager";
  }

  @Override
  public void fillInProfile(Profile profile) {
    assert profile instanceof JmxManagerProfile;
    JmxManagerProfile jmxp = (JmxManagerProfile) profile;
    DistributionConfig dc = getSystem().getConfig();
    boolean jmxManager = dc.getJmxManager();
    String host = "";
    int port = 0;
    boolean ssl = false;
    boolean started = false;
    SystemManagementService service = (SystemManagementService)ManagementService.getExistingManagementService(this.cache);
    if (service != null) {
      jmxManager = service.isManagerCreated();
      started = service.isManager();
    }
    if (jmxManager) {
      port = dc.getJmxManagerPort();
      boolean usingJdkConfig = false;
      if (port == 0) {
        port = Integer.getInteger("com.sun.management.jmxremote.port", 0);
        if (port != 0) {
          usingJdkConfig = true;
          ssl = true; // the jdk default
          if (System.getProperty("com.sun.management.jmxremote.ssl") != null) {
            ssl = Boolean.getBoolean("com.sun.management.jmxremote.ssl");
          }
        }
      }
      if (port != 0) {
        if (!usingJdkConfig) {
          ssl = dc.getJmxManagerSSLEnabled();
          host = dc.getJmxManagerHostnameForClients();
          if (host == null || host.equals("")) {
            host = dc.getJmxManagerBindAddress();
          }
        }
        if (host == null || host.equals("")) {
          try {
            host = SocketCreator.getLocalHost().getHostAddress(); // fixes 46317
          } catch (UnknownHostException ex) {
            host = "127.0.0.1";
          }
        }
      }
    }
    jmxp.setInfo(jmxManager, host, port, ssl, started);
    this.myMostRecentProfile = jmxp;
  }

  @Override
  public int getSerialNumber() {
    return this.serialNumber;
  }
  
  public JmxManagerProfile getMyMostRecentProfile() {
    return this.myMostRecentProfile;
  }
  void initProfile(JmxManagerProfile p) {
    this.myMostRecentProfile = p;
  }

}
