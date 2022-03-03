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
package org.apache.geode.management.internal;

import java.net.UnknownHostException;

import org.apache.geode.CancelCriterion;
import org.apache.geode.distributed.internal.DistributionAdvisee;
import org.apache.geode.distributed.internal.DistributionAdvisor;
import org.apache.geode.distributed.internal.DistributionAdvisor.Profile;
import org.apache.geode.distributed.internal.DistributionConfig;
import org.apache.geode.distributed.internal.DistributionManager;
import org.apache.geode.distributed.internal.InternalDistributedSystem;
import org.apache.geode.internal.cache.InternalCacheForClientAccess;
import org.apache.geode.internal.inet.LocalHostUtil;
import org.apache.geode.internal.net.SSLConfig;
import org.apache.geode.internal.net.SSLConfigurationFactory;
import org.apache.geode.internal.security.SecurableCommunicationChannel;
import org.apache.geode.management.ManagementService;
import org.apache.geode.management.internal.JmxManagerAdvisor.JmxManagerProfile;

/**
 * @since GemFire 7.0
 */
public class JmxManagerAdvisee implements DistributionAdvisee {

  private final int serialNumber;
  private final InternalCacheForClientAccess cache;
  private JmxManagerProfile myMostRecentProfile;

  public JmxManagerAdvisee(InternalCacheForClientAccess cache) {
    serialNumber = DistributionAdvisor.createSerialNumber();
    this.cache = cache;
  }

  @Override
  public DistributionManager getDistributionManager() {
    return cache.getDistributionManager();
  }

  @Override
  public CancelCriterion getCancelCriterion() {
    return cache.getCancelCriterion();
  }

  @Override
  public DistributionAdvisor getDistributionAdvisor() {
    return cache.getJmxManagerAdvisor();
  }

  @Override
  public Profile getProfile() {
    return cache.getJmxManagerAdvisor().createProfile();
  }

  @Override
  public DistributionAdvisee getParentAdvisee() {
    return null;
  }

  @Override
  public InternalDistributedSystem getSystem() {
    return cache.getInternalDistributedSystem();
  }

  @Override
  public String getName() {
    return cache.getName();
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
    SystemManagementService service =
        (SystemManagementService) ManagementService.getExistingManagementService(cache);
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
          SSLConfig jmxSSL =
              SSLConfigurationFactory.getSSLConfigForComponent(dc,
                  SecurableCommunicationChannel.JMX);
          ssl = jmxSSL.isEnabled();
          host = dc.getJmxManagerHostnameForClients();
          if (host == null || host.equals("")) {
            host = dc.getJmxManagerBindAddress();
          }
        }
        if (host == null || host.equals("")) {
          try {
            host = LocalHostUtil.getLocalHost().getHostAddress(); // fixes 46317
          } catch (UnknownHostException ex) {
            host = "127.0.0.1";
          }
        }
      }
    }
    jmxp.setInfo(jmxManager, host, port, ssl, started);
    myMostRecentProfile = jmxp;
  }

  @Override
  public int getSerialNumber() {
    return serialNumber;
  }

  public JmxManagerProfile getMyMostRecentProfile() {
    return myMostRecentProfile;
  }

  void initProfile(JmxManagerProfile p) {
    myMostRecentProfile = p;
  }

}
