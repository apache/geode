/*=========================================================================
 * Copyright (c) 2010-2014 Pivotal Software, Inc. All Rights Reserved.
 * This product is protected by U.S. and international copyright
 * and intellectual property laws. Pivotal products are covered by
 * one or more patents listed at http://www.pivotal.io/patents.
 *=========================================================================
 */
package com.gemstone.gemfire.management.internal;

import java.net.InetAddress;
import java.net.UnknownHostException;

import com.gemstone.gemfire.CancelCriterion;
import com.gemstone.gemfire.distributed.Locator;
import com.gemstone.gemfire.distributed.internal.DM;
import com.gemstone.gemfire.distributed.internal.DistributionAdvisee;
import com.gemstone.gemfire.distributed.internal.DistributionAdvisor;
import com.gemstone.gemfire.distributed.internal.DistributionConfig;
import com.gemstone.gemfire.distributed.internal.InternalDistributedSystem;
import com.gemstone.gemfire.distributed.internal.DistributionAdvisor.Profile;
import com.gemstone.gemfire.internal.SocketCreator;
import com.gemstone.gemfire.internal.cache.GemFireCacheImpl;
import com.gemstone.gemfire.internal.cache.control.MemoryEventImpl;
import com.gemstone.gemfire.internal.cache.control.ResourceAdvisor.ResourceManagerProfile;
import com.gemstone.gemfire.management.ManagementService;
import com.gemstone.gemfire.management.internal.JmxManagerAdvisor.JmxManagerProfile;

/**
 * 
 * @author darrel
 * @since 7.0
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
