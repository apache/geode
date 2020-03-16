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
package org.apache.geode.management.internal.beans;

import java.util.ArrayList;
import java.util.List;
import java.util.Set;

import javax.management.Notification;
import javax.management.ObjectName;

import org.apache.geode.LogWriter;
import org.apache.geode.annotations.internal.MakeNotStatic;
import org.apache.geode.distributed.internal.DistributionManager;
import org.apache.geode.distributed.internal.InternalDistributedSystem;
import org.apache.geode.distributed.internal.membership.InternalDistributedMember;
import org.apache.geode.management.CacheServerMXBean;
import org.apache.geode.management.GatewayReceiverMXBean;
import org.apache.geode.management.GatewaySenderMXBean;
import org.apache.geode.management.LockServiceMXBean;
import org.apache.geode.management.MemberMXBean;
import org.apache.geode.management.RegionMXBean;
import org.apache.geode.management.internal.FederationComponent;
import org.apache.geode.management.internal.ProxyListener;
import org.apache.geode.management.internal.SystemManagementService;

/**
 * This class is responsible for creating Aggregate MBeans at GemFire layer. It acts as a
 * ProxyListener and add/remove/update the corresponding MBean aggregates accordingly.
 *
 *
 */
public class MBeanAggregator implements ProxyListener {

  @MakeNotStatic
  private static final List<Class> distributedMBeanList = new ArrayList<>();


  /**
   * Log writer
   */

  protected LogWriter logger;



  /*
   * Static list of MBeans which are currently supported for aggregation
   */
  static {
    distributedMBeanList.add(RegionMXBean.class);
    distributedMBeanList.add(MemberMXBean.class);
    distributedMBeanList.add(LockServiceMXBean.class);
    distributedMBeanList.add(CacheServerMXBean.class);
    distributedMBeanList.add(GatewayReceiverMXBean.class);
    distributedMBeanList.add(GatewaySenderMXBean.class);
  }


  /**
   * private instance of SystemManagementService
   */
  private SystemManagementService service;

  /**
   * Region aggregate handler
   */
  private RegionHandler regionHandler;

  /**
   * Member level aggregate handler
   */
  private MemberHandler memberHandler;

  /**
   * Lock service aggregate handler.
   */
  private LockServiceHandler lockServiceHandler;

  /**
   * Cache server aggregate Handler
   */
  private CacheServerHandler cacheServerHandler;

  /**
   * Gateway receiver handler
   */
  private GatewayReceiverHandler gatewayReceiverHandler;

  /**
   * Gateway sender handler
   */

  private GatewaySenderHandler gatewaySenderHandler;


  private DistributedSystemBridge distributedSystemBridge;

  /**
   * Public constructor.
   *
   */
  public MBeanAggregator(DistributedSystemBridge distributedSystemBridge) {

    this.regionHandler = new RegionHandler();
    this.memberHandler = new MemberHandler();
    this.lockServiceHandler = new LockServiceHandler();
    this.cacheServerHandler = new CacheServerHandler();
    this.gatewayReceiverHandler = new GatewayReceiverHandler();
    this.gatewaySenderHandler = new GatewaySenderHandler();
    this.logger = InternalDistributedSystem.getLogger();
    this.distributedSystemBridge = distributedSystemBridge;


  }

  /**
   *
   * @param interfaceClass class of the proxy interface
   * @return appropriate handler instance to handle the proxy addition or removal
   */
  private AggregateHandler getHandler(Class interfaceClass) {
    if (interfaceClass.equals(RegionMXBean.class)) {
      return regionHandler;
    } else if (interfaceClass.equals(MemberMXBean.class)) {
      return memberHandler;
    } else if (interfaceClass.equals(LockServiceMXBean.class)) {
      return lockServiceHandler;
    } else if (interfaceClass.equals(CacheServerMXBean.class)) {
      return cacheServerHandler;
    } else if (interfaceClass.equals(GatewayReceiverMXBean.class)) {
      return gatewayReceiverHandler;
    } else if (interfaceClass.equals(GatewaySenderMXBean.class)) {
      return gatewaySenderHandler;
    }
    return null;
  }

  @Override
  public void afterCreateProxy(ObjectName objectName, Class interfaceClass, Object proxyObject,
      FederationComponent newVal) {
    if (!distributedMBeanList.contains(interfaceClass)) {
      return;
    }
    AggregateHandler handler = getHandler(interfaceClass);
    handler.handleProxyAddition(objectName, interfaceClass, proxyObject, newVal);
  }

  @Override
  public void afterRemoveProxy(ObjectName objectName, Class interfaceClass, Object proxyObject,
      FederationComponent oldVal) {
    if (!distributedMBeanList.contains(interfaceClass)) {
      return;
    }
    AggregateHandler handler = getHandler(interfaceClass);
    handler.handleProxyRemoval(objectName, interfaceClass, proxyObject, oldVal);
  }

  @Override
  public void afterUpdateProxy(ObjectName objectName, Class interfaceClass, Object proxyObject,
      FederationComponent newVal, FederationComponent oldVal) {
    if (!distributedMBeanList.contains(interfaceClass)) {
      return;
    }
    AggregateHandler handler = getHandler(interfaceClass);
    handler.handleProxyUpdate(objectName, interfaceClass, proxyObject, newVal, oldVal);
  }

  @Override
  public void afterPseudoCreateProxy(ObjectName objectName, Class interfaceClass,
      Object proxyObject, FederationComponent newVal) {
    if (!distributedMBeanList.contains(interfaceClass)) {
      return;
    }
    AggregateHandler handler = getHandler(interfaceClass);
    handler.handlePseudoCreateProxy(objectName, interfaceClass, proxyObject, newVal);
  }

  /**
   * Handler class for CacheServer MBeans only to provide data to Distributed System MBean As of
   * today there wont be any Distributed counterpart of Cache Server MBean
   *
   *
   */
  private class CacheServerHandler implements AggregateHandler {

    @Override
    public void handleProxyAddition(ObjectName objectName, Class interfaceClass, Object proxyObject,
        FederationComponent newVal) {

      CacheServerMXBean serverProxy = (CacheServerMXBean) interfaceClass.cast(proxyObject);
      distributedSystemBridge.addServerToSystem(objectName, serverProxy, newVal);

    }

    @Override
    public void handleProxyRemoval(ObjectName objectName, Class interfaceClass, Object obj,
        FederationComponent oldVal) {
      CacheServerMXBean serverProxy = (CacheServerMXBean) interfaceClass.cast(obj);
      distributedSystemBridge.removeServerFromSystem(objectName, serverProxy, oldVal);

    }

    @Override
    public void handleProxyUpdate(ObjectName objectName, Class interfaceClass, Object proxyObject,
        FederationComponent newVal, FederationComponent oldVal) {
      distributedSystemBridge.updateCacheServer(objectName, newVal, oldVal);
    }

    @Override
    public void handlePseudoCreateProxy(ObjectName objectName, Class interfaceClass,
        Object proxyObject, FederationComponent newVal) {

    }
  }

  /**
   * Handler class for GatewayReceiverHandler MBeans only to provide data to Distributed System
   * MBean As of today there wont be any Distributed counterpart of GatewayReceiverHandler MBean
   *
   *
   */
  private class GatewayReceiverHandler implements AggregateHandler {

    @Override
    public void handleProxyAddition(ObjectName objectName, Class interfaceClass, Object proxyObject,
        FederationComponent newVal) {

      GatewayReceiverMXBean proxy = (GatewayReceiverMXBean) interfaceClass.cast(proxyObject);
      distributedSystemBridge.addGatewayReceiverToSystem(objectName, proxy, newVal);

    }

    @Override
    public void handleProxyRemoval(ObjectName objectName, Class interfaceClass, Object proxyObject,
        FederationComponent oldVal) {
      GatewayReceiverMXBean proxy = (GatewayReceiverMXBean) interfaceClass.cast(proxyObject);
      distributedSystemBridge.removeGatewayReceiverFromSystem(objectName, proxy, oldVal);

    }

    @Override
    public void handleProxyUpdate(ObjectName objectName, Class interfaceClass, Object proxyObject,
        FederationComponent newVal, FederationComponent oldVal) {
      distributedSystemBridge.updateGatewayReceiver(objectName, newVal, oldVal);
    }

    @Override
    public void handlePseudoCreateProxy(ObjectName objectName, Class interfaceClass,
        Object proxyObject, FederationComponent newVal) {

    }
  }



  /**
   * Handler class for GatewayReceiverHandler MBeans only to provide data to Distributed System
   * MBean As of today there wont be any Distributed counterpart of GatewayReceiverHandler MBean
   *
   *
   */
  private class GatewaySenderHandler implements AggregateHandler {

    @Override
    public void handleProxyAddition(ObjectName objectName, Class interfaceClass, Object proxyObject,
        FederationComponent newVal) {

      GatewaySenderMXBean proxy = (GatewaySenderMXBean) interfaceClass.cast(proxyObject);
      distributedSystemBridge.addGatewaySenderToSystem(objectName, proxy, newVal);
    }

    @Override
    public void handleProxyRemoval(ObjectName objectName, Class interfaceClass, Object proxyObject,
        FederationComponent oldVal) {
      GatewaySenderMXBean proxy = (GatewaySenderMXBean) interfaceClass.cast(proxyObject);
      distributedSystemBridge.removeGatewaySenderFromSystem(objectName, proxy, oldVal);
    }

    @Override
    public void handleProxyUpdate(ObjectName objectName, Class interfaceClass, Object proxyObject,
        FederationComponent newVal, FederationComponent oldVal) {
      distributedSystemBridge.updateGatewaySender(objectName, newVal, oldVal);
    }

    @Override
    public void handlePseudoCreateProxy(ObjectName objectName, Class interfaceClass,
        Object proxyObject, FederationComponent newVal) {

    }
  }

  /**
   * Handler class for DistributedRegion
   *
   *
   */
  private class RegionHandler implements AggregateHandler {

    @Override
    public void handleProxyAddition(ObjectName objectName, Class interfaceClass, Object proxyObject,
        FederationComponent newVal) {
      RegionMXBean regionProxy = (RegionMXBean) interfaceClass.cast(proxyObject);
      distributedSystemBridge.addRegion(objectName, regionProxy, newVal);

    }

    @Override
    public void handleProxyRemoval(ObjectName objectName, Class interfaceClass, Object proxyObject,
        FederationComponent oldVal) {
      RegionMXBean regionProxy = (RegionMXBean) interfaceClass.cast(proxyObject);
      distributedSystemBridge.removeRegion(objectName, regionProxy, oldVal);
    }

    @Override
    public void handleProxyUpdate(ObjectName objectName, Class interfaceClass, Object proxyObject,
        FederationComponent newVal, FederationComponent oldVal) {
      distributedSystemBridge.updateRegion(objectName, oldVal, newVal);
    }

    @Override
    public void handlePseudoCreateProxy(ObjectName objectName, Class interfaceClass,
        Object proxyObject, FederationComponent newVal) {

    }
  }

  /**
   * Handler class for Distributed System
   *
   *
   */
  private class MemberHandler implements AggregateHandler {

    @Override
    public void handleProxyAddition(ObjectName objectName, Class interfaceClass, Object proxyObject,
        FederationComponent newVal) {
      MemberMXBean memberProxy = (MemberMXBean) interfaceClass.cast(proxyObject);
      distributedSystemBridge.addMemberToSystem(objectName, memberProxy, newVal);

    }

    @Override
    public void handleProxyRemoval(ObjectName objectName, Class interfaceClass, Object proxyObject,
        FederationComponent oldVal) {
      MemberMXBean memberProxy = (MemberMXBean) interfaceClass.cast(proxyObject);
      distributedSystemBridge.removeMemberFromSystem(objectName, memberProxy, oldVal);
    }

    @Override
    public void handleProxyUpdate(ObjectName objectName, Class interfaceClass, Object proxyObject,
        FederationComponent newVal, FederationComponent oldVal) {
      distributedSystemBridge.updateMember(objectName, newVal, oldVal);
    }

    @Override
    public void handlePseudoCreateProxy(ObjectName objectName, Class interfaceClass,
        Object proxyObject, FederationComponent newVal) {
      distributedSystemBridge.updateMember(objectName, newVal, null);

    }
  }

  /**
   * Handler class for Distributed Lock Service
   *
   *
   */
  private class LockServiceHandler implements AggregateHandler {

    @Override
    public void handleProxyAddition(ObjectName objectName, Class interfaceClass, Object proxyObject,
        FederationComponent newVal) {

      LockServiceMXBean lockServiceProxy = (LockServiceMXBean) interfaceClass.cast(proxyObject);
      distributedSystemBridge.addLockService(objectName, lockServiceProxy, newVal);

    }

    @Override
    public void handleProxyRemoval(ObjectName objectName, Class interfaceClass, Object proxyObject,
        FederationComponent oldVal) {
      LockServiceMXBean lockServiceProxy = (LockServiceMXBean) interfaceClass.cast(proxyObject);
      distributedSystemBridge.removeLockService(objectName, lockServiceProxy, oldVal);
    }

    @Override
    public void handleProxyUpdate(ObjectName objectName, Class interfaceClass, Object proxyObject,
        FederationComponent newVal, FederationComponent oldVal) {}

    @Override
    public void handlePseudoCreateProxy(ObjectName objectName, Class interfaceClass,
        Object proxyObject, FederationComponent newVal) {

    }
  }

  @Override
  public void memberDeparted(DistributionManager distributionManager, InternalDistributedMember id,
      boolean crashed) {
    distributedSystemBridge.memberDeparted(id, crashed);
  }

  @Override
  public void memberJoined(DistributionManager distributionManager, InternalDistributedMember id) {
    distributedSystemBridge.memberJoined(id);

  }

  @Override
  public void quorumLost(DistributionManager distributionManager,
      Set<InternalDistributedMember> failures, List<InternalDistributedMember> remaining) {}

  @Override
  public void memberSuspect(DistributionManager distributionManager, InternalDistributedMember id,
      InternalDistributedMember whoSuspected, String reason) {
    distributedSystemBridge.memberSuspect(id, whoSuspected);
  }

  @Override
  public void handleNotification(Notification notification) {
    distributedSystemBridge.sendSystemLevelNotification(notification);
  }

}
