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

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.powermock.api.mockito.PowerMockito.mock;
import static org.powermock.api.mockito.PowerMockito.mockStatic;
import static org.powermock.api.mockito.PowerMockito.when;

import javax.management.InstanceAlreadyExistsException;
import javax.management.InstanceNotFoundException;
import javax.management.MBeanServer;
import javax.management.ObjectName;

import org.apache.logging.log4j.Logger;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.powermock.core.classloader.annotations.PowerMockIgnore;
import org.powermock.core.classloader.annotations.PrepareForTest;
import org.powermock.core.classloader.annotations.SuppressStaticInitializationFor;
import org.powermock.modules.junit4.PowerMockRunner;

import org.apache.geode.distributed.internal.InternalDistributedSystem;
import org.apache.geode.internal.logging.LogService;

@RunWith(PowerMockRunner.class)
@PrepareForTest({InternalDistributedSystem.class, LogService.class})
@PowerMockIgnore({"javax.management.*", "javax.script.*"})
@SuppressStaticInitializationFor({"org.apache.geode.internal.logging.LogService",
    "org.apache.geode.distributed.internal.InternalDistributedSystem"})
public class MBeanJMXAdapterTest {
  private ObjectName objectName;
  private MBeanServer mockMBeanServer;
  private Logger mockLogger;

  @Before
  public void setUp() throws Exception {
    mockStatic(InternalDistributedSystem.class);
    when(InternalDistributedSystem.getConnectedInstance())
        .thenReturn(mock(InternalDistributedSystem.class));

    mockStatic(LogService.class);
    mockLogger = mock(Logger.class);
    when(mockLogger.isDebugEnabled()).thenReturn(true);
    when(LogService.getLogger()).thenReturn(mockLogger);

    mockMBeanServer = mock(MBeanServer.class);
    objectName = new ObjectName("d:type=Foo,name=Bar");
  }

  @Test
  public void unregisterMBeanInstanceNotFoundMessageLogged() throws Exception {
    // This mocks the race condition where the server indicates that the object is registered,
    // but when we go to unregister it, it has already been unregistered.
    when(mockMBeanServer.isRegistered(objectName)).thenReturn(true);

    // Mock unregisterMBean to throw the InstanceNotFoundException, indicating that the MBean
    // has already been unregistered
    doThrow(new InstanceNotFoundException()).when(mockMBeanServer).unregisterMBean(objectName);

    MBeanJMXAdapter mBeanJMXAdapter = new MBeanJMXAdapter();
    MBeanJMXAdapter.mbeanServer = mockMBeanServer;

    mBeanJMXAdapter.unregisterMBean(objectName);

    // InstanceNotFoundException should just log a debug message as it is essentially a no-op
    // during unregistration
    verify(mockLogger, times(1)).warn(anyString());
  }

  @Test
  public void registerMBeanProxyInstanceNotFoundMessageLogged() throws Exception {
    // This mocks the race condition where the server indicates that the object is unregistered,
    // but when we go to register it, it has already been register.
    when(mockMBeanServer.isRegistered(objectName)).thenReturn(false);

    // Mock unregisterMBean to throw the InstanceAlreadyExistsException, indicating that the MBean
    // has already been unregistered
    doThrow(new InstanceAlreadyExistsException()).when(mockMBeanServer)
        .registerMBean(any(Object.class), eq(objectName));

    MBeanJMXAdapter mBeanJMXAdapter = new MBeanJMXAdapter();
    MBeanJMXAdapter.mbeanServer = mockMBeanServer;

    mBeanJMXAdapter.registerMBeanProxy(mock(Object.class), objectName);

    // InstanceNotFoundException should just log a debug message as it is essentially a no-op
    // during registration
    verify(mockLogger, times(1)).warn(anyString());
  }
}
