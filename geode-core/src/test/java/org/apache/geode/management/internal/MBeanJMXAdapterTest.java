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
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.powermock.api.mockito.PowerMockito.mock;
import static org.powermock.api.mockito.PowerMockito.mockStatic;
import static org.powermock.api.mockito.PowerMockito.when;

import javax.management.InstanceNotFoundException;
import javax.management.MBeanServer;
import javax.management.ObjectName;

import org.apache.logging.log4j.Logger;
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
@PowerMockIgnore("javax.management.*")
@SuppressStaticInitializationFor({"org.apache.geode.internal.logging.LogService",
    "org.apache.geode.distributed.internal.InternalDistributedSystem"})
public class MBeanJMXAdapterTest {

  @Test
  public void unregisterMBeanInstanceNotFoundMessageLogged() throws Exception {
    mockStatic(InternalDistributedSystem.class);
    when(InternalDistributedSystem.getConnectedInstance())
        .thenReturn(mock(InternalDistributedSystem.class));

    mockStatic(LogService.class);
    Logger logger = mock(Logger.class);
    when(logger.isDebugEnabled()).thenReturn(true);
    when(LogService.getLogger()).thenReturn(logger);

    MBeanServer mBeanServer = mock(MBeanServer.class);
    final ObjectName objectName = new ObjectName("d:type=Foo,name=Bar");
    when(mBeanServer.isRegistered(objectName)).thenReturn(true);

    // Mock unregisterMBean to throw the InstanceNotFoundException, indicating that the MBean
    // has already been unregistered
    doThrow(new InstanceNotFoundException()).when(mBeanServer).unregisterMBean(objectName);

    MBeanJMXAdapter mBeanJMXAdapter = new MBeanJMXAdapter();
    MBeanJMXAdapter.mbeanServer = mBeanServer;

    mBeanJMXAdapter.unregisterMBean(objectName);

    // InstanceNotFoundException should just log a debug message as it is essentially a no-op
    // during unregistration
    verify(logger, times(1)).debug(any(String.class));
  }
}
