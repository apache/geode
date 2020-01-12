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

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import javax.management.InstanceAlreadyExistsException;
import javax.management.InstanceNotFoundException;
import javax.management.MBeanServer;
import javax.management.ObjectName;

import org.junit.Before;
import org.junit.Test;

import org.apache.geode.distributed.DistributedMember;

/**
 * Unique tests for {@link MBeanJMXAdapter}.
 */
public class MBeanJMXAdapterTest {

  private static final String UNIQUE_ID = "unique-id";

  private ObjectName objectName;
  private MBeanServer mockMBeanServer;
  private DistributedMember distMember;

  @Before
  public void setUp() throws Exception {
    mockMBeanServer = mock(MBeanServer.class);
    objectName = new ObjectName("d:type=Foo,name=Bar");
    distMember = mock(DistributedMember.class);
    when(distMember.getUniqueId()).thenReturn(UNIQUE_ID);
  }

  @Test
  public void unregisterMBeanInstanceNotFoundMessageLogged() throws Exception {
    // This mocks the race condition where the server indicates that the object is registered,
    // but when we go to unregister it, it has already been unregistered.
    when(mockMBeanServer.isRegistered(objectName)).thenReturn(true);

    // Mock unregisterMBean to throw the InstanceNotFoundException, indicating that the MBean
    // has already been unregistered
    doThrow(new InstanceNotFoundException()).when(mockMBeanServer).unregisterMBean(objectName);

    MBeanJMXAdapter mBeanJMXAdapter = spy(new MBeanJMXAdapter(distMember));
    MBeanJMXAdapter.mbeanServer = mockMBeanServer;

    mBeanJMXAdapter.unregisterMBean(objectName);

    // InstanceNotFoundException should just log a debug message as it is essentially a no-op
    // during unregistration
    verify(mBeanJMXAdapter, times(1)).logRegistrationWarning(any(ObjectName.class), eq(false));
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

    MBeanJMXAdapter mBeanJMXAdapter = spy(new MBeanJMXAdapter(distMember));
    MBeanJMXAdapter.mbeanServer = mockMBeanServer;

    mBeanJMXAdapter.registerMBeanProxy(objectName, objectName);

    // InstanceNotFoundException should just log a debug message as it is essentially a no-op
    // during registration
    verify(mBeanJMXAdapter, times(1)).logRegistrationWarning(any(ObjectName.class), eq(true));
  }

  @Test
  public void getMemberNameOrUniqueIdReturnsNameIfProvided() {
    String memberName = "member-name";
    when(distMember.getName()).thenReturn(memberName);

    String result = MBeanJMXAdapter.getMemberNameOrUniqueId(distMember);

    assertThat(result).isEqualTo(memberName);
  }

  @Test
  public void getMemberNameOrUniqueIdReturnsUniqueIdIfNameIsNotProvided() {
    when(distMember.getName()).thenReturn("");

    String result = MBeanJMXAdapter.getMemberNameOrUniqueId(distMember);

    assertThat(result).isEqualTo(UNIQUE_ID);
  }
}
