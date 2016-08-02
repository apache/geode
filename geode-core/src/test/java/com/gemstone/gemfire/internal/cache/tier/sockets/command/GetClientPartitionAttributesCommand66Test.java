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
package com.gemstone.gemfire.internal.cache.tier.sockets.command;

import static org.mockito.Matchers.any;
import static org.mockito.Matchers.eq;
import static org.mockito.Matchers.isA;
import static org.mockito.Mockito.*;

import java.util.Properties;

import org.junit.Before;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.mockito.InjectMocks;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;

import com.gemstone.gemfire.cache.Cache;
import com.gemstone.gemfire.internal.cache.LocalRegion;
import com.gemstone.gemfire.internal.cache.tier.sockets.Message;
import com.gemstone.gemfire.internal.cache.tier.sockets.Part;
import com.gemstone.gemfire.internal.cache.tier.sockets.ServerConnection;
import com.gemstone.gemfire.internal.security.SecurityService;
import com.gemstone.gemfire.test.junit.categories.UnitTest;

@Category(UnitTest.class)
public class GetClientPartitionAttributesCommand66Test {

  private static final String REGION_NAME = "region1";

  @Mock
  private SecurityService securityService;
  @Mock
  private Message message;
  @Mock
  private ServerConnection serverConnection;
  @Mock
  private Cache cache;
  @Mock
  private Part regionNamePart;
  @Mock
  private Message responseMessage;

  @InjectMocks
  private GetClientPartitionAttributesCommand66 getClientPartitionAttributesCommand66;

  @Before
  public void setUp() throws Exception {
    this.getClientPartitionAttributesCommand66 = new GetClientPartitionAttributesCommand66();
    MockitoAnnotations.initMocks(this);

    when(this.cache.getRegion(isA(String.class))).thenReturn(mock(LocalRegion.class));

    when(this.message.getPart(eq(0))).thenReturn(this.regionNamePart);

    when(this.regionNamePart.getString()).thenReturn(REGION_NAME);

    when(this.serverConnection.getCache()).thenReturn(this.cache);
    when(this.serverConnection.getResponseMessage()).thenReturn(this.responseMessage);
  }

  @Test
  public void noSecuirtyShouldSucceed() throws Exception {
    when(this.securityService.isClientSecurityRequired()).thenReturn(false);

    this.getClientPartitionAttributesCommand66.cmdExecute(this.message, this.serverConnection, 0);

    verify(this.responseMessage).send();
  }

}
