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
package org.apache.geode.internal.cache.tier.sockets.command;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.ArgumentMatchers.isA;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.io.IOException;

import org.junit.Before;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.mockito.ArgumentCaptor;
import org.mockito.InjectMocks;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;

import org.apache.geode.CancelCriterion;
import org.apache.geode.cache.PartitionAttributes;
import org.apache.geode.cache.TransactionException;
import org.apache.geode.cache.operations.KeySetOperationContext;
import org.apache.geode.internal.cache.InternalCache;
import org.apache.geode.internal.cache.LocalRegion;
import org.apache.geode.internal.cache.tier.CachedRegionHelper;
import org.apache.geode.internal.cache.tier.sockets.CacheServerStats;
import org.apache.geode.internal.cache.tier.sockets.ChunkedMessage;
import org.apache.geode.internal.cache.tier.sockets.Message;
import org.apache.geode.internal.cache.tier.sockets.Part;
import org.apache.geode.internal.cache.tier.sockets.ServerConnection;
import org.apache.geode.internal.security.AuthorizeRequest;
import org.apache.geode.internal.security.SecurityService;
import org.apache.geode.security.NotAuthorizedException;
import org.apache.geode.security.ResourcePermission.Operation;
import org.apache.geode.security.ResourcePermission.Resource;
import org.apache.geode.test.junit.categories.ClientServerTest;

@Category({ClientServerTest.class})
public class KeySetTest {

  private static final String REGION_NAME = "region1";
  private static final String KEY = "key1";
  private static final Object CALLBACK_ARG = "arg";
  private static final byte[] EVENT = new byte[8];

  @Mock
  private SecurityService securityService;
  @Mock
  private Message message;
  @Mock
  private ServerConnection serverConnection;
  @Mock
  private AuthorizeRequest authzRequest;
  @Mock
  private LocalRegion region;
  @Mock
  private InternalCache cache;
  @Mock
  private ChunkedMessage chunkedResponseMessage;
  @Mock
  private Part regionNamePart;
  @Mock
  private KeySetOperationContext keySetOperationContext;
  @InjectMocks
  private KeySet keySet;

  @Before
  public void setUp() throws Exception {
    keySet = new KeySet();
    MockitoAnnotations.initMocks(this);

    when(authzRequest.keySetAuthorize(eq(REGION_NAME)))
        .thenReturn(keySetOperationContext);

    when(cache.getRegion(isA(String.class))).thenReturn(region);
    when(cache.getCancelCriterion()).thenReturn(mock(CancelCriterion.class));

    when(message.getPart(eq(0))).thenReturn(regionNamePart);

    when(regionNamePart.getCachedString()).thenReturn(REGION_NAME);

    when(serverConnection.getCache()).thenReturn(cache);
    when(serverConnection.getCacheServerStats()).thenReturn(mock(CacheServerStats.class));
    when(serverConnection.getAuthzRequest()).thenReturn(authzRequest);
    when(serverConnection.getCachedRegionHelper()).thenReturn(mock(CachedRegionHelper.class));
    when(serverConnection.getChunkedResponseMessage()).thenReturn(chunkedResponseMessage);
  }

  @Test
  public void retryKeySet_doesNotWriteTransactionException_ifIsNotInTransaction() throws Exception {
    long startTime = 0; // arbitrary value
    TestableKeySet keySet = new TestableKeySet();
    keySet.setIsInTransaction(false);
    when(message.isRetry()).thenReturn(true);
    when(region.getPartitionAttributes()).thenReturn(mock(PartitionAttributes.class));

    keySet.cmdExecute(message, serverConnection, securityService, startTime);

    assertThat(keySet.exceptionSentToClient).isNull();
  }

  @Test
  public void nonRetryKeySet_doesNotWriteTransactionException() throws Exception {
    long startTime = 0; // arbitrary value
    TestableKeySet keySet = new TestableKeySet();
    keySet.setIsInTransaction(true);
    when(message.isRetry()).thenReturn(false);
    when(region.getPartitionAttributes()).thenReturn(mock(PartitionAttributes.class));

    keySet.cmdExecute(message, serverConnection, securityService, startTime);

    assertThat(keySet.exceptionSentToClient).isNull();
  }

  @Test
  public void retryKeySet_doesNotWriteTransactionException_ifIsInTransactionAndIsNotPartitionedRegion()
      throws Exception {
    long startTime = 0; // arbitrary value
    TestableKeySet keySet = new TestableKeySet();
    keySet.setIsInTransaction(true);
    when(message.isRetry()).thenReturn(true);
    when(region.getPartitionAttributes()).thenReturn(null);

    keySet.cmdExecute(message, serverConnection, securityService, startTime);

    assertThat(keySet.exceptionSentToClient).isNull();
  }

  @Test
  public void retryKeySet_writesTransactionException_ifIsInTransactionAndIsPartitionedRegion()
      throws Exception {
    long startTime = 0; // arbitrary value
    TestableKeySet keySet = new TestableKeySet();
    keySet.setIsInTransaction(true);
    when(message.isRetry()).thenReturn(true);
    when(region.getPartitionAttributes()).thenReturn(mock(PartitionAttributes.class));

    keySet.cmdExecute(message, serverConnection, securityService, startTime);

    assertThat(keySet.exceptionSentToClient).isInstanceOf(TransactionException.class).hasMessage(
        "Failover on a set operation of a partitioned region is not allowed in a transaction.");
  }

  @Test
  public void noSecurityShouldSucceed() throws Exception {
    when(securityService.isClientSecurityRequired()).thenReturn(false);

    keySet.cmdExecute(message, serverConnection, securityService, 0);

    verify(chunkedResponseMessage).sendChunk(serverConnection);
  }

  @Test
  public void integratedSecurityShouldSucceedIfAuthorized() throws Exception {
    when(securityService.isClientSecurityRequired()).thenReturn(true);
    when(securityService.isIntegratedSecurity()).thenReturn(true);

    keySet.cmdExecute(message, serverConnection, securityService, 0);

    verify(securityService).authorize(Resource.DATA, Operation.READ, REGION_NAME);
    verify(chunkedResponseMessage).sendChunk(serverConnection);
  }

  @Test
  public void integratedSecurityShouldFailIfNotAuthorized() throws Exception {
    when(securityService.isClientSecurityRequired()).thenReturn(true);
    when(securityService.isIntegratedSecurity()).thenReturn(true);
    doThrow(new NotAuthorizedException("")).when(securityService).authorize(Resource.DATA,
        Operation.READ, REGION_NAME);

    keySet.cmdExecute(message, serverConnection, securityService, 0);

    verify(securityService).authorize(Resource.DATA, Operation.READ, REGION_NAME);
    verify(chunkedResponseMessage).sendChunk(serverConnection);
  }

  @Test
  public void oldSecurityShouldSucceedIfAuthorized() throws Exception {
    when(securityService.isClientSecurityRequired()).thenReturn(true);
    when(securityService.isIntegratedSecurity()).thenReturn(false);

    keySet.cmdExecute(message, serverConnection, securityService, 0);

    verify(authzRequest).keySetAuthorize(eq(REGION_NAME));
    verify(chunkedResponseMessage).sendChunk(serverConnection);
  }

  @Test
  public void oldSecurityShouldFailIfNotAuthorized() throws Exception {
    when(securityService.isClientSecurityRequired()).thenReturn(true);
    when(securityService.isIntegratedSecurity()).thenReturn(false);
    doThrow(new NotAuthorizedException("")).when(authzRequest)
        .keySetAuthorize(eq(REGION_NAME));

    keySet.cmdExecute(message, serverConnection, securityService, 0);

    verify(authzRequest).keySetAuthorize(eq(REGION_NAME));

    ArgumentCaptor<NotAuthorizedException> argument =
        ArgumentCaptor.forClass(NotAuthorizedException.class);
    verify(chunkedResponseMessage).addObjPart(argument.capture());
    assertThat(argument.getValue()).isExactlyInstanceOf(NotAuthorizedException.class);
    verify(chunkedResponseMessage).sendChunk(eq(serverConnection));
  }

  private class TestableKeySet extends KeySet {
    private boolean isInTransaction = false;
    public Throwable exceptionSentToClient;

    public void setIsInTransaction(boolean isInTransaction) {
      this.isInTransaction = isInTransaction;
    }

    @Override
    public boolean isInTransaction() {
      return isInTransaction;
    }

    @Override
    protected void keySetWriteChunkedException(Message clientMessage, Throwable ex,
        ServerConnection serverConnection) throws IOException {
      exceptionSentToClient = ex;
    }
  }
}
