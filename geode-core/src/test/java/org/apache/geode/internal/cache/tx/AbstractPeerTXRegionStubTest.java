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

package org.apache.geode.internal.cache.tx;

import static org.junit.Assert.fail;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import org.junit.After;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Rule;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.rules.ExpectedException;
import org.mockito.Mockito;

import org.apache.geode.CancelCriterion;
import org.apache.geode.cache.CacheClosedException;
import org.apache.geode.cache.Region.Entry;
import org.apache.geode.cache.RegionDestroyedException;
import org.apache.geode.cache.TransactionDataNodeHasDepartedException;
import org.apache.geode.cache.TransactionDataNotColocatedException;
import org.apache.geode.distributed.internal.ClusterDistributionManager;
import org.apache.geode.distributed.internal.InternalDistributedSystem;
import org.apache.geode.distributed.internal.ReplyException;
import org.apache.geode.distributed.internal.membership.InternalDistributedMember;
import org.apache.geode.internal.cache.DistributedPutAllOperation;
import org.apache.geode.internal.cache.DistributedRemoveAllOperation;
import org.apache.geode.internal.cache.EntryEventImpl;
import org.apache.geode.internal.cache.InternalRegion;
import org.apache.geode.internal.cache.KeyInfo;
import org.apache.geode.internal.cache.LocalRegion;
import org.apache.geode.internal.cache.RemoteOperationException;
import org.apache.geode.internal.cache.TXStateStub;
import org.apache.geode.internal.cache.tier.sockets.ClientProxyMembershipID;
import org.apache.geode.internal.cache.tier.sockets.VersionedObjectList;
import org.apache.geode.test.junit.categories.UnitTest;

@Category(UnitTest.class)
public class AbstractPeerTXRegionStubTest {
  private AbstractPeerTXRegionStub txrStub;
  private TXStateStub state;
  private LocalRegion region;

  private class TestingAbstractPeerTXRegionStub extends AbstractPeerTXRegionStub {

    private final LocalRegion region;

    private TestingAbstractPeerTXRegionStub(TXStateStub txState, LocalRegion r) {
      super(txState);
      this.region = r;
    }

    @Override
    public void destroyExistingEntry(EntryEventImpl event, boolean cacheWrite,
        Object expectedOldValue) {}

    @Override
    public Entry getEntry(KeyInfo keyInfo, boolean allowTombstone) {
      return null;
    }

    @Override
    public void invalidateExistingEntry(EntryEventImpl event, boolean invokeCallbacks,
        boolean forceNewEntry) {}

    @Override
    public boolean containsKey(KeyInfo keyInfo) {
      return false;
    }

    @Override
    public boolean containsValueForKey(KeyInfo keyInfo) {
      return false;
    }

    @Override
    public Object findObject(KeyInfo keyInfo, boolean isCreate, boolean generateCallbacks,
        Object value, boolean preferCD, ClientProxyMembershipID requestingClient,
        EntryEventImpl clientEvent) {
      return null;
    }

    @Override
    public Object getEntryForIterator(KeyInfo keyInfo, boolean allowTombstone) {
      return null;
    }

    @Override
    public boolean putEntry(EntryEventImpl event, boolean ifNew, boolean ifOld,
        Object expectedOldValue, boolean requireOldValue, long lastModified,
        boolean overwriteDestroyed) {
      return false;
    }

    @Override
    public int entryCount() {
      return 0;
    }

    @Override
    public void postPutAll(DistributedPutAllOperation putallOp, VersionedObjectList successfulPuts,
        InternalRegion region) {}

    @Override
    public void postRemoveAll(DistributedRemoveAllOperation op, VersionedObjectList successfulOps,
        InternalRegion region) {}

    @Override
    public void cleanup() {}

    @Override
    protected InternalRegion getRegion() {
      return this.region;
    }
  }

  @Rule
  public ExpectedException expectedException = ExpectedException.none();

  @BeforeClass
  public static void setUpBeforeClass() throws Exception {}

  @AfterClass
  public static void tearDownAfterClass() throws Exception {}

  @Before
  public void setUp() throws Exception {
    state = mock(TXStateStub.class);
    region = mock(LocalRegion.class);
    txrStub = new TestingAbstractPeerTXRegionStub(state, region);
  }

  @After
  public void tearDown() throws Exception {}

  @Test
  public void getRegionKeysForIterationTranslatesCacheClosedException() {
    expectedException.expect(TransactionDataNodeHasDepartedException.class);

    // Mocking to cause getSystem() to throw exceptions for testing
    // getSystem is called when creating FetchKeysResponse in RemoteFetchKeysResponse.send, which is
    // called from getRegionKeysForIteration
    when((region).getSystem()).thenThrow(CacheClosedException.class);

    txrStub.getRegionKeysForIteration();
    fail(
        "AbstractPeerTXRegionStub expected to transalate CacheClosedException to TransactionDataNodeHasDepartedException ");
  }

  @Test
  public void getRegionKeysForIterationTranslatesRemoteOperationException() {
    expectedException.expect(TransactionDataNodeHasDepartedException.class);


    InternalDistributedSystem system = mock(InternalDistributedSystem.class);
    ClusterDistributionManager manager = mock(ClusterDistributionManager.class);
    when(system.getDistributionManager()).thenReturn(manager);
    when(manager.getCancelCriterion()).thenReturn(mock(CancelCriterion.class));
    InternalDistributedMember target = new InternalDistributedMember("localhost", 1234);

    RemoteFetchKeysMessage.FetchKeysResponse responseProcessor =
        new RemoteFetchKeysMessage.FetchKeysResponse(system, target);
    RemoteFetchKeysMessage.FetchKeysResponse spy = Mockito.spy(responseProcessor);

    Exception replyException = new ReplyException("testing",
        new RemoteOperationException("The cache is closing", new CacheClosedException()));
    doThrow(replyException).when(spy).waitForRepliesUninterruptibly();

    spy.waitForKeys();

    fail(
        "Expected to transalate RemoteOperationException.CacheClosedException to TransactionDataNodeHasDepartedException ");
  }

  @Test
  public void getRegionKeysForIterationTranslatesRegionDestroyedException() {
    expectedException.expect(TransactionDataNotColocatedException.class);

    // Mocking to cause getSystem() to throw exceptions for testing
    // getSystem is called when creating FetchKeysResponse in RemoteFetchKeysResponse.send, which is
    // called from getRegionKeysForIteration
    when((region).getSystem()).thenThrow(RegionDestroyedException.class);

    txrStub.getRegionKeysForIteration();
    fail(
        "AbstractPeerTXRegionStub expected to transalate CacheClosedException to TransactionDataNodeHasDepartedException ");
  }

}
