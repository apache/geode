package org.apache.geode.internal.cache.partitioned;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.same;
import static org.mockito.Mockito.RETURNS_DEEP_STUBS;
import static org.mockito.Mockito.inOrder;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import org.junit.Before;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.mockito.ArgumentCaptor;
import org.mockito.InOrder;
import org.mockito.Mock;
import org.mockito.Mockito;

import org.apache.geode.distributed.DistributedSystem;
import org.apache.geode.distributed.internal.DistributionConfig;
import org.apache.geode.distributed.internal.DistributionManager;
import org.apache.geode.distributed.internal.InternalDistributedSystem;
import org.apache.geode.distributed.internal.membership.InternalDistributedMember;
import org.apache.geode.internal.cache.CachePerfStats;
import org.apache.geode.internal.cache.InternalCache;
import org.apache.geode.internal.cache.PartitionedRegion;
import org.apache.geode.internal.cache.TXId;
import org.apache.geode.internal.cache.TXManagerImpl;
import org.apache.geode.internal.cache.TXStateProxy;
import org.apache.geode.test.junit.categories.UnitTest;

@Category(UnitTest.class)
public class FetchKeysMessageTest {

  private PartitionedRegion region;
  private DistributionManager distributionManager;
  private TXStateProxy txStateProxy;
  private TXManagerImpl txManager;
  private InternalDistributedMember recipient;
  private ArgumentCaptor<FetchKeysMessage> sentMessage;

  @Before
  public void setup() {
    recipient = mock(InternalDistributedMember.class);
    txStateProxy = mock(TXStateProxy.class);
    region = mock(PartitionedRegion.class, RETURNS_DEEP_STUBS);
    distributionManager = mock(DistributionManager.class);
    InternalDistributedSystem distributedSystem = mock(InternalDistributedSystem.class);
    when(distributedSystem.getDistributionManager()).thenReturn(distributionManager);

    InternalCache cache = mock(InternalCache.class, RETURNS_DEEP_STUBS);
    when(cache.getDistributedSystem()).thenReturn(distributedSystem);
    
    // The constructor sets the new tx manager as currentInstance
    txManager = spy(new TXManagerImpl(mock(CachePerfStats.class), cache));

    when(cache.getTxManager()).thenReturn(txManager);
    
    sentMessage = ArgumentCaptor.forClass(FetchKeysMessage.class);

    when(distributedSystem.getOriginalConfig()).thenReturn(mock(DistributionConfig.class));
    when(txStateProxy.isInProgress()).thenReturn(true);
    when(region.getDistributionManager()).thenReturn(distributionManager);
    when(region.getCache()).thenReturn(cache);
    
    txManager.setTXState(txStateProxy);
    txManager.setDistributed(false);
  }
  
  @Test
  public void sendsWithTransactionPaused_ifTransactionIsHostedLocally() throws Exception {
    // Transaction is locally hosted
    when(txStateProxy.isRealDealLocal()).thenReturn(true);
    when(txStateProxy.isDistTx()).thenReturn(false);
    
    FetchKeysMessage.send(recipient, region, 1, false);

    InOrder inOrder = inOrder(txManager, distributionManager);
    inOrder.verify(txManager, times(1)).pauseTransaction();
    inOrder.verify(distributionManager, times(1)).putOutgoing(sentMessage.capture());
    inOrder.verify(txManager, times(1)).unpauseTransaction(same(txStateProxy));

    assertThat(sentMessage.getValue().getTXUniqId()).isEqualTo(TXManagerImpl.NOTX);
  }

  @Test
  public void sendsWithoutPausingTransaction_ifTransactionIsNotHostedLocally() throws Exception {
    // Transaction is not locally hosted
    when(txStateProxy.isRealDealLocal()).thenReturn(false);

    int uniqueId = 99;
    TXId txID = new TXId(recipient, uniqueId);
    when(txStateProxy.getTxId()).thenReturn(txID);

    FetchKeysMessage.send(recipient, region, 1, false);

    verify(distributionManager, times(1)).putOutgoing(sentMessage.capture());
    assertThat(sentMessage.getValue().getTXUniqId()).isEqualTo(uniqueId);
    verify(txManager, never()).pauseTransaction();
    verify(txManager, never()).unpauseTransaction(any());
  }
}
