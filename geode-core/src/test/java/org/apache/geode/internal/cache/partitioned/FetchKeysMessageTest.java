package org.apache.geode.internal.cache.partitioned;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.same;
import static org.mockito.Mockito.RETURNS_DEEP_STUBS;
import static org.mockito.Mockito.inOrder;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.when;

import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.mockito.ArgumentCaptor;
import org.mockito.InOrder;

import org.apache.geode.distributed.internal.DistributionManager;
import org.apache.geode.distributed.internal.membership.InternalDistributedMember;
import org.apache.geode.internal.cache.PartitionedRegion;
import org.apache.geode.internal.cache.TXManagerImpl;
import org.apache.geode.internal.cache.TXStateProxy;
import org.apache.geode.test.junit.categories.UnitTest;

@Category(UnitTest.class)
public class FetchKeysMessageTest {

  @Test
  public void sendsMessageWithoutTransaction_ifTransactionIsHostedLocally() throws Exception {
    PartitionedRegion region = mock(PartitionedRegion.class, RETURNS_DEEP_STUBS);
    DistributionManager distributionManager = mock(DistributionManager.class);
    TXStateProxy txStateProxy = mock(TXStateProxy.class);
    TXManagerImpl txManager = mock(TXManagerImpl.class);
    InternalDistributedMember recipient = mock(InternalDistributedMember.class);

    when(region.getDistributionManager()).thenReturn(distributionManager);
    when(region.getCache().getTxManager()).thenReturn(txManager);
    when(txManager.getTXState()).thenReturn(txStateProxy);
    when(txManager.pauseTransaction()).thenReturn(txStateProxy);
    when(txStateProxy.isRealDealLocal()).thenReturn(true);
    when(txStateProxy.isDistTx()).thenReturn(false);

    ArgumentCaptor<FetchKeysMessage> sentMessage = ArgumentCaptor.forClass(FetchKeysMessage.class);

    FetchKeysMessage.send(recipient, region, 1, false);

    InOrder inOrder = inOrder(txManager, distributionManager);
    inOrder.verify(txManager, times(1)).pauseTransaction();
    inOrder.verify(distributionManager, times(1)).putOutgoing(sentMessage.capture());
    inOrder.verify(txManager, times(1)).unpauseTransaction(same(txStateProxy));

    assertThat(sentMessage.getValue().getTXUniqId()).isEqualTo(TXManagerImpl.NOTX);
  }
}
