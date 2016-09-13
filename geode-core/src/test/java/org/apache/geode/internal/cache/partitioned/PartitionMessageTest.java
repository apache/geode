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
package com.gemstone.gemfire.internal.cache.partitioned;

import static org.mockito.Mockito.*;

import java.io.IOException;

import org.junit.Before;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.mockito.internal.stubbing.answers.CallsRealMethods;

import com.gemstone.gemfire.cache.CacheException;
import com.gemstone.gemfire.cache.query.QueryException;
import com.gemstone.gemfire.distributed.internal.DistributionManager;
import com.gemstone.gemfire.internal.cache.DataLocationException;
import com.gemstone.gemfire.internal.cache.GemFireCacheImpl;
import com.gemstone.gemfire.internal.cache.PartitionedRegion;
import com.gemstone.gemfire.internal.cache.TXManagerImpl;
import com.gemstone.gemfire.internal.cache.TXStateProxy;
import com.gemstone.gemfire.internal.cache.TXStateProxyImpl;
import com.gemstone.gemfire.test.fake.Fakes;
import com.gemstone.gemfire.test.junit.categories.UnitTest;


@Category(UnitTest.class)
public class PartitionMessageTest {

  private GemFireCacheImpl cache;
  private PartitionMessage msg;
  private DistributionManager dm;
  private PartitionedRegion pr;
  private TXManagerImpl txMgr;
  private long startTime = 1;
  TXStateProxy tx;
  
  @Before
  public void setUp() throws PRLocallyDestroyedException, InterruptedException {
    cache = Fakes.cache();
    dm = mock(DistributionManager.class);  
    msg = mock(PartitionMessage.class);
    pr = mock(PartitionedRegion.class);
    txMgr = mock(TXManagerImpl.class);
    tx = mock(TXStateProxyImpl.class);
    
    when(msg.checkCacheClosing(dm)).thenReturn(false);
    when(msg.checkDSClosing(dm)).thenReturn(false);
    when(msg.getPartitionedRegion()).thenReturn(pr);
    when(msg.getGemFireCacheImpl()).thenReturn(cache);
    when(msg.getStartPartitionMessageProcessingTime(pr)).thenReturn(startTime);
    when(msg.getTXManagerImpl(cache)).thenReturn(txMgr);
    
    doAnswer(new CallsRealMethods()).when(msg).process(dm);     
  }

  @Test
  public void messageWithNoTXPerformsOnRegion() throws InterruptedException, CacheException, QueryException, DataLocationException, IOException {   
    when(txMgr.masqueradeAs(msg)).thenReturn(null);
    msg.process(dm);
    
    verify(msg, times(1)).operateOnPartitionedRegion(dm, pr, startTime);
  }
  
  @Test
  public void messageForNotFinishedTXPerformsOnRegion() throws InterruptedException, CacheException, QueryException, DataLocationException, IOException {   
    when(txMgr.masqueradeAs(msg)).thenReturn(tx);
    when(tx.isInProgress()).thenReturn(true);
    msg.process(dm);
    
    verify(msg, times(1)).operateOnPartitionedRegion(dm, pr, startTime);
  }
  
  @Test
  public void messageForFinishedTXDoesNotPerformOnRegion() throws InterruptedException, CacheException, QueryException, DataLocationException, IOException {   
    when(txMgr.masqueradeAs(msg)).thenReturn(tx);
    when(tx.isInProgress()).thenReturn(false);
    msg.process(dm);
  
    verify(msg, times(0)).operateOnPartitionedRegion(dm, pr, startTime);
  }

  @Test
  public void noNewTxProcessingAfterTXManagerImplClosed() throws CacheException, QueryException, DataLocationException, InterruptedException, IOException {
    txMgr = new TXManagerImpl(null, cache);
    when(msg.getPartitionedRegion()).thenReturn(pr);
    when(msg.getGemFireCacheImpl()).thenReturn(cache);
    when(msg.getStartPartitionMessageProcessingTime(pr)).thenReturn(startTime);
    when(msg.getTXManagerImpl(cache)).thenReturn(txMgr);
    when(msg.canParticipateInTransaction()).thenReturn(true);
    when(msg.canStartRemoteTransaction()).thenReturn(true);
    
    msg.process(dm);
    
    txMgr.close();
    
    msg.process(dm);

    verify(msg, times(1)).operateOnPartitionedRegion(dm, pr, startTime);
  }
}
