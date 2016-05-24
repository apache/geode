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
package com.gemstone.gemfire.internal.cache;

import static org.mockito.Mockito.*;

import org.junit.Before;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.mockito.internal.stubbing.answers.CallsRealMethods;

import com.gemstone.gemfire.distributed.internal.DistributionManager;
import com.gemstone.gemfire.internal.cache.GemFireCacheImpl;
import com.gemstone.gemfire.internal.cache.TXId;
import com.gemstone.gemfire.internal.cache.TXManagerImpl;
import com.gemstone.gemfire.internal.cache.TXStateProxy;
import com.gemstone.gemfire.internal.cache.TXStateProxyImpl;
import com.gemstone.gemfire.test.fake.Fakes;
import com.gemstone.gemfire.test.junit.categories.UnitTest;


@Category(UnitTest.class)
public class RemoteOperationMessageTest {
  private GemFireCacheImpl cache;
  private RemoteOperationMessage msg;
  private DistributionManager dm;
  private LocalRegion r;
  private TXManagerImpl txMgr;
  private TXId txid;
  private long startTime = 0;
  TXStateProxy tx;
  
  @Before
  public void setUp() throws InterruptedException {
    cache = Fakes.cache();
    dm = mock(DistributionManager.class);  
    msg = mock(RemoteOperationMessage.class);
    r = mock(LocalRegion.class);
    txMgr = mock(TXManagerImpl.class);
    txid = new TXId(null, 0);
    tx = mock(TXStateProxyImpl.class);
    
    when(msg.checkCacheClosing(dm)).thenReturn(false);
    when(msg.checkDSClosing(dm)).thenReturn(false);
    when(msg.getCache(dm)).thenReturn(cache);
    when(msg.getRegionByPath(cache)).thenReturn(r);
    when(msg.getTXManager(cache)).thenReturn(txMgr);
    when(txMgr.hasTxAlreadyFinished(tx, txid)).thenCallRealMethod();

    doAnswer(new CallsRealMethods()).when(msg).process(dm);    
  }
  
  @Test
  public void messageWithNoTXPerformsOnRegion() throws InterruptedException, RemoteOperationException {
    when(txMgr.masqueradeAs(msg)).thenReturn(null);
    msg.process(dm);

    verify(msg, times(1)).operateOnRegion(dm, r, startTime);
  }

  @Test
  public void messageForNotFinishedTXPerformsOnRegion() throws InterruptedException, RemoteOperationException {
    when(txMgr.masqueradeAs(msg)).thenReturn(tx);
    when(msg.hasTxAlreadyFinished(tx, txMgr, txid)).thenCallRealMethod(); 
    msg.process(dm);

    verify(msg, times(1)).operateOnRegion(dm, r, startTime);
  }
  
  @Test
  public void messageForFinishedTXDoesNotPerformOnRegion() throws InterruptedException, RemoteOperationException {
    when(txMgr.masqueradeAs(msg)).thenReturn(tx);
    when(msg.hasTxAlreadyFinished(tx, txMgr, txid)).thenReturn(true); 
    msg.process(dm);

    verify(msg, times(0)).operateOnRegion(dm, r, startTime);
  }

}
