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

import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;

import com.gemstone.gemfire.cache.Region;
import com.gemstone.gemfire.cache.partition.PartitionRegionHelper;
import com.gemstone.gemfire.distributed.internal.membership.InternalDistributedMember;
import com.gemstone.gemfire.internal.cache.versions.VersionTag;
import com.gemstone.gemfire.test.dunit.IgnoredException;
import com.gemstone.gemfire.test.dunit.LogWriterUtils;
import com.gemstone.gemfire.test.dunit.SerializableCallable;
import com.gemstone.gemfire.test.dunit.VM;

/**
 * test client initiated transactions with concurrency checks enabled.
 * @author sbawaska
 */
public class ClientServerTransactionCCEDUnitTest extends
    ClientServerTransactionDUnitTest {

  
  public void setUp() throws Exception {
    super.setUp();
    IgnoredException.addIgnoredException("Connection reset");
    IgnoredException.addIgnoredException("SocketTimeoutException");
    IgnoredException.addIgnoredException("ServerConnectivityException");
    IgnoredException.addIgnoredException("Socket Closed");

  }
  /**
   * 
   */
  private static final long serialVersionUID = -6785438240204988439L;

  public ClientServerTransactionCCEDUnitTest(String name) {
    super(name);
  }

  @Override
  protected boolean getConcurrencyChecksEnabled() {
    return true;
  }
  
  @SuppressWarnings("unchecked")
  public Map<Object, VersionTag> getVersionTagsForRegion(VM vm, final String regionName) {
    return (Map<Object, VersionTag>) vm.invoke(new SerializableCallable() {
      @Override
      public Object call() throws Exception {
        Map<Object, VersionTag> map = new HashMap<Object, VersionTag>();
        LocalRegion r = (LocalRegion) getCache().getRegion(regionName);
        Iterator<Object> it = null;
        if (r instanceof PartitionedRegion) {
          Region l = PartitionRegionHelper.getLocalPrimaryData(r);
          it = l.keySet().iterator();
        } else {
          it = r.keySet().iterator();
        }
        while (it.hasNext()) {
          Object key = it.next();
          map.put(key, r.getRegionEntry(key).getVersionStamp().asVersionTag());
        }
        return map;
      }
    });
  }

  public InternalDistributedMember getMemberId(VM vm) {
    return (InternalDistributedMember) vm.invoke(new SerializableCallable() {
      @Override
      public Object call() throws Exception {
        return getCache().getDistributedSystem().getDistributedMember();
      }
    });
  }

  public void verifyVersionTags(VM client, VM server1, VM server2, VM server3) {
    Map<Object, VersionTag> clientTags = getVersionTagsForRegion(client, D_REFERENCE);
    Map<Object, VersionTag> serverTags = getVersionTagsForRegion(server1, D_REFERENCE);

    InternalDistributedMember serverId = getMemberId(server1);
    for (Object key : clientTags.keySet()) {
      VersionTag serverTag = serverTags.get(key);
      serverTag.setMemberID(serverId);
      LogWriterUtils.getLogWriter().fine("SWAP:key:"+key+" clientVersion:"+clientTags.get(key)+" serverVersion:"+serverTag);
      assertEquals(clientTags.get(key), serverTags.get(key));
      serverTags.remove(key);
    }
    assertTrue(serverTags.isEmpty());
  }
}
