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


package org.apache.geode.internal.admin.remote;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.apache.geode.DataSerializer;
import org.apache.geode.distributed.internal.DistributionManager;
import org.apache.geode.distributed.internal.locks.DLockService;
import org.apache.geode.distributed.internal.locks.DLockToken;
import org.apache.geode.distributed.internal.membership.InternalDistributedMember;
import org.apache.geode.internal.admin.DLockInfo;

public class FetchDistLockInfoResponse extends AdminResponse {
  // instance variables
  DLockInfo[] lockInfos;

  /**
   * Returns a <code>FetchDistLockInfoResponse</code> that will be returned to the specified
   * recipient. The message will contains a copy of the local manager's distributed lock service
   * information.
   */
  public static FetchDistLockInfoResponse create(DistributionManager dm,
      InternalDistributedMember recipient) {
    FetchDistLockInfoResponse m = new FetchDistLockInfoResponse();
    InternalDistributedMember id = dm.getDistributionManagerId();
    Set entries = DLockService.snapshotAllServices().entrySet();
    List infos = new ArrayList();
    Iterator iter = entries.iterator();
    while (iter.hasNext()) {
      Map.Entry entry = (Map.Entry) iter.next();
      String serviceName = entry.getKey().toString();
      DLockService service = (DLockService) entry.getValue();
      Set serviceEntries = service.snapshotService().entrySet();
      Iterator iter1 = serviceEntries.iterator();
      while (iter1.hasNext()) {
        Map.Entry token = (Map.Entry) iter1.next();
        infos.add(new RemoteDLockInfo(serviceName, token.getKey().toString(),
            (DLockToken) token.getValue(), id));
      }
    }
    m.lockInfos = (DLockInfo[]) infos.toArray(new DLockInfo[0]);
    m.setRecipient(recipient);
    return m;
  }

  // instance methods
  public DLockInfo[] getLockInfos() {
    return lockInfos;
  }

  public int getDSFID() {
    return FETCH_DIST_LOCK_INFO_RESPONSE;
  }

  @Override
  public void toData(DataOutput out) throws IOException {
    super.toData(out);
    DataSerializer.writeObject(this.lockInfos, out);
  }

  @Override
  public void fromData(DataInput in) throws IOException, ClassNotFoundException {
    super.fromData(in);
    this.lockInfos = (DLockInfo[]) DataSerializer.readObject(in);
  }

  @Override
  public String toString() {
    return String.format("FetchDistLockInfoResponse from %s",
        this.getSender());
  }
}
