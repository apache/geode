/*=========================================================================
 * Copyright (c) 2002-2014 Pivotal Software, Inc. All Rights Reserved.
 * This product is protected by U.S. and international copyright
 * and intellectual property laws. Pivotal products are covered by
 * more patents listed at http://www.pivotal.io/patents.
 *=========================================================================
 */
   
   
package com.gemstone.gemfire.internal.admin.remote;

import com.gemstone.gemfire.*;
//import com.gemstone.gemfire.internal.*;
import com.gemstone.gemfire.internal.admin.*;
import com.gemstone.gemfire.internal.i18n.LocalizedStrings;
import com.gemstone.gemfire.distributed.internal.*;
import com.gemstone.gemfire.distributed.internal.locks.*;
import java.io.*;
import java.util.*;
import com.gemstone.gemfire.distributed.internal.membership.*;

public final class FetchDistLockInfoResponse extends AdminResponse {
  // instance variables
  DLockInfo[] lockInfos;
  
  /**
   * Returns a <code>FetchDistLockInfoResponse</code> that will be returned to the
   * specified recipient. The message will contains a copy of the local manager's
   * distributed lock service information.
   */
  public static FetchDistLockInfoResponse create(DistributionManager dm,
                                                 InternalDistributedMember recipient) {
    FetchDistLockInfoResponse m = new FetchDistLockInfoResponse();
    InternalDistributedMember id = dm.getDistributionManagerId();
    Set entries = DLockService.snapshotAllServices().entrySet();
    List infos = new ArrayList();
    Iterator iter = entries.iterator();
    while (iter.hasNext()) {
      Map.Entry entry = (Map.Entry)iter.next();
      String serviceName = entry.getKey().toString();
      DLockService service = 
          (DLockService) entry.getValue();
      Set serviceEntries = service.snapshotService().entrySet();
      Iterator iter1 = serviceEntries.iterator();
      while(iter1.hasNext()) {
        Map.Entry token = (Map.Entry)iter1.next();
        infos.add(new RemoteDLockInfo(serviceName,
                                      token.getKey().toString(),
                                      (DLockToken)token.getValue(),
                                      id));
      }
    }    
    m.lockInfos = (DLockInfo[])infos.toArray(new DLockInfo[0]);
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
  public void fromData(DataInput in)
    throws IOException, ClassNotFoundException {
    super.fromData(in);
    this.lockInfos = (DLockInfo[])DataSerializer.readObject(in);
  }

  @Override
  public String toString() {
    return LocalizedStrings.FetchDistLockInfoResponse_FETCHDISTLOCKINFORESPONSE_FROM_0.toLocalizedString(this.getSender());
  }
}
