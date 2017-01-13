/*=========================================================================
 * Copyright (c) 2010-2014 Pivotal Software, Inc. All Rights Reserved.
 * This product is protected by U.S. and international copyright
 * and intellectual property laws. Pivotal products are covered by
 * one or more patents listed at http://www.pivotal.io/patents.
 *=========================================================================
 */
/*=========================================================================
* This implementation is provided on an "AS IS" BASIS,  WITHOUT WARRANTIES
* OR CONDITIONS OF ANY KIND, either express or implied."
*==========================================================================
*/

package javaobject;

import java.security.Principal;
import java.util.HashSet;
import java.util.Set;
import org.apache.geode.LogWriter;
import org.apache.geode.cache.Cache;
import org.apache.geode.cache.operations.OperationContext;
import org.apache.geode.cache.operations.*;
import org.apache.geode.cache.operations.OperationContext.OperationCode;
import org.apache.geode.distributed.DistributedMember;
import org.apache.geode.security.AccessControl;
import org.apache.geode.security.NotAuthorizedException;

/**
 * A dummy implementation of the <code>AccessControl</code> interface that
 * allows authorization depending on the format of the <code>Principal</code>
 * string.
 * 
 * 
 */
public class DummyAuthorization implements AccessControl {

  private Set allowedOps;

  private DistributedMember remoteDistributedMember;

  private LogWriter logger;

  private int count;

  public static final OperationCode[] READER_OPS = { OperationCode.GET,
      OperationCode.QUERY, OperationCode.EXECUTE_CQ, OperationCode.CLOSE_CQ,
      OperationCode.STOP_CQ, OperationCode.REGISTER_INTEREST,
      OperationCode.UNREGISTER_INTEREST, OperationCode.KEY_SET,
      OperationCode.CONTAINS_KEY };

  public static final OperationCode[] WRITER_OPS = { OperationCode.PUT,
      OperationCode.PUTALL, OperationCode.DESTROY, OperationCode.REGION_CLEAR };

  public DummyAuthorization() {
    this.allowedOps = new HashSet(20);
    this.count = 0;
  }

  public static AccessControl create() {
    return new DummyAuthorization();
  }

  private void addReaderOps() {

    for (int index = 0; index < READER_OPS.length; index++) {
      this.allowedOps.add(READER_OPS[index]);
    }
  }

  private void addWriterOps() {

    for (int index = 0; index < WRITER_OPS.length; index++) {
      this.allowedOps.add(WRITER_OPS[index]);
    }
  }

  public void init(Principal principal, DistributedMember remoteMember,
      Cache cache) throws NotAuthorizedException {

    if (principal != null) {
      String name = principal.getName().toLowerCase();
      if (name != null) {
        if (name.equals("root") || name.equals("admin")
            || name.equals("administrator")) {
          addReaderOps();
          addWriterOps();
          this.allowedOps.add(OperationCode.REGION_CREATE);
          this.allowedOps.add(OperationCode.REGION_DESTROY);
        }
        else if (name.startsWith("writer")) {
          addWriterOps();
        }
        else if (name.startsWith("reader")) {
          addReaderOps();
        }
      }
    }
    this.remoteDistributedMember = remoteMember;
    this.logger = cache.getSecurityLogger();
  }

  public boolean authorizeOperation(String regionName,
      OperationContext context) {
    if (!(context instanceof KeyOperationContext)) {
      return true;
    }
    OperationCode opCode = context.getOperationCode();
    Object key = ((KeyOperationContext)context).getKey();
    if (key instanceof String) {
      String invalidkey = (String)key;
      if (invalidkey.equals("invalidkey-1"))
        return false;
    }
    if (opCode.isPut() || opCode.isDestroy()) {
      Object cb = ((KeyOperationContext)context).getCallbackArg();
      if ((cb != null) && (Boolean)cb == Boolean.TRUE) {
        try {
          Thread.sleep(10000);
        } catch (InterruptedException abort) {
          // ignore
        }
      }
    }
    if (opCode.isGet() && context.isPostOperation()) {
      try {
        ++count;
        if (count == 3) {
          Thread.sleep(10000);
          count = 0;
        }
      } catch (InterruptedException abort) {
      }
    }
    this.logger.fine("Invoked authorize operation for [" + opCode
        + "] in region [" + regionName + "] for client: "
        + remoteDistributedMember);
    return this.allowedOps.contains(opCode);
  }

  public void close() {

    this.allowedOps.clear();
  }

}
