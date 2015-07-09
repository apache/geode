package com.gemstone.gemfire.internal.cache;

import org.apache.logging.log4j.Logger;

import com.gemstone.gemfire.distributed.internal.membership.InternalDistributedMember;
import com.gemstone.gemfire.internal.logging.LogService;

/**
 * 
 * @author shirishd
 *
 */
public abstract class DistTXStateProxyImpl extends TXStateProxyImpl {

  protected static final Logger logger = LogService.getLogger();

  public DistTXStateProxyImpl(TXManagerImpl managerImpl, TXId id,
      InternalDistributedMember clientMember) {
    super(managerImpl, id, clientMember);
    // TODO Auto-generated constructor stub
  }

  public DistTXStateProxyImpl(TXManagerImpl managerImpl, TXId id, boolean isjta) {
    super(managerImpl, id, isjta);
    // TODO Auto-generated constructor stub
  }
  
  @Override
  public boolean isDistTx() {
    return true;
  }
}
