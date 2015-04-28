/*=========================================================================
 * Copyright (c) 2010-2014 Pivotal Software, Inc. All Rights Reserved.
 * This product is protected by U.S. and international copyright
 * and intellectual property laws. Pivotal products are covered by
 * one or more patents listed at http://www.pivotal.io/patents.
 *=========================================================================
 */
package com.gemstone.gemfire.internal.cache.execute;

import java.util.Collection;
import java.util.Iterator;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

import com.gemstone.gemfire.cache.TransactionDataNodeHasDepartedException;
import com.gemstone.gemfire.cache.TransactionId;
import com.gemstone.gemfire.cache.client.Pool;
import com.gemstone.gemfire.cache.client.internal.Endpoint;
import com.gemstone.gemfire.cache.client.internal.EndpointManager;
import com.gemstone.gemfire.cache.client.internal.PoolImpl;
import com.gemstone.gemfire.cache.execute.Execution;
import com.gemstone.gemfire.cache.execute.FunctionService;
import com.gemstone.gemfire.distributed.DistributedMember;
import com.gemstone.gemfire.distributed.internal.ServerLocation;
import com.gemstone.gemfire.internal.cache.GemFireCacheImpl;
import com.gemstone.gemfire.internal.cache.PoolManagerImpl;
import com.gemstone.gemfire.internal.cache.TXId;
import com.gemstone.gemfire.internal.cache.execute.util.CommitFunction;
import com.gemstone.gemfire.internal.cache.execute.util.RollbackFunction;

/**
 * Access point for executing functions that should be targeted to
 * execute on member where the transaction is hosted.
 * </p> To commit a transaction which is not hosted locally, this
 * functionService can be used to obtain an execution object on which
 * {@link CommitFunction} and {@link RollbackFunction} can be called.
 * 
 * @author sbawaska
 * @since 6.6.1
 */
public final class TransactionFunctionService {

  private TransactionFunctionService() {
  }

  /**
   * This is a convenience method to obtain execution objects for transactional
   * functions. This method can be invoked from clients as well as peer members.
   * When invoked from a client this method makes sure that the function is
   * executed on the member where the transaction is hosted. This prevents the
   * need for fanning out the function to multiple servers. e.g. A remotely
   * hosted transaction can be committed as:
   * 
   * <pre>
   * Execution exe = TransactionFunctionService.onTransaction(txId);
   * List l = (List) exe.execute(transactionalFunction).getResult();
   * </pre>
   * 
   * @param transactionId
   *          the transaction identifier
   * @return the execution object
   * @see CommitFunction
   * @see RollbackFunction
   * @since 6.6.1
   */
  public static Execution onTransaction(TransactionId transactionId) {
    Execution execution = null;
    GemFireCacheImpl cache = GemFireCacheImpl
        .getExisting("getting transaction execution");
    if (cache.hasPool()) {
      if (cache.getLoggerI18n().fineEnabled()) {
        cache.getLoggerI18n().fine("TxFunctionService: inClientMode");
      }
      PoolImpl pool = (PoolImpl) getPool(cache);
      
      DistributedMember member = ((TXId)transactionId).getMemberId();
      ServerLocation loc = memberToServerLocation.get(member);
      if (loc == null) {
        loc = getServerLocationForMember(pool, member);
        if (loc == null) {
          throw new TransactionDataNodeHasDepartedException("Could not connect to member:"+member);
        }
        memberToServerLocation.put(member, loc);
      }
      // setup server affinity so that the function is executed
      // on the member hosting the transaction
      pool.setupServerAffinity(false/*allowFailover*/);
      pool.setServerAffinityLocation(loc);
      if (cache.getLoggerI18n().fineEnabled()) {
        cache.getLoggerI18n().fine(
            "TxFunctionService: setting server affinity to:" + loc
                + " which represents member:" + member);
      }
      execution = FunctionService.onServer(pool).withArgs(
          transactionId);
      // this serverAffinity is cleared up in the finally blocks
      // of ServerFunctionExecutor
      ((ServerFunctionExecutor)execution).setOnBehalfOfTXFunctionService(true);
    } else {
      if (cache.getLoggerI18n().fineEnabled()) {
        cache.getLoggerI18n().fine("TxFunctionService: inPeerMode");
      }
      TXId txId = (TXId) transactionId;
      DistributedMember member = txId.getMemberId();
      execution = FunctionService
          .onMember(cache.getDistributedSystem(), member).withArgs(
              transactionId);
    }
    return execution;
  }
  
  /**
   * keep a mapping of DistributedMemeber to ServerLocation so that we can still
   * connect to the same server when the poolLoadConditioningMonitor closes
   * connection. fix for bug 43810
   */
  private static final ConcurrentMap<DistributedMember, ServerLocation> memberToServerLocation =
    new ConcurrentHashMap<DistributedMember, ServerLocation>();
  
  /**
   * Get the ServerLocation corresponding to the given DistributedMember
   * or null if there is no endPoint to the member
   * @param pool
   * @param member
   */
  private static ServerLocation getServerLocationForMember(PoolImpl pool,
      DistributedMember member) {
    Map<ServerLocation, Endpoint> endPoints = pool.getEndpointMap();
    for (Endpoint endPoint : endPoints.values()) {
      if (endPoint.getMemberId().equals(member)) {
        return endPoint.getLocation();
      }
    }
    return null;
  }

  private static Pool getPool(GemFireCacheImpl cache) {
    Pool pool = cache.getDefaultPool();
    if (pool == null) {
      Collection<Pool> pools = getPools();
      if (pools.size() > 1) {
        throw new IllegalStateException("More than one pools found");
      }
      pool = pools.iterator().next();
    }
    return pool;
  }
  
  private static Collection<Pool> getPools() {
    Collection<Pool> pools = PoolManagerImpl.getPMI().getMap().values();
    for(Iterator<Pool> itr= pools.iterator(); itr.hasNext(); ) {
      PoolImpl pool = (PoolImpl) itr.next();
      if(pool.isUsedByGateway()) {
        itr.remove();
      }
    }
    return pools;
  }
  
  /**
   * When an EndPoint becomes available we remember the mapping of
   * DistributedMember to ServerLocation so that we can reconnect to the server.
   * fix for bug 43817
   * 
   * @author sbawaska
   */
  public static class ListenerForTransactionFunctionService implements
      EndpointManager.EndpointListener {

    public void endpointNoLongerInUse(Endpoint endpoint) {
    }

    public void endpointCrashed(Endpoint endpoint) {
    }

    public void endpointNowInUse(Endpoint endpoint) {
      assert endpoint != null;
      memberToServerLocation.put(endpoint.getMemberId(),
          endpoint.getLocation());
    }

  }
}
