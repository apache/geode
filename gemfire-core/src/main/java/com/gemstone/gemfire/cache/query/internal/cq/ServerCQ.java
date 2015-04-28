package com.gemstone.gemfire.cache.query.internal.cq;

import com.gemstone.gemfire.cache.query.CqClosedException;
import com.gemstone.gemfire.cache.query.CqException;
import com.gemstone.gemfire.cache.query.RegionNotFoundException;
import com.gemstone.gemfire.internal.cache.tier.sockets.CacheClientNotifier;
import com.gemstone.gemfire.internal.cache.tier.sockets.ClientProxyMembershipID;

public interface ServerCQ extends InternalCqQuery {

  /**
   * @return the filterID
   */
  public abstract Long getFilterID();

  /**
   * @param filterID the filterID to set
   */
  public abstract void setFilterID(Long filterID);

  /**
   * Register the Query on server.
   * @param p_clientProxyId
   * @param p_ccn
   * @throws CqException
   */
  public abstract void registerCq(ClientProxyMembershipID p_clientProxyId,
      CacheClientNotifier p_ccn, int p_cqState) throws CqException,
      RegionNotFoundException;

  /**
   * Adds into the CQ Results key cache.
   * @param key
   */
  public abstract void addToCqResultKeys(Object key);

  /**
   * Removes the key from CQ Results key cache. 
   * @param key
   * @param isTokenMode if true removes the key if its in destroy token mode
   *          if false removes the key without any check.
   */
  public abstract void removeFromCqResultKeys(Object key, boolean isTokenMode);

  /**
   * Sets the CQ Results key cache state as initialized.
   */
  public abstract void setCqResultsCacheInitialized();

  /**
   * Returns true if old value is required for query processing.
   */
  public abstract boolean isOldValueRequiredForQueryProcessing(Object key);

  /**
   * Closes the Query.
   *        On Client side, sends the cq close request to server.
   *        On Server side, takes care of repository cleanup.
   * @param sendRequestToServer true to send the request to server.
   * @throws CqException
   */
  public abstract void close(boolean sendRequestToServer)
      throws CqClosedException, CqException;


  public abstract boolean isPR();
  
  public ClientProxyMembershipID getClientProxyId();

  public void stop() throws CqClosedException, CqException;
}
