package com.gemstone.gemfire.cache.query.internal.cq;

import com.gemstone.gemfire.cache.client.internal.Connection;
import com.gemstone.gemfire.cache.client.internal.ProxyCache;
import com.gemstone.gemfire.cache.query.CqClosedException;
import com.gemstone.gemfire.cache.query.CqException;
import com.gemstone.gemfire.cache.query.CqQuery;

public interface ClientCQ extends InternalCqQuery  {

  /**
   * Closes the Query.
   *        On Client side, sends the cq close request to server.
   *        On Server side, takes care of repository cleanup.
   * @param sendRequestToServer true to send the request to server.
   * @throws CqException
   */
  public abstract void close(boolean sendRequestToServer)
      throws CqClosedException, CqException;

  public abstract void setProxyCache(ProxyCache proxyCache);

  public abstract void createOn(Connection recoveredConnection,
      boolean isDurable);

}
