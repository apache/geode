package com.gemstone.gemfire.cache.query.internal.cq;

import com.gemstone.gemfire.cache.query.CqClosedException;
import com.gemstone.gemfire.cache.query.CqException;
import com.gemstone.gemfire.cache.query.CqQuery;
import com.gemstone.gemfire.cache.query.CqState;
import com.gemstone.gemfire.cache.query.internal.CqQueryVsdStats;
import com.gemstone.gemfire.cache.query.internal.CqStateImpl;
import com.gemstone.gemfire.internal.cache.LocalRegion;

public interface InternalCqQuery extends CqQuery {

  /** 
   * sets the CqName.
   */
  public abstract void setName(String cqName);
  
  public String getName();

  /**
   * Closes the Query.
   *        On Client side, sends the cq close request to server.
   *        On Server side, takes care of repository cleanup.
   * @param sendRequestToServer true to send the request to server.
   * @throws CqException
   */
  public abstract void close(boolean sendRequestToServer)
      throws CqClosedException, CqException;


  public abstract LocalRegion getCqBaseRegion();

  /**
   * @return Returns the serverCqName.
   */
  public abstract String getServerCqName();

  /**
   * Sets the state of the cq.
   * Server side method. Called during cq registration time.
   */
  public abstract void setCqState(int state);

  /**
   * get the region name in CQ query
   */
  public String getRegionName();

  public abstract void setCqService(CqService cqService);

  /**
   * Returns a reference to VSD stats of the CQ
   * @return VSD stats of the CQ
   */
  public CqQueryVsdStats getVsdStats();
  
  public abstract CqState getState();
  
  public String getQueryString();
  
  public abstract boolean isDurable();

  public abstract void close() throws CqClosedException, CqException;

  public abstract void stop() throws CqClosedException, CqException;
}
