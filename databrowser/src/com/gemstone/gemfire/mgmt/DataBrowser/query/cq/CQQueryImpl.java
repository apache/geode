/*========================================================================= 
 * (c)Copyright 2002-2009, GemStone Systems, Inc. All Rights Reserved.
 * 1260 NW Waterhouse Ave., Suite 200, Beaverton, OR 97006 
 * All Rights Reserved.
 * =======================================================================*/
package com.gemstone.gemfire.mgmt.DataBrowser.query.cq;

import com.gemstone.gemfire.cache.query.CqQuery;
import com.gemstone.gemfire.cache.query.CqStatistics;
import com.gemstone.gemfire.mgmt.DataBrowser.model.member.GemFireMember;

public class CQQueryImpl implements CQQuery {
  private GemFireMember member;
  private CqQuery query;
  private CQResult result;

  public CQQueryImpl(GemFireMember mbr, CqQuery qry, CQResult res) {
    super();
    member = mbr;
    query = qry;
    result = res;
  }
  
  public GemFireMember getMember() {
    return member;
  }

  public void close() throws CQException {
    try {
      query.close();
      result.close();
    }
    catch (com.gemstone.gemfire.cache.query.CqException e) {
      throw new CQException(e);
    }
  }

  public void execute() throws CQException {
    try {
      query.execute();
    }
    catch (Exception e) {
      throw new CQException(e);
    }
  }

  public String getName() {
    return query.getName();
  }

  public CQResult getQueryResult() {
    return result;
  }

  public String getQueryString() {
    return query.getQueryString();
  }

  public CqStatistics getStatistics() {
    return this.query.getStatistics();
  }

  public boolean isClosed() {
    return this.query.isClosed();
  }

  public boolean isDurable() {
    return this.query.isDurable();
  }

  public boolean isRunning() {
    return this.query.isRunning();
  }

  public boolean isStopped() {
    return this.query.isStopped();
  }

  public void stop() throws CQException {
    try {
      this.query.stop();
    }
    catch (Exception e) {
      throw new CQException(e);
    }
  }

}
