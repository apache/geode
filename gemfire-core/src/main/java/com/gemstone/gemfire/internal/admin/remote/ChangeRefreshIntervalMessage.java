/*=========================================================================
 * Copyright (c) 2002-2014 Pivotal Software, Inc. All Rights Reserved.
 * This product is protected by U.S. and international copyright
 * and intellectual property laws. Pivotal products are covered by
 * more patents listed at http://www.pivotal.io/patents.
 *=========================================================================
 */
package com.gemstone.gemfire.internal.admin.remote;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import com.gemstone.gemfire.distributed.internal.DistributionManager;
import com.gemstone.gemfire.distributed.internal.InternalDistributedSystem;
import com.gemstone.gemfire.distributed.internal.PooledDistributionMessage;
import com.gemstone.gemfire.internal.admin.StatAlertsManager;

/**
 * Distribution message, sets refresh time interval in member's alerts manager
 * 
 * @see StatAlertsManager
 * 
 * @author mjha
 * @since 5.7
 */
public class ChangeRefreshIntervalMessage extends PooledDistributionMessage {

  private long _refreshInterval;

  public static ChangeRefreshIntervalMessage create(long refreshInterval) {
    ChangeRefreshIntervalMessage m = new ChangeRefreshIntervalMessage();

    m._refreshInterval = refreshInterval;

    return m;
  }

  public ChangeRefreshIntervalMessage() {

  }

  @Override
  public void toData(DataOutput out) throws IOException {
    super.toData(out);
    out.writeLong(_refreshInterval);
  }

  @Override
  public void fromData(DataInput in) throws IOException, ClassNotFoundException {
    super.fromData(in);
    _refreshInterval = in.readLong();
  }

  /**
   * Returns the DataSerializer fixed id for the class that implements this method.
   */
  public int getDSFID() {
    return CHANGE_REFRESH_INT_MESSAGE;
  }


  @Override
  protected void process(DistributionManager dm) {
    StatAlertsManager.getInstance(dm)
        .setRefreshTimeInterval(getRefreshInterval());
  }

  /**
   * @return refresh time interval for {@link StatAlertsManager}
   */
  public long getRefreshInterval() {
    return _refreshInterval;
  }

  /**
   * @param interval
   *                Refresh time interval for {@link StatAlertsManager}
   */
  public void setRefreshInterval(long interval) {
    _refreshInterval = interval;
  }

  @Override
  public String toString() {
    return "Set alerts refresh time interval in "
        + InternalDistributedSystem.getAnyInstance().getDistributedMember();
  }
}
