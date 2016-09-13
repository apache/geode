/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
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
 * @since GemFire 5.7
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
