/*
 * Licensed to the Apache Software Foundation (ASF) under one or more contributor license
 * agreements. See the NOTICE file distributed with this work for additional information regarding
 * copyright ownership. The ASF licenses this file to You under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance with the License. You may obtain a
 * copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software distributed under the License
 * is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express
 * or implied. See the License for the specific language governing permissions and limitations under
 * the License.
 */
package org.apache.geode.cache.server.internal;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.Properties;

import org.apache.geode.DataSerializable;
import org.apache.geode.cache.server.ServerLoad;
import org.apache.geode.cache.server.ServerLoadProbeAdapter;
import org.apache.geode.cache.server.ServerMetrics;
import org.apache.geode.internal.cache.xmlcache.Declarable2;

/**
 * A load probe which returns load as a function of the number of connections to the cache server.
 *
 * The Load object returned by this probe reports the connection load as the number of connections
 * to this server divided by the max connections for this server. This means that servers with a
 * lower max connections will receive fewer connections than servers with a higher max connections.
 * The load therefore is a number between 0 and 1, where 0 means there are are no connections, and 1
 * means the server at max connections.
 *
 * The queue load is reported simply as the number of queues hosted by this cache server.
 *
 *
 * @since GemFire 5.7
 */
public class ConnectionCountProbe extends ServerLoadProbeAdapter
    implements Declarable2, DataSerializable {

  private static final long serialVersionUID = -5072528455996471323L;

  /**
   * Get a loads object representing the number of connections to this cache server
   */
  public ServerLoad getLoad(ServerMetrics metrics) {
    float load = metrics.getConnectionCount() / (float) metrics.getMaxConnections();
    int queueLoad = metrics.getSubscriptionConnectionCount();
    float loadPerConnection = 1 / (float) metrics.getMaxConnections();

    return new ServerLoad(load, loadPerConnection, queueLoad, 1);
  }

  public Properties getConfig() {
    return new Properties();
  }

  public void init(Properties props) {}

  @Override
  public boolean equals(Object other) {
    return (other != null && this.getClass().equals(other.getClass()));
  }

  @Override
  public int hashCode() {
    // since we have no state defer to super. Added this to keep findbugs happy.
    return super.hashCode();
  }

  @Override
  public String toString() {
    return "ConnectionCountProbe";
  }

  public void fromData(DataInput in) throws IOException, ClassNotFoundException {
    // do nothing, we have no state
  }

  public void toData(DataOutput out) throws IOException {
    // do nothing, we have no state
  }
}
