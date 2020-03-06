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
package org.apache.geode.management.internal.cli.functions;

import java.io.Serializable;
import java.util.Collection;

import org.apache.geode.cache.execute.FunctionContext;
import org.apache.geode.internal.cache.CacheServerImpl;
import org.apache.geode.internal.cache.InternalCache;
import org.apache.geode.internal.cache.execute.InternalFunction;
import org.apache.geode.internal.cache.tier.Acceptor;
import org.apache.geode.internal.cache.tier.sockets.CacheClientNotifier;
import org.apache.geode.internal.cache.tier.sockets.CacheClientProxy;
import org.apache.geode.internal.cache.tier.sockets.ClientProxyMembershipID;
import org.apache.geode.internal.cache.tier.sockets.ServerConnection;

/**
 * @since GemFire 8.0
 */
public class ContinuousQueryFunction implements InternalFunction<String> {
  private static final long serialVersionUID = 1L;

  public static final String ID = ContinuousQueryFunction.class.getName();

  @Override
  public void execute(FunctionContext<String> context) {
    try {
      String clientID = context.getArguments();
      InternalCache cache = (InternalCache) context.getCache();
      if (cache.getCacheServers().size() > 0) {
        CacheServerImpl server = (CacheServerImpl) cache.getCacheServers().iterator().next();
        if (server != null) {
          Acceptor acceptor = server.getAcceptor();
          if (acceptor != null) {
            CacheClientNotifier cacheClientNotifier = acceptor.getCacheClientNotifier();
            if (cacheClientNotifier != null) {
              Collection<CacheClientProxy> cacheClientProxySet =
                  cacheClientNotifier.getClientProxies();
              ClientInfo clientInfo = null;
              boolean foundClientinCCP = false;
              for (CacheClientProxy ccp : cacheClientProxySet) {

                if (ccp != null) {
                  String clientIdFromProxy = ccp.getProxyID().getDSMembership();
                  if (clientIdFromProxy != null && clientIdFromProxy.equals(clientID)) {
                    foundClientinCCP = true;
                    String durableId = ccp.getProxyID().getDurableId();
                    boolean isPrimary = ccp.isPrimary();
                    final String id = cache.getDistributedSystem().getDistributedMember().getId();
                    clientInfo = new ClientInfo(
                        (durableId != null && durableId.length() > 0 ? "Yes" : "No"),
                        (isPrimary ? id : ""),
                        (!isPrimary ? id : ""));
                    break;

                  }
                }
              }

              // try getting from server connections
              if (!foundClientinCCP) {
                ServerConnection[] serverConnections = acceptor.getAllServerConnectionList();

                for (ServerConnection conn : serverConnections) {
                  ClientProxyMembershipID cliIdFrmProxy = conn.getProxyID();

                  if (clientID.equals(cliIdFrmProxy.getDSMembership())) {
                    String durableId = cliIdFrmProxy.getDurableId();
                    clientInfo =
                        new ClientInfo((durableId != null && durableId.length() > 0 ? "Yes" : "No"),
                            "N.A.", "N.A.");
                  }

                }
              }
              context.getResultSender().lastResult(clientInfo);
            }
          }
        }
      }
    } catch (Exception e) {
      context.getResultSender()
          .lastResult("Exception in ContinuousQueryFunction =" + e.getMessage());
    }
    context.getResultSender().lastResult(null);
  }

  @Override
  public String getId() {
    return ContinuousQueryFunction.ID;
  }

  @Override
  public boolean hasResult() {
    return true;
  }

  @Override
  public boolean optimizeForWrite() {
    return false;
  }

  @Override
  public boolean isHA() {
    return false;
  }

  public class ClientInfo implements Serializable {
    private static final long serialVersionUID = 1L;
    public String isDurable;
    public String primaryServer;
    public String secondaryServer;

    public ClientInfo(String IsClientDurable, String primaryServerId, String secondaryServerId) {
      isDurable = IsClientDurable;
      primaryServer = primaryServerId;
      secondaryServer = secondaryServerId;
    }

    @Override
    public String toString() {
      return "ClientInfo [isDurable=" + isDurable + ", primaryServer=" + primaryServer
          + ", secondaryServer=" + secondaryServer + "]";
    }
  }
}
