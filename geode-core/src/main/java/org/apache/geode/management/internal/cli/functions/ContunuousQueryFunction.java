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
import java.util.Iterator;
import org.apache.geode.cache.CacheFactory;
import org.apache.geode.cache.execute.Function;
import org.apache.geode.cache.execute.FunctionContext;
import org.apache.geode.internal.InternalEntity;
import org.apache.geode.internal.cache.CacheServerImpl;
import org.apache.geode.internal.cache.GemFireCacheImpl;
import org.apache.geode.internal.cache.tier.sockets.AcceptorImpl;
import org.apache.geode.internal.cache.tier.sockets.CacheClientNotifier;
import org.apache.geode.internal.cache.tier.sockets.CacheClientProxy;
import org.apache.geode.internal.cache.tier.sockets.ClientProxyMembershipID;
import org.apache.geode.internal.cache.tier.sockets.ServerConnection;



/**
 * 
 * @since GemFire 8.0
 */

public class ContunuousQueryFunction implements Function, InternalEntity {
  public static final String ID = ContunuousQueryFunction.class.getName();
  private static final long serialVersionUID = 1L;

  @Override
  public void execute(FunctionContext context) {
    try {
      String clientID = (String) context.getArguments();
      GemFireCacheImpl cache = (GemFireCacheImpl) CacheFactory.getAnyInstance();
      if (cache.getCacheServers().size() > 0) {
        CacheServerImpl server = (CacheServerImpl) cache.getCacheServers().iterator().next();
        if (server != null) {
          AcceptorImpl acceptorImpl = server.getAcceptor();
          if (acceptorImpl != null) {
            CacheClientNotifier cacheClientNotifier = acceptorImpl.getCacheClientNotifier();
            if (cacheClientNotifier != null) {
              Collection<CacheClientProxy> cacheClientProxySet =
                  cacheClientNotifier.getClientProxies();
              ClientInfo clientInfo = null;
              boolean foundClientinCCP = false;
              Iterator<CacheClientProxy> it = cacheClientProxySet.iterator();
              while (it.hasNext()) {

                CacheClientProxy ccp = it.next();
                if (ccp != null) {
                  String clientIdFromProxy = ccp.getProxyID().getDSMembership();
                  if (clientIdFromProxy != null && clientIdFromProxy.equals(clientID)) {
                    foundClientinCCP = true;
                    String durableId = ccp.getProxyID().getDurableId();
                    boolean isPrimary = ccp.isPrimary();
                    clientInfo = new ClientInfo(
                        (durableId != null && durableId.length() > 0 ? "Yes" : "No"),
                        (isPrimary == true
                            ? cache.getDistributedSystem().getDistributedMember().getId() : ""),
                        (isPrimary == false
                            ? cache.getDistributedSystem().getDistributedMember().getId() : ""));
                    break;

                  }
                }
              }

              // try getting from server connections
              if (foundClientinCCP == false) {
                ServerConnection[] serverConnections = acceptorImpl.getAllServerConnectionList();

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
          .lastResult("Exception in ContunuousQueryFunction =" + e.getMessage());
    }
    context.getResultSender().lastResult(null);

  }

  @Override
  public String getId() {
    return ContunuousQueryFunction.ID;

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
