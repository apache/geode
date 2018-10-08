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
package org.apache.geode.internal.cache.tier.sockets;

import java.util.Iterator;
import java.util.Map;
import java.util.Random;
import java.util.concurrent.ConcurrentHashMap;

import org.apache.logging.log4j.Logger;
import org.apache.shiro.subject.Subject;

import org.apache.geode.internal.logging.LogService;
import org.apache.geode.internal.security.AuthorizeRequest;
import org.apache.geode.internal.security.AuthorizeRequestPP;

public class ClientUserAuths {
  private static Logger logger = LogService.getLogger();
  // private AtomicLong counter = new AtomicLong(1);
  private Random uniqueIdGenerator = null;
  private int m_seed;
  private long m_firstId;

  private ConcurrentHashMap<Long, UserAuthAttributes> uniqueIdVsUserAuth =
      new ConcurrentHashMap<Long, UserAuthAttributes>();
  private ConcurrentHashMap<String, UserAuthAttributes> cqNameVsUserAuth =
      new ConcurrentHashMap<String, UserAuthAttributes>();
  private ConcurrentHashMap<Long, Subject> uniqueIdVsSubject =
      new ConcurrentHashMap<Long, Subject>();

  public long putUserAuth(UserAuthAttributes userAuthAttr) {
    // TODO:hitesh should we do random here
    // long newId = counter.getAndIncrement();
    long newId = getNextID();
    uniqueIdVsUserAuth.put(newId, userAuthAttr);
    return newId;
  }

  public long putSubject(Subject subject) {
    long newId = getNextID();
    uniqueIdVsSubject.put(newId, subject);
    logger.debug("Subject of {} added.", newId);
    return newId;
  }

  public ClientUserAuths(int clientProxyHashcode) {
    m_seed = clientProxyHashcode;
    uniqueIdGenerator = new Random(m_seed + System.currentTimeMillis());
    m_firstId = uniqueIdGenerator.nextLong();
  }

  private synchronized long getNextID() {
    long uniqueId = uniqueIdGenerator.nextLong();
    if (uniqueId == m_firstId) {
      uniqueIdGenerator = new Random(m_seed + System.currentTimeMillis());
      m_firstId = uniqueIdGenerator.nextLong();
      // now every user need to reauthenticate as we are short of Ids..
      // though possibility of this is rare.
      uniqueIdVsUserAuth.clear();
      return m_firstId;
    }
    return uniqueId;
  }

  public UserAuthAttributes getUserAuthAttributes(long userId) {
    return uniqueIdVsUserAuth.get(userId);
  }

  public Subject getSubject(long userId) {
    return uniqueIdVsSubject.get(userId);
  }

  public boolean removeSubject(long userId) {
    Subject subject = uniqueIdVsSubject.remove(userId);
    logger.debug("Subject of {} removed.", userId);
    if (subject == null)
      return false;

    subject.logout();
    return true;
  }

  public UserAuthAttributes getUserAuthAttributes(String cqName) {
    // Long uniqueId = cqNameVsUserAuth.get(cqName);
    // return uniqueIdVsUserAuth.get(uniqueId);
    return cqNameVsUserAuth.get(cqName);
  }

  public void setUserAuthAttributesForCq(String cqName, long uniqueId, boolean isDurable) {
    UserAuthAttributes uaa = this.uniqueIdVsUserAuth.get(uniqueId);

    if (uaa != null) {
      if (!isDurable)
        this.cqNameVsUserAuth.put(cqName, uaa);
      else {
        UserAuthAttributes oldUaa = this.cqNameVsUserAuth.put(cqName, uaa);
        if (oldUaa != null) {
          if (oldUaa != uaa)// clean earlier one
          {
            this.cleanUserAuth(oldUaa);
            // add durable(increment)
            uaa.setDurable();
          } else {
            // if looks extra call from client
          }
        } else {
          uaa.setDurable();
        }
      }
    }
  }

  public void removeUserAuthAttributesForCq(String cqName, boolean isDurable) {
    UserAuthAttributes uaa = this.cqNameVsUserAuth.remove(cqName);
    if (uaa != null && isDurable)
      uaa.unsetDurable();
  }

  public boolean removeUserId(long userId, boolean keepAlive) {
    UserAuthAttributes uaa = uniqueIdVsUserAuth.get(userId);
    if (uaa != null && !(uaa.isDurable() && keepAlive)) {
      uaa = uniqueIdVsUserAuth.remove(userId);
      logger.debug("UserAuth of {} removed.");
      if (uaa != null) {
        cleanUserAuth(uaa);
        return true;
      }
    }
    return false;
  }



  public void cleanUserAuth(UserAuthAttributes userAuth) {
    if (userAuth != null) {
      AuthorizeRequest authReq = userAuth.getAuthzRequest();
      try {
        if (authReq != null) {
          authReq.close();
          authReq = null;
        }
      } catch (Exception ex) {
        // TODO:hitesh
        /*
         * if (securityLogger.warningEnabled()) { securityLogger.warning( LocalizedStrings.
         * String.
         * format("%s: An exception was thrown while closing client authorization callback. %s",
         * new Object[] {"", ex})); }
         */
      }
      try {
        AuthorizeRequestPP postAuthzReq = userAuth.getPostAuthzRequest();
        if (postAuthzReq != null) {
          postAuthzReq.close();
          postAuthzReq = null;
        }
      } catch (Exception ex) {
        // TODO:hitesh
        /*
         * if (securityLogger.warningEnabled()) { securityLogger.warning( LocalizedStrings.
         * String.
         * format("%s: An exception was thrown while closing client post-process authorization callback. %s"
         * ,
         * new Object[] {"", ex})); }
         */
      }
    }

  }

  public void cleanup(boolean fromCacheClientProxy) {
    for (UserAuthAttributes userAuth : this.uniqueIdVsUserAuth.values()) {
      // isDurable is checked for multiuser in CQ
      if (!fromCacheClientProxy && !userAuth.isDurable()) {// from serverConnection class
        cleanUserAuth(userAuth);
      } else if (fromCacheClientProxy && userAuth.isDurable()) {// from cacheclientProxy class
        cleanUserAuth(userAuth);
      }
    }
  }

  public void fillPreviousCQAuth(ClientUserAuths previousClientUserAuths) {
    for (Iterator<Map.Entry<String, UserAuthAttributes>> iter =
        previousClientUserAuths.cqNameVsUserAuth.entrySet().iterator(); iter.hasNext();) {
      Map.Entry<String, UserAuthAttributes> ent = iter.next();
      String cqName = ent.getKey();
      UserAuthAttributes prevUaa = ent.getValue();
      UserAuthAttributes newUaa = this.cqNameVsUserAuth.putIfAbsent(cqName, prevUaa);

      if (newUaa != null) {
        previousClientUserAuths.cleanUserAuth(prevUaa);
      }
    }
  }
}
