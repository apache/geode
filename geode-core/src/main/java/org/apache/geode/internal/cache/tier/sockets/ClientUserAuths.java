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

import static org.apache.geode.cache.client.internal.AuthenticateUserOp.NOT_A_USER_ID;

import java.util.Collection;
import java.util.Collections;
import java.util.Map;
import java.util.Random;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

import org.apache.logging.log4j.Logger;
import org.apache.shiro.subject.Subject;

import org.apache.geode.annotations.VisibleForTesting;
import org.apache.geode.internal.security.AuthorizeRequest;
import org.apache.geode.internal.security.AuthorizeRequestPP;
import org.apache.geode.logging.internal.log4j.api.LogService;

public class ClientUserAuths {
  private static final Logger logger = LogService.getLogger();

  private final ConcurrentMap<Long, UserAuthAttributes> uniqueIdVsUserAuth =
      new ConcurrentHashMap<>();
  private final ConcurrentMap<String, UserAuthAttributes> cqNameVsUserAuth =
      new ConcurrentHashMap<>();
  private final ConcurrentMap<Long, Subject> uniqueIdVsSubject = new ConcurrentHashMap<>();
  private final ConcurrentMap<String, Long> cqNameVsUniqueId = new ConcurrentHashMap<>();

  private final int m_seed;

  private Random uniqueIdGenerator;
  private long m_firstId;

  public Long putUserAuth(UserAuthAttributes userAuthAttr) {
    final Long newId = getNextID();
    uniqueIdVsUserAuth.put(newId, userAuthAttr);
    return newId;
  }

  public Long putSubject(Subject subject, long existingUniqueId) {
    final Long newId;
    if (existingUniqueId == 0 || existingUniqueId == NOT_A_USER_ID) {
      newId = getNextID();
    } else {
      newId = existingUniqueId;
    }

    Subject oldSubject = uniqueIdVsSubject.put(newId, subject);
    logger.info("Jinmei: Subject of {} replaced.", newId);
    removeSubject(oldSubject);
    logger.debug("Subject of {} added.", newId);
    return newId;
  }

  public ClientUserAuths(int clientProxyHashcode) {
    m_seed = clientProxyHashcode;
    uniqueIdGenerator = new Random(m_seed + System.currentTimeMillis());
    m_firstId = uniqueIdGenerator.nextLong();
  }

  synchronized long getNextID() {
    final long uniqueId = uniqueIdGenerator.nextLong();
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

  public UserAuthAttributes getUserAuthAttributes(final Long userId) {
    return uniqueIdVsUserAuth.get(userId);
  }

  @VisibleForTesting
  protected Collection<Subject> getSubjects() {
    return Collections.unmodifiableCollection(uniqueIdVsSubject.values());
  }

  public Subject getSubject(final Long userId) {
    return uniqueIdVsSubject.get(userId);
  }

  public void removeSubject(final Long userId) {
    logger.info("Jinmei: Subject of {} removed.", userId);
    removeSubject(uniqueIdVsSubject.remove(userId));
  }

  @VisibleForTesting
  void removeSubject(Subject subject) {
    if (subject == null) {
      return;
    }
    if (subject.getPrincipal() == null) {
      return;
    }
    logger.info("Jinmei: Subject {} log out.", subject.getPrincipal());
    subject.logout();
  }

  public UserAuthAttributes getUserAuthAttributes(final String cqName) {
    return cqNameVsUserAuth.get(cqName);
  }

  public Subject getSubject(final String cqName) {
    Long uniqueId = cqNameVsUniqueId.get(cqName);
    if (uniqueId != null) {
      return uniqueIdVsSubject.get(uniqueId);
    }
    return null;
  }

  public void setUserAuthAttributesForCq(final String cqName, final Long uniqueId,
      final boolean isDurable) {
    final UserAuthAttributes uaa = uniqueIdVsUserAuth.get(uniqueId);

    if (uaa != null) {
      if (!isDurable) {
        cqNameVsUserAuth.put(cqName, uaa);
      } else {
        final UserAuthAttributes oldUaa = cqNameVsUserAuth.put(cqName, uaa);
        if (oldUaa != null) {
          if (oldUaa != uaa) {
            cleanUserAuth(oldUaa);
            uaa.setDurable();
          }
        } else {
          uaa.setDurable();
        }
      }
    }
    cqNameVsUniqueId.put(cqName, uniqueId);
  }

  public void removeUserAuthAttributesForCq(final String cqName, final boolean isDurable) {
    final UserAuthAttributes uaa = cqNameVsUserAuth.remove(cqName);
    if (uaa != null && isDurable) {
      uaa.unsetDurable();
    }
    cqNameVsUniqueId.remove(cqName);
  }

  public void removeUserId(final Long userId, final boolean keepAlive) {
    UserAuthAttributes uaa = uniqueIdVsUserAuth.get(userId);
    if (uaa != null && !(uaa.isDurable() && keepAlive)) {
      uaa = uniqueIdVsUserAuth.remove(userId);
      logger.debug("UserAuth of {} removed.", userId);
      if (uaa != null) {
        cleanUserAuth(uaa);
      }
    }
  }

  public void cleanUserAuth(final UserAuthAttributes userAuth) {
    if (userAuth != null) {
      final AuthorizeRequest authReq = userAuth.getAuthzRequest();
      try {
        if (authReq != null) {
          authReq.close();
        }
      } catch (Exception ignored) {
      }
      try {
        final AuthorizeRequestPP postAuthzReq = userAuth.getPostAuthzRequest();
        if (postAuthzReq != null) {
          postAuthzReq.close();
        }
      } catch (Exception ignored) {
      }
    }

  }

  public void cleanup(boolean fromCacheClientProxy) {
    for (UserAuthAttributes userAuth : uniqueIdVsUserAuth.values()) {
      // isDurable is checked for multiuser in CQ
      if (!fromCacheClientProxy && !userAuth.isDurable()) {
        // from serverConnection class
        cleanUserAuth(userAuth);
      } else if (fromCacheClientProxy && userAuth.isDurable()) {
        // from CacheClientProxy class
        cleanUserAuth(userAuth);
      }
    }

    // Logout the subjects
    for (final Long subjectId : uniqueIdVsSubject.keySet()) {
      removeSubject(subjectId);
    }
  }

  public void fillPreviousCQAuth(ClientUserAuths previousClientUserAuths) {
    for (Map.Entry<String, UserAuthAttributes> ent : previousClientUserAuths.cqNameVsUserAuth
        .entrySet()) {
      final String cqName = ent.getKey();
      final UserAuthAttributes prevUaa = ent.getValue();
      final UserAuthAttributes newUaa = cqNameVsUserAuth.putIfAbsent(cqName, prevUaa);

      if (newUaa != null) {
        previousClientUserAuths.cleanUserAuth(prevUaa);
      }
    }
  }
}
