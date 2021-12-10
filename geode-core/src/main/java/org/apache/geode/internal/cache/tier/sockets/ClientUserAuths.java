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
import static org.apache.geode.logging.internal.spi.LoggingProvider.SECURITY_LOGGER_NAME;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.stream.Collectors;

import org.apache.logging.log4j.Logger;
import org.apache.shiro.subject.Subject;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.TestOnly;

import org.apache.geode.internal.security.AuthorizeRequest;
import org.apache.geode.internal.security.AuthorizeRequestPP;
import org.apache.geode.logging.internal.log4j.api.LogService;

/**
 * This is per ServerConnection or per CacheClientProxy, corresponding to only one client
 * connection.
 * Credentials should usually be just one, only multiple in multi-user case.
 */
public class ClientUserAuths {
  private static final Logger logger = LogService.getLogger();
  private static final Logger secureLogger = LogService.getLogger(SECURITY_LOGGER_NAME);

  private final ConcurrentMap<Long, UserAuthAttributes> uniqueIdVsUserAuth =
      new ConcurrentHashMap<>();
  private final ConcurrentMap<String, UserAuthAttributes> cqNameVsUserAuth =
      new ConcurrentHashMap<>();
  // use a list to store all the subjects that's created for this uniqueId
  // In the expirable credential case, there will be multiple
  // subjects created associated with one uniqueId. We always save the current subject to the top of
  // the list. The rest are "to-be-retired".
  private final ConcurrentMap<Long, List<Subject>> uniqueIdVsSubjects =
      new ConcurrentHashMap<>();
  private final ConcurrentMap<String, Long> cqNameVsUniqueId = new ConcurrentHashMap<>();

  private final SubjectIdGenerator idGenerator;

  public Long putUserAuth(UserAuthAttributes userAuthAttr) {
    final Long newId = getNextID();
    uniqueIdVsUserAuth.put(newId, userAuthAttr);
    return newId;
  }


  public long putSubject(@NotNull Subject subject, long existingUniqueId) {
    final long newId;
    if (existingUniqueId == 0 || existingUniqueId == NOT_A_USER_ID) {
      newId = getNextID();
    } else {
      newId = existingUniqueId;
    }

    // we are saving all the subjects that's related to this uniqueId
    // we cannot immediately log out the old subject of this uniqueId because
    // it might already be bound to another thread and doing operations. If
    // we log out that subject immediately, that thread "authorize" would get null principal.
    synchronized (this) {
      List<Subject> subjects;
      if (!uniqueIdVsSubjects.containsKey(newId)) {
        secureLogger.debug("Subject of {} added.", newId);
        subjects = new ArrayList<>();
        uniqueIdVsSubjects.put(newId, subjects);
      } else {
        secureLogger.debug("Subject of {} replaced.", newId);
        subjects = uniqueIdVsSubjects.get(newId);
      }
      // always add the latest subject to the top of the list;
      subjects.add(0, subject);
    }
    return newId;
  }

  public ClientUserAuths(SubjectIdGenerator idGenerator) {
    this.idGenerator = idGenerator;
  }

  synchronized long getNextID() {
    final long uniqueId = idGenerator.generateId();
    if (uniqueId == -1) {
      // now every user need to reauthenticate as we are short of Ids..
      // though possibility of this is rare.
      uniqueIdVsUserAuth.clear();
      return idGenerator.generateId();
    }
    return uniqueId;
  }

  public UserAuthAttributes getUserAuthAttributes(final Long userId) {
    return uniqueIdVsUserAuth.get(userId);
  }

  @TestOnly
  protected synchronized Collection<Subject> getAllSubjects() {
    List<Subject> all = uniqueIdVsSubjects.values().stream()
        .flatMap(List::stream)
        .collect(Collectors.toList());
    return Collections.unmodifiableCollection(all);
  }

  public synchronized Subject getSubject(final Long userId) {
    List<Subject> subjects = uniqueIdVsSubjects.get(userId);
    if (subjects == null || subjects.isEmpty()) {
      return null;
    }
    return subjects.get(0);
  }

  public synchronized void removeSubject(final Long userId) {
    List<Subject> subjects = uniqueIdVsSubjects.remove(userId);
    if (subjects == null) {
      return;
    }
    secureLogger.debug("{} Subjects of {} removed.", subjects.size(), userId);
    subjects.forEach(Subject::logout);
  }

  public UserAuthAttributes getUserAuthAttributes(final String cqName) {
    return cqNameVsUserAuth.get(cqName);
  }

  public Subject getSubject(final String cqName) {
    Long uniqueId = cqNameVsUniqueId.get(cqName);
    if (uniqueId == null) {
      return null;
    }
    return getSubject(uniqueId);
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
    // for old security model
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

    // for integrated security, doesn't matter if this is called from proxy
    // or from the connection, we are closing the client connection
    synchronized (this) {
      for (final Long subjectId : uniqueIdVsSubjects.keySet()) {
        removeSubject(subjectId);
      }
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
