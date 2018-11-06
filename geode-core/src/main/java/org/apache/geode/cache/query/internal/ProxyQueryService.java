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
package org.apache.geode.cache.query.internal;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.List;

import org.apache.logging.log4j.Logger;

import org.apache.geode.cache.Region;
import org.apache.geode.cache.client.internal.ProxyCache;
import org.apache.geode.cache.client.internal.UserAttributes;
import org.apache.geode.cache.query.CqAttributes;
import org.apache.geode.cache.query.CqException;
import org.apache.geode.cache.query.CqExistsException;
import org.apache.geode.cache.query.CqQuery;
import org.apache.geode.cache.query.CqServiceStatistics;
import org.apache.geode.cache.query.Index;
import org.apache.geode.cache.query.IndexExistsException;
import org.apache.geode.cache.query.IndexInvalidException;
import org.apache.geode.cache.query.IndexNameConflictException;
import org.apache.geode.cache.query.IndexType;
import org.apache.geode.cache.query.MultiIndexCreationException;
import org.apache.geode.cache.query.Query;
import org.apache.geode.cache.query.QueryInvalidException;
import org.apache.geode.cache.query.QueryService;
import org.apache.geode.cache.query.RegionNotFoundException;
import org.apache.geode.cache.query.internal.cq.ClientCQ;
import org.apache.geode.cache.query.internal.cq.InternalCqQuery;
import org.apache.geode.internal.logging.LogService;

/**
 * A wrapper class over an actual QueryService instance. This is used when the
 * multiuser-authentication attribute is set to true.
 *
 * @see ProxyCache
 * @since GemFire 6.5
 */
public class ProxyQueryService implements QueryService {
  private static final Logger logger = LogService.getLogger();

  ProxyCache proxyCache;
  QueryService realQueryService;
  List<String> cqNames = new ArrayList<String>();

  public ProxyQueryService(ProxyCache proxyCache, QueryService realQueryService) {
    this.proxyCache = proxyCache;
    this.realQueryService = realQueryService;
  }

  public void closeCqs() {
    closeCqs(false);
  }

  public void closeCqs(boolean keepAlive) {
    preOp();
    try {
      ((DefaultQueryService) realQueryService).getCqService().closeAllCqs(!keepAlive,
          Arrays.asList(this.getCqs()), keepAlive);
      this.cqNames.clear();
    } catch (CqException cqe) {
      if (logger.isDebugEnabled()) {
        logger.debug("Unable to closeAll Cqs. Error: {}", cqe.getMessage(), cqe);
      }
    }
  }

  public Index createHashIndex(String indexName, String indexedExpression, String fromClause)
      throws IndexInvalidException, IndexNameConflictException, IndexExistsException,
      RegionNotFoundException, UnsupportedOperationException {
    throw new UnsupportedOperationException(
        "Index creation on the server is not supported from the client.");
  }

  public Index createHashIndex(String indexName, String indexedExpression, String fromClause,
      String imports) throws IndexInvalidException, IndexNameConflictException,
      IndexExistsException, RegionNotFoundException, UnsupportedOperationException {
    throw new UnsupportedOperationException(
        "Index creation on the server is not supported from the client.");
  }

  public Index createIndex(String indexName, IndexType indexType, String indexedExpression,
      String fromClause) throws IndexInvalidException, IndexNameConflictException,
      IndexExistsException, RegionNotFoundException, UnsupportedOperationException {
    throw new UnsupportedOperationException(
        "Index creation on the server is not supported from the client.");
  }

  public Index createIndex(String indexName, IndexType indexType, String indexedExpression,
      String fromClause, String imports) throws IndexInvalidException, IndexNameConflictException,
      IndexExistsException, RegionNotFoundException, UnsupportedOperationException {
    throw new UnsupportedOperationException(
        "Index creation on the server is not supported from the client.");
  }

  public Index createIndex(String indexName, String indexedExpression, String fromClause)
      throws IndexInvalidException, IndexNameConflictException, IndexExistsException,
      RegionNotFoundException, UnsupportedOperationException {
    throw new UnsupportedOperationException(
        "Index creation on the server is not supported from the client.");
  }

  public Index createIndex(String indexName, String indexedExpression, String fromClause,
      String imports) throws IndexInvalidException, IndexNameConflictException,
      IndexExistsException, RegionNotFoundException, UnsupportedOperationException {
    throw new UnsupportedOperationException(
        "Index creation on the server is not supported from the client.");
  }

  public Index createKeyIndex(String indexName, String indexedExpression, String fromClause)
      throws IndexInvalidException, IndexNameConflictException, IndexExistsException,
      RegionNotFoundException, UnsupportedOperationException {
    throw new UnsupportedOperationException(
        "Index creation on the server is not supported from the client.");
  }


  @Override
  public void defineKeyIndex(String indexName, String indexedExpression, String fromClause)
      throws RegionNotFoundException {
    throw new UnsupportedOperationException(
        "Index creation on the server is not supported from the client.");
  }

  @Override
  public void defineHashIndex(String indexName, String indexedExpression, String regionPath)
      throws RegionNotFoundException {
    throw new UnsupportedOperationException(
        "Index creation on the server is not supported from the client.");
  }

  @Override
  public void defineHashIndex(String indexName, String indexedExpression, String regionPath,
      String imports) throws RegionNotFoundException {
    throw new UnsupportedOperationException(
        "Index creation on the server is not supported from the client.");
  }

  @Override
  public void defineIndex(String indexName, String indexedExpression, String regionPath)
      throws RegionNotFoundException {
    throw new UnsupportedOperationException(
        "Index creation on the server is not supported from the client.");
  }

  @Override
  public void defineIndex(String indexName, String indexedExpression, String regionPath,
      String imports) throws RegionNotFoundException {
    throw new UnsupportedOperationException(
        "Index creation on the server is not supported from the client.");
  }

  @Override
  public List<Index> createDefinedIndexes() throws MultiIndexCreationException {
    throw new UnsupportedOperationException(
        "Index creation on the server is not supported from the client.");
  }

  @Override
  public boolean clearDefinedIndexes() {
    throw new UnsupportedOperationException(
        "Index creation on the server is not supported from the client.");
  }

  public void executeCqs() throws CqException {
    preOp();
    try {
      ((DefaultQueryService) realQueryService).getCqService()
          .executeCqs(Arrays.asList(this.getCqs()));
    } catch (CqException cqe) {
      if (logger.isDebugEnabled()) {
        logger.debug("Unable to execute cqs. Error: {}", cqe.getMessage(), cqe);
      }
    }
  }

  public void executeCqs(String regionName) throws CqException {
    preOp();
    try {
      ((DefaultQueryService) realQueryService).getCqService()
          .executeCqs(Arrays.asList(this.getCqs(regionName)));
    } catch (CqException cqe) {
      if (logger.isDebugEnabled()) {
        logger.debug("Unable to execute cqs on the specified region. Error: {}", cqe.getMessage(),
            cqe);
      }
    }
  }

  public CqQuery getCq(String cqName) {
    preOp();
    if (this.cqNames.contains(cqName)) {
      return this.realQueryService.getCq(cqName);
    } else {
      return null;
    }
  }

  public CqServiceStatistics getCqStatistics() {
    throw new UnsupportedOperationException();
  }

  public ClientCQ[] getCqs() {
    preOp();
    ClientCQ[] cqs = null;
    try {
      ArrayList<InternalCqQuery> cqList = new ArrayList<InternalCqQuery>();
      for (String name : this.cqNames) {
        cqList.add(((DefaultQueryService) realQueryService).getCqService().getCq(name));
      }
      cqs = new ClientCQ[cqList.size()];
      cqList.toArray(cqs);
    } catch (CqException cqe) {
      if (logger.isDebugEnabled()) {
        logger.debug("Unable to get Cqs. Error: {}", cqe.getMessage(), cqe);
      }
    }
    return cqs;
  }

  public ClientCQ[] getCqs(String regionName) throws CqException {
    preOp();
    Collection<? extends InternalCqQuery> cqs = null;
    try {
      ArrayList<CqQuery> cqList = new ArrayList<CqQuery>();
      cqs = ((DefaultQueryService) realQueryService).getCqService().getAllCqs(regionName);
      for (InternalCqQuery cq : cqs) {
        if (this.cqNames.contains(cq.getName())) {
          cqList.add((CqQuery) cq);
        }
      }
      ClientCQ[] results = new ClientCQ[cqList.size()];
      cqList.toArray(results);
      return results;
    } catch (CqException cqe) {
      if (logger.isDebugEnabled()) {
        logger.debug("Unable to get Cqs. Error: {}", cqe.getMessage(), cqe);
      }
      throw cqe;
    }
  }

  public Index getIndex(Region<?, ?> region, String indexName) {
    throw new UnsupportedOperationException(
        "Index operation on the server is not supported from the client.");
  }

  public Collection<Index> getIndexes() {
    throw new UnsupportedOperationException(
        "Index operation on the server is not supported from the client.");
  }

  public Collection<Index> getIndexes(Region<?, ?> region) {
    throw new UnsupportedOperationException(
        "Index operation on the server is not supported from the client.");
  }

  public Collection<Index> getIndexes(Region<?, ?> region, IndexType indexType) {
    throw new UnsupportedOperationException(
        "Index operation on the server is not supported from the client.");
  }

  public CqQuery newCq(String queryString, CqAttributes cqAttributes)
      throws QueryInvalidException, CqException {
    preOp(true);
    ClientCQ cq = null;
    try {
      cq = ((DefaultQueryService) realQueryService).getCqService().newCq(null, queryString,
          cqAttributes, ((DefaultQueryService) realQueryService).getPool(), false);
      cq.setProxyCache(this.proxyCache);
      this.cqNames.add(cq.getName());
    } catch (CqExistsException cqe) {
      // Should not throw in here.
      if (logger.isDebugEnabled()) {
        logger.debug("Unable to createCq. Error: {}", cqe.getMessage(), cqe);
      }
    } finally {
      postOp();
    }
    return cq;
  }

  public CqQuery newCq(String queryString, CqAttributes cqAttributes, boolean isDurable)
      throws QueryInvalidException, CqException {
    preOp(true);
    ClientCQ cq = null;
    try {
      cq = ((DefaultQueryService) realQueryService).getCqService().newCq(null, queryString,
          cqAttributes, ((DefaultQueryService) realQueryService).getPool(), isDurable);
      cq.setProxyCache(this.proxyCache);
      this.cqNames.add(cq.getName());
    } catch (CqExistsException cqe) {
      // Should not throw in here.
      if (logger.isDebugEnabled()) {
        logger.debug("Unable to createCq. Error: {}", cqe.getMessage(), cqe);
      }
    } finally {
      postOp();
    }
    return cq;
  }

  public CqQuery newCq(String cqName, String queryString, CqAttributes cqAttributes)
      throws QueryInvalidException, CqExistsException, CqException {
    preOp(true);
    try {
      if (cqName == null) {
        throw new IllegalArgumentException(
            "cqName must not be null");
      }
      ClientCQ cq = ((DefaultQueryService) realQueryService).getCqService().newCq(cqName,
          queryString, cqAttributes, ((DefaultQueryService) realQueryService).getPool(), false);
      cq.setProxyCache(proxyCache);
      this.cqNames.add(cq.getName());
      return cq;
    } finally {
      postOp();
    }
  }

  public CqQuery newCq(String cqName, String queryString, CqAttributes cqAttributes,
      boolean isDurable) throws QueryInvalidException, CqExistsException, CqException {
    preOp(true);
    try {
      if (cqName == null) {
        throw new IllegalArgumentException(
            "cqName must not be null");
      }
      ClientCQ cq = ((DefaultQueryService) realQueryService).getCqService().newCq(cqName,
          queryString, cqAttributes, ((DefaultQueryService) realQueryService).getPool(), isDurable);
      cq.setProxyCache(proxyCache);
      this.cqNames.add(cq.getName());
      return cq;
    } finally {
      postOp();
    }
  }

  public Query newQuery(String queryString) {
    preOp();
    return ((DefaultQueryService) realQueryService).newQuery(queryString, this.proxyCache);
  }

  public void removeIndex(Index index) {
    throw new UnsupportedOperationException(
        "Index operation on the server is not supported from the client.");
  }

  public void removeIndexes() {
    throw new UnsupportedOperationException(
        "Index operation on the server is not supported from the client.");
  }

  public void removeIndexes(Region<?, ?> region) {
    throw new UnsupportedOperationException(
        "Index operation on the server is not supported from the client.");
  }

  public void stopCqs() throws CqException {
    preOp();
    try {
      ((DefaultQueryService) realQueryService).getCqService().stopCqs(Arrays.asList(this.getCqs()));
    } catch (CqException cqe) {
      if (logger.isDebugEnabled()) {
        logger.debug("Unable to stop all CQs. Error: {}", cqe.getMessage(), cqe);
      }
    }
  }

  public void stopCqs(String regionName) throws CqException {
    preOp();
    try {
      ((DefaultQueryService) realQueryService).getCqService()
          .stopCqs(Arrays.asList(this.getCqs(regionName)));
    } catch (CqException cqe) {
      if (logger.isDebugEnabled()) {
        logger.debug("Unable to stop cqs on the specified region. Error: {}", cqe.getMessage(),
            cqe);
      }
    }
  }

  public List<String> getAllDurableCqsFromServer() throws CqException {
    preOp();
    try {
      ((DefaultQueryService) realQueryService).getAllDurableCqsFromServer();
    } catch (CqException cqe) {
      if (logger.isDebugEnabled()) {
        logger.debug("Unable to get all durablec client cqs on the specified region. Error: {}",
            cqe.getMessage(), cqe);
      }
    }
    return Collections.EMPTY_LIST;
  }

  private void preOp() {
    preOp(false);
  }

  private void preOp(boolean setTL) {
    if (this.proxyCache.isClosed()) {
      throw proxyCache.getCacheClosedException("Cache is closed for this user.");
    }
    if (setTL) {
      UserAttributes.userAttributes.set(this.proxyCache.getUserAttributes());
    }
  }

  private void postOp() {
    UserAttributes.userAttributes.set(null);
  }

}
