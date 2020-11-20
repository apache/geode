/*
 * Licensed to the Apache Software Foundation (ASF) under one or more contributor license
 * agreements. See the NOTICE file distributed with this work for additional information regarding
 * copyright ownership. The ASF licenses this file to you under the Apache License, Version 2.0 (the
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

package org.apache.geode.cache.lucene.internal.results;

import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Set;

import org.apache.logging.log4j.Logger;

import org.apache.geode.cache.CacheClosedException;
import org.apache.geode.cache.Region;
import org.apache.geode.cache.execute.FunctionContext;
import org.apache.geode.cache.execute.RegionFunctionContext;
import org.apache.geode.cache.partition.PartitionRegionHelper;
import org.apache.geode.internal.cache.EntrySnapshot;
import org.apache.geode.internal.cache.InternalCache;
import org.apache.geode.internal.cache.PrimaryBucketException;
import org.apache.geode.internal.cache.Token;
import org.apache.geode.internal.cache.execute.InternalFunction;
import org.apache.geode.internal.cache.execute.InternalFunctionInvocationTargetException;
import org.apache.geode.internal.security.SecurityService;
import org.apache.geode.logging.internal.log4j.api.LogService;
import org.apache.geode.security.ResourcePermission;

/**
 * {@link LuceneGetPageFunction} Returns the values of entries back to the user This behaves
 * basically like a getAll, but it does not invoke a cache loader
 */
public class LuceneGetPageFunction implements InternalFunction<Object> {
  private static final long serialVersionUID = 1L;
  public static final String ID = LuceneGetPageFunction.class.getName();

  private static final Logger logger = LogService.getLogger();

  @Override
  public void execute(FunctionContext context) {
    try {
      RegionFunctionContext ctx = (RegionFunctionContext) context;
      Region region = PartitionRegionHelper.getLocalDataForContext(ctx);
      Set<?> keys = ctx.getFilter();
      SecurityService securityService = ((InternalCache) ctx.getCache()).getSecurityService();
      List<PageEntry> results = new PageResults(keys.size());
      Object principal = context.getPrincipal();

      for (Object key : keys) {
        PageEntry entry = getEntry(region, key, securityService, principal);
        if (entry != null) {
          results.add(entry);
        }
      }
      ctx.getResultSender().lastResult(results);
    } catch (CacheClosedException | PrimaryBucketException e) {
      logger.debug("Exception during lucene query function", e);
      throw new InternalFunctionInvocationTargetException(e);
    }
  }

  protected PageEntry getEntry(final Region region, final Object key,
      SecurityService securityService, Object principal) {
    final EntrySnapshot entry = (EntrySnapshot) region.getEntry(key);
    if (entry == null) {
      return null;
    }

    Object value = entry.getRegionEntry().getValue(null);
    if (value == null || Token.isInvalidOrRemoved(value)) {
      return null;
    } else if (securityService.needPostProcess()) {
      value = securityService.postProcess(principal, region.getFullPath(), key, entry.getValue(),
          false);
    }

    return new PageEntry(key, value);
  }


  @Override
  public String getId() {
    return ID;
  }

  @Override
  public boolean optimizeForWrite() {
    return false;
  }

  @Override
  public Collection<ResourcePermission> getRequiredPermissions(String regionName) {
    return Collections.singletonList(new ResourcePermission(ResourcePermission.Resource.DATA,
        ResourcePermission.Operation.READ, regionName));
  }
}
