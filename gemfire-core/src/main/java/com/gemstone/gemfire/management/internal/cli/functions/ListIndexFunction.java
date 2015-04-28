/*
 * =========================================================================
 *  Copyright (c) 2002-2014 Pivotal Software, Inc. All Rights Reserved.
 *  This product is protected by U.S. and international copyright
 *  and intellectual property laws. Pivotal products are covered by
 *  more patents listed at http://www.pivotal.io/patents.
 * ========================================================================
 */

package com.gemstone.gemfire.management.internal.cli.functions;

import java.util.HashSet;
import java.util.Set;

import com.gemstone.gemfire.cache.Cache;
import com.gemstone.gemfire.cache.CacheFactory;
import com.gemstone.gemfire.cache.execute.FunctionAdapter;
import com.gemstone.gemfire.cache.execute.FunctionContext;
import com.gemstone.gemfire.cache.query.Index;
import com.gemstone.gemfire.distributed.DistributedMember;
import com.gemstone.gemfire.internal.InternalEntity;
import com.gemstone.gemfire.management.internal.cli.domain.IndexDetails;

/**
 * The ListIndexFunction class is a GemFire function used to collect all the index information on all Regions across
 * the entire GemFire Cache (distributed system).
 * </p>
 * @author John Blum
 * @see com.gemstone.gemfire.cache.Cache
 * @see com.gemstone.gemfire.cache.execute.Function
 * @see com.gemstone.gemfire.cache.execute.FunctionAdapter
 * @see com.gemstone.gemfire.cache.execute.FunctionContext
 * @see com.gemstone.gemfire.internal.InternalEntity
 * @see com.gemstone.gemfire.management.internal.cli.domain.IndexDetails
 * @since 7.0
 */
@SuppressWarnings("unused")
public class ListIndexFunction extends FunctionAdapter implements InternalEntity {

  protected Cache getCache() {
    return CacheFactory.getAnyInstance();
  }

  public String getId() {
    return ListIndexFunction.class.getName();
  }

  public void execute(final FunctionContext context) {
    try {
      final Set<IndexDetails> indexDetailsSet = new HashSet<IndexDetails>();

      final Cache cache = getCache();
      final DistributedMember member = cache.getDistributedSystem().getDistributedMember();

      for (final Index index : cache.getQueryService().getIndexes()) {
        indexDetailsSet.add(new IndexDetails(member, index));
      }

      context.getResultSender().lastResult(indexDetailsSet);
    }
    catch (Exception e) {
      context.getResultSender().sendException(e);
    }
  }

}
