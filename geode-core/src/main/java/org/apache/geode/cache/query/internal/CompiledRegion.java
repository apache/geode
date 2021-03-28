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

import java.util.Set;

import org.apache.geode.cache.Cache;
import org.apache.geode.cache.CacheClosedException;
import org.apache.geode.cache.Region;
import org.apache.geode.cache.query.NameResolutionException;
import org.apache.geode.cache.query.RegionNotFoundException;
import org.apache.geode.internal.cache.BucketRegion;
import org.apache.geode.internal.cache.PartitionedRegion;
import org.apache.geode.internal.cache.partitioned.BucketId;


/**
 * Class Description
 *
 */



public class CompiledRegion extends AbstractCompiledValue {
  private final String regionPath;


  public CompiledRegion(String regionPath) {
    this.regionPath = regionPath;
  }

  @Override
  public int getType() {
    return RegionPath;
  }

  public String getRegionPath() {
    return regionPath;
  }

  @Override
  public Object evaluate(ExecutionContext context) throws RegionNotFoundException {
    Region<?, ?> region;
    Cache cache = context.getCache();
    // do PR bucketRegion substitution here for expressions that evaluate to a Region.
    PartitionedRegion pr = context.getPartitionedRegion();


    if (pr != null && pr.getFullPath().equals(regionPath)) {
      region = context.getBucketRegion();
    } else if (pr != null) {
      // Asif : This is a very tricky solution to allow equijoin queries on PartitionedRegion
      // locally
      // We have possibly got a situation of equijoin. it may be across PRs. so use the context's
      // bucket region
      // to get ID and then retrieve this region's bucket region
      BucketRegion br = context.getBucketRegion();
      BucketId bucketID = br.getId();
      // Is current region a partitioned region
      region = cache.getRegion(regionPath);
      if (region.getAttributes().getDataPolicy().withPartitioning()) {
        // convert it into bucket region.
        PartitionedRegion prLocal = (PartitionedRegion) region;
        region = prLocal.getDataStore().getLocalBucketById(bucketID);
      }

    } else {
      region = cache.getRegion(regionPath);
    }

    if (region == null) {
      // if we couldn't find the region because the cache is closed, throw
      // a CacheClosedException
      if (cache.isClosed()) {
        throw new CacheClosedException();
      }
      throw new RegionNotFoundException(
          String.format("Region not found: %s", regionPath));
    }

    if (context.isCqQueryContext()) {
      return new QRegion(region, true, context);
    } else {
      return new QRegion(region, false, context);
    }
  }

  @Override
  public void generateCanonicalizedExpression(StringBuilder clauseBuffer, ExecutionContext context)
      throws NameResolutionException {
    clauseBuffer.insert(0, regionPath);
    // rahul : changed for running queries on partitioned region.
    // clauseBuffer.insert(0, ((QRegion)this.evaluate(context)).getFullPath());
  }

  @Override
  public void getRegionsInQuery(Set<String> regionsInQuery, Object[] parameters) {
    regionsInQuery.add(regionPath);
  }


}
