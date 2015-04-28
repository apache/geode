/*=========================================================================
 * Copyright Copyright (c) 2000-2014 Pivotal Software, Inc. All Rights Reserved.
 * This product is protected by U.S. and international copyright
 * and intellectual property laws. Pivotal products are covered by
 * more patents listed at http://www.pivotal.io/patents.
 * $Id: CompiledID.java,v 1.2 2005/02/01 17:19:20 vaibhav Exp $
 *=========================================================================
 */
package com.gemstone.gemfire.cache.query.internal;

import java.util.*;
import com.gemstone.gemfire.cache.*;
import com.gemstone.gemfire.cache.query.*;
import com.gemstone.gemfire.cache.Region;
import com.gemstone.gemfire.internal.cache.BucketRegion;
import com.gemstone.gemfire.internal.cache.LocalDataSet;
import com.gemstone.gemfire.internal.cache.PartitionedRegion;
import com.gemstone.gemfire.internal.i18n.LocalizedStrings;


/**
 * Class Description
 *
 * @author      ericz
 */



public class CompiledRegion extends AbstractCompiledValue {
  private String regionPath;
  
  
  public CompiledRegion(String regionPath) {
    this.regionPath = regionPath;
  }
  
  public int getType() {
    return RegionPath;
  }
  
  public String getRegionPath() {
    return this.regionPath;
  }
  
  public Object evaluate(ExecutionContext context)
  throws RegionNotFoundException {
    Region rgn;
    Cache cache = context.getCache();
    // do PR bucketRegion substitution here for expressions that evaluate to a Region.
    PartitionedRegion pr = context.getPartitionedRegion();

    
    if (pr != null && pr.getFullPath().equals(this.regionPath)) {
      rgn = context.getBucketRegion();
    }else if(pr != null) {
    //Asif : This is a   very tricky solution to allow equijoin queries on PartitionedRegion locally 
      //We have possibly got a situation of equijoin. it may be across PRs. so use the context's bucket region
      // to get ID and then retrieve the this region's bucket region
      BucketRegion br = context.getBucketRegion();
      int bucketID = br.getId();
      //Is current region a partitioned region
      rgn = cache.getRegion(this.regionPath);
      if(rgn.getAttributes().getDataPolicy().withPartitioning()) {
        // convert it into bucket region.
        PartitionedRegion prLocal = (PartitionedRegion)rgn;
        rgn = prLocal.getDataStore().getLocalBucketById(bucketID);
      }
      
    }
    else {
      rgn = cache.getRegion(this.regionPath);
    }
    
    if (rgn == null) {
      // if we couldn't find the region because the cache is closed, throw
      // a CacheClosedException
      if (cache.isClosed()) {
        throw new CacheClosedException();
      }
      throw new RegionNotFoundException(LocalizedStrings.CompiledRegion_REGION_NOT_FOUND_0.toLocalizedString(this.regionPath));
    }
    
    if (context.isCqQueryContext()) {
      return new QRegion(rgn, true, context);
    } else {
      return new QRegion(rgn, false, context);  
    }
  }
  
  @Override
  public void generateCanonicalizedExpression(StringBuffer clauseBuffer,ExecutionContext context ) throws NameResolutionException{
    clauseBuffer.insert(0, regionPath);
    //rahul : changed for running queries on partitioned region.
    //clauseBuffer.insert(0, ((QRegion)this.evaluate(context)).getFullPath());
  }
  
  @Override
  public void getRegionsInQuery(Set regionsInQuery, Object[] parameters) {
    regionsInQuery.add(this.regionPath);
  }
  
  
}
