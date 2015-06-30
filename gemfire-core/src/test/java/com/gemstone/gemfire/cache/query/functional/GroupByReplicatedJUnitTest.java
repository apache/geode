package com.gemstone.gemfire.cache.query.functional;

import org.junit.experimental.categories.Category;

import com.gemstone.gemfire.cache.Region;
import com.gemstone.gemfire.cache.query.CacheUtils;
import com.gemstone.gemfire.test.junit.categories.IntegrationTest;

/**
 * 
 * @author ashahid
 *
 */
@Category(IntegrationTest.class)
public class GroupByReplicatedJUnitTest extends GroupByTestImpl {

  @Override
  public Region createRegion(String regionName, Class valueConstraint) {
    Region r1 = CacheUtils.createRegion(regionName, valueConstraint);
    return r1;

  }
}
