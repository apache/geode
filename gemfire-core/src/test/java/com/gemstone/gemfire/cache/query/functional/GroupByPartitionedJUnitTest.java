package com.gemstone.gemfire.cache.query.functional;

import org.junit.experimental.categories.Category;

import com.gemstone.gemfire.cache.AttributesFactory;
import com.gemstone.gemfire.cache.PartitionAttributesFactory;
import com.gemstone.gemfire.cache.Region;
import com.gemstone.gemfire.cache.query.CacheUtils;
import com.gemstone.gemfire.test.junit.categories.IntegrationTest;

/**
 * 
 * @author ashahid
 *
 */
@Category(IntegrationTest.class)
public class GroupByPartitionedJUnitTest extends GroupByTestImpl {

//  public static Test suite() {
//    TestSuite suite = new TestSuite(GroupByPartitionedJUnitTest.class);
//    return suite;
//  }

  @Override
  public Region createRegion(String regionName, Class valueConstraint) {
    PartitionAttributesFactory paf = new PartitionAttributesFactory();
    AttributesFactory af = new AttributesFactory();
    af.setPartitionAttributes(paf.create());
    af.setValueConstraint(valueConstraint);
    Region r1 = CacheUtils.createRegion(regionName, af.create(), false);
    return r1;

  }
}
