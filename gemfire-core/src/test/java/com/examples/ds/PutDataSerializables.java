/*=========================================================================
 * Copyright (c) 2010-2014 Pivotal Software, Inc. All Rights Reserved.
 * This product is protected by U.S. and international copyright
 * and intellectual property laws. Pivotal products are covered by
 * one or more patents listed at http://www.pivotal.io/patents.
 *=========================================================================
 */
package com.examples.ds;

import com.gemstone.gemfire.cache.*;
import com.gemstone.gemfire.distributed.DistributedSystem;
import java.util.Date;
import java.util.Properties;

/**
 * Places various objects that use {@link DataSerializer}s and {@link
 * Instantiator}s into a cache {@link Region}.  Among other things,
 * this is used to test bug 31573.
 *
 * @since 3.5
 * @author David Whitlock
 */
public class PutDataSerializables {

  public static void main(String[] args) throws Throwable {
    Properties props = new Properties();
    DistributedSystem system = DistributedSystem.connect(props);
    Cache cache = CacheFactory.create(system);
    AttributesFactory factory = new AttributesFactory();
    Region region =
      cache.createRegion("DataSerializable",
                           factory.create());
    region.put("User", new User("Fred", 42));

    new CompanySerializer();
    Address address = new Address();
    Company company = new Company("My Company", address);

    region.put("Company", company);
    region.put("Employee",
               new Employee(43, "Bob", new Date(), company));

    Thread.sleep(60 * 1000);
  }

}
