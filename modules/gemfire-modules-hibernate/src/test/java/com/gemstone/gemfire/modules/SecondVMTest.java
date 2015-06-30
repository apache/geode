/*=========================================================================
 * Copyright (c) 2010-2014 Pivotal Software, Inc. All Rights Reserved.
 * This product is protected by U.S. and international copyright
 * and intellectual property laws. Pivotal products are covered by
 * one or more patents listed at http://www.pivotal.io/patents.
 *=========================================================================
 */
package com.gemstone.gemfire.modules;

import java.io.IOException;
import java.util.Iterator;
import java.util.Properties;
import java.util.Set;
import java.util.logging.Level;

import junit.framework.TestCase;

import org.hibernate.Session;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.gemstone.gemfire.cache.Cache;
import com.gemstone.gemfire.cache.CacheFactory;
import com.gemstone.gemfire.cache.GemFireCache;
import com.gemstone.gemfire.cache.Region;
import com.gemstone.gemfire.cache.Region.Entry;
import com.gemstone.gemfire.internal.cache.GemFireCacheImpl;

public class SecondVMTest extends TestCase {

  private Logger log = LoggerFactory.getLogger(getClass());
  
  public void testNoop() {
    
  }
  public void _testStartEmptyVM() throws IOException {
    Properties gemfireProperties = new Properties();
    gemfireProperties.setProperty("mcast-port", "5555");
    gemfireProperties.setProperty("log-level", "fine");
    Cache cache = new CacheFactory(gemfireProperties).create();
    System.in.read();
    Iterator it = cache.rootRegions().iterator();
    while (it.hasNext()) {
      Region r = (Region)it.next();
      System.out.println("Region:"+r);
      Iterator enIt = r.entrySet().iterator();
      while (enIt.hasNext()) {
        Region.Entry re = (Entry)enIt.next();
        System.out.println("key:"+re.getKey()+" value:"+re.getValue());
      }
    }
  }
  
  public void _testStartVM() throws Exception {
    java.util.logging.Logger.getLogger("org.hibernate").setLevel(Level.ALL);
    Session session = HibernateTestCase.getSessionFactory(null).openSession();
    log.info("SWAP:new session open");
    long id = 1;
    log.info("loading new person:"+(id));
    GemFireCache cache = GemFireCacheImpl.getInstance();
    Iterator<Region<?, ?>> rSet = cache.rootRegions().iterator();
    while (rSet.hasNext()) {
      Region<?, ?> r = rSet.next();
      log.info("SWAP:Region "+r);
      Iterator<?> keySet = r.keySet().iterator();
      while (keySet.hasNext()) {
        log.info("key:"+keySet.next());
      }
    }
    log.info("loading new person:"+(id));
    session.beginTransaction();
    Person p = (Person)session.load(Person.class, id);
    p.setFirstname("SecondVMfirstname"+id);
    log.info("loading events");
    log.info(p.getE()+"");
    session.getTransaction().commit();
    //System.in.read();
  }
  
}
