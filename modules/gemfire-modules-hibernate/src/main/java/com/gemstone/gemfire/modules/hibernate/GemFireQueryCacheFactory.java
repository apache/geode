/*=========================================================================
 * Copyright (c) 2010-2014 Pivotal Software, Inc. All Rights Reserved.
 * This product is protected by U.S. and international copyright
 * and intellectual property laws. Pivotal products are covered by
 * one or more patents listed at http://www.pivotal.io/patents.
 *=========================================================================
 */
package com.gemstone.gemfire.modules.hibernate;

import java.util.Properties;

import org.hibernate.HibernateException;
import org.hibernate.cache.QueryCache;
import org.hibernate.cache.QueryCacheFactory;
import org.hibernate.cache.UpdateTimestampsCache;
import org.hibernate.cfg.Settings;

/**
 * Defines a factory for query cache instances. These factories are responsible
 * for creating individual QueryCache instances.
 * 
 */
public class GemFireQueryCacheFactory implements QueryCacheFactory {
  public QueryCache getQueryCache(String regionName,
      UpdateTimestampsCache updateTimestampsCache, Settings settings,
      Properties props) throws HibernateException {
    return new org.hibernate.cache.StandardQueryCache(settings, props,
        updateTimestampsCache, regionName);
  }
}
