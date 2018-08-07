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
package org.apache.geode.cache.query;

import static org.apache.geode.distributed.ConfigurationProperties.MCAST_PORT;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.fail;

import java.util.Iterator;
import java.util.Properties;
import java.util.Set;

import org.apache.logging.log4j.Logger;

import org.apache.geode.LogWriter;
import org.apache.geode.cache.AttributesFactory;
import org.apache.geode.cache.CacheFactory;
import org.apache.geode.cache.CacheTransactionManager;
import org.apache.geode.cache.CacheWriterException;
import org.apache.geode.cache.GatewayException;
import org.apache.geode.cache.Region;
import org.apache.geode.cache.RegionAttributes;
import org.apache.geode.cache.RegionExistsException;
import org.apache.geode.cache.Scope;
import org.apache.geode.cache.TimeoutException;
import org.apache.geode.cache.query.types.CollectionType;
import org.apache.geode.cache.query.types.ObjectType;
import org.apache.geode.distributed.DistributedSystem;
import org.apache.geode.internal.cache.GemFireCacheImpl;
import org.apache.geode.internal.cache.InternalCache;
import org.apache.geode.internal.logging.LogService;

public class CacheUtils {
  private static final Logger logger = LogService.getLogger();

  private static Properties props = new Properties();
  private static DistributedSystem ds;
  static volatile InternalCache cache;
  static QueryService qs;
  static {
    init();
  }

  private static void init()
      throws TimeoutException, CacheWriterException, GatewayException, RegionExistsException {
    if (GemFireCacheImpl.getInstance() == null) {
      props.setProperty(MCAST_PORT, "0");
      cache = (InternalCache) new CacheFactory(props).create();
    } else {
      cache = GemFireCacheImpl.getInstance();
    }
    ds = cache.getDistributedSystem();
    qs = cache.getQueryService();
  }

  public static InternalCache getCache() {
    return cache;
  }

  public static void startCache() {
    try {
      if (cache.isClosed()) {
        cache = (InternalCache) new CacheFactory(props).create();
        ds = cache.getDistributedSystem();
        qs = cache.getQueryService();
      }
    } catch (Exception e) {
      throw new AssertionError(e);
    }
  }

  public static void closeCache() {
    try {
      if (!cache.isClosed()) {
        cache.close();
      }
    } catch (Exception e) {
      throw new AssertionError(e);
    }
  }

  public static void restartCache() {
    try {
      if (!cache.isClosed()) {
        cache.close();
      }
      cache = (InternalCache) new CacheFactory(props).create();
      ds = cache.getDistributedSystem();
      qs = cache.getQueryService();
    } catch (Exception e) {
      throw new AssertionError(e);
    }
  }

  public static Region createRegion(String regionName, Class valueConstraint, Scope scope) {
    try {
      AttributesFactory attributesFactory = new AttributesFactory();
      attributesFactory.setValueConstraint(valueConstraint);
      if (scope != null) {
        attributesFactory.setScope(scope);
      }
      RegionAttributes regionAttributes = attributesFactory.create();
      return cache.createRegion(regionName, regionAttributes);
    } catch (Exception e) {
      throw new AssertionError(e);
    }
  }

  // TODO: paramter flag is unused
  public static Region createRegion(String regionName, RegionAttributes regionAttributes,
      boolean flag) {
    try {
      return cache.createRegion(regionName, regionAttributes);
    } catch (Exception e) {
      throw new AssertionError(e);
    }
  }

  public static Region createRegion(String regionName, Class valueConstraint) {
    return createRegion(regionName, valueConstraint, null);
  }

  public static Region createRegion(String regionName, Class valueConstraint,
      boolean indexMaintenanceSynchronous) {
    try {
      AttributesFactory attributesFactory = new AttributesFactory();
      attributesFactory.setValueConstraint(valueConstraint);
      attributesFactory.setIndexMaintenanceSynchronous(indexMaintenanceSynchronous);
      RegionAttributes regionAttributes = attributesFactory.create();
      return cache.createRegion(regionName, regionAttributes);
    } catch (Exception e) {
      throw new AssertionError(e);
    }
  }

  public static Region createRegion(Region parentRegion, String regionName, Class valueConstraint) {
    try {
      AttributesFactory attributesFactory = new AttributesFactory();
      if (valueConstraint != null)
        attributesFactory.setValueConstraint(valueConstraint);
      RegionAttributes regionAttributes = attributesFactory.create();
      return parentRegion.createSubregion(regionName, regionAttributes);
    } catch (Exception e) {
      throw new AssertionError(e);
    }
  }

  public static Region getRegion(String regionPath) {
    return cache.getRegion(regionPath);
  }

  public static QueryService getQueryService() {
    if (cache.isClosed())
      startCache();
    return cache.getQueryService();
  }

  public static LogWriter getLogger() {
    if (cache == null) {
      return null;
    }
    return cache.getLogger();
  }

  public static void log(Object message) {
    logger.debug(message);
  }

  public static CacheTransactionManager getCacheTranxnMgr() {
    return cache.getCacheTransactionManager();
  }

  public static void compareResultsOfWithAndWithoutIndex(SelectResults[][] r, Object test) {
    Set set1;
    Set set2;
    Iterator itert1;
    Iterator itert2;
    ObjectType type1;
    ObjectType type2;

    for (final SelectResults[] selectResults : r) {
      CollectionType collType1 = selectResults[0].getCollectionType();
      CollectionType collType2 = selectResults[1].getCollectionType();
      type1 = collType1.getElementType();
      type2 = collType2.getElementType();

      if (collType1.getSimpleClassName().equals(collType2.getSimpleClassName())) {
        log("Both SelectResults are of the same Type i.e.--> " + collType1);
      } else {
        log("Collection type are : " + collType1 + "and  " + collType2);
        fail(
            "FAILED:Select results Collection Type is different in both the cases. CollectionType1="
                + collType1 + " CollectionType2=" + collType2);
      }

      if (type1.equals(type2)) {
        log("Both SelectResults have same element Type i.e.--> " + type1);
      } else {
        log("Classes are :  type1=" + type1.getSimpleClassName() + " type2= "
            + type2.getSimpleClassName());
        fail("FAILED:SelectResult Element Type is different in both the cases. Type1=" + type1
            + " Type2=" + type2);
      }

      if (collType1.equals(collType2)) {
        log("Both SelectResults are of the same Type i.e.--> " + collType1);
      } else {
        log("Collections are : " + collType1 + " " + collType2);
        fail("FAILED:SelectResults Collection Type is different in both the cases. CollType1="
            + collType1 + " CollType2=" + collType2);
      }

      if (selectResults[0].size() == selectResults[1].size()) {
        log("Both SelectResults are of Same Size i.e.  Size= " + selectResults[1].size());
      } else {
        fail("FAILED:SelectResults size is different in both the cases. Size1="
            + selectResults[0].size() + " Size2 = " + selectResults[1].size());
      }

      set2 = selectResults[1].asSet();
      set1 = selectResults[0].asSet();

      itert1 = set1.iterator();
      while (itert1.hasNext()) {
        Object p1 = itert1.next();
        itert2 = set2.iterator();

        boolean exactMatch = false;
        while (itert2.hasNext()) {
          Object p2 = itert2.next();

          if (p1 instanceof Struct) {
            Object[] values1 = ((Struct) p1).getFieldValues();
            Object[] values2 = ((Struct) p2).getFieldValues();
            assertEquals(values1.length, values2.length);
            boolean elementEqual = true;
            for (int i = 0; i < values1.length; ++i) {
              elementEqual =
                  elementEqual && (values1[i] == values2[i] || values1[i].equals(values2[i]));
            }
            exactMatch = elementEqual;
          } else {
            exactMatch = p2 == p1 || p2.equals(p1);
          }
          if (exactMatch) {
            break;
          }
        }
        if (!exactMatch) {
          fail(
              "At least one element in the pair of SelectResults supposedly identical, is not equal");
        }
      }
    }
  }

  public static boolean compareResultsOfWithAndWithoutIndex(SelectResults[][] r) {
    boolean ok = true;
    Set set1;
    Set set2;
    Iterator itert1;
    Iterator itert2;
    ObjectType type1;
    ObjectType type2;

    // TODO: eliminate loop labels
    outer: for (final SelectResults[] aR : r) {
      CollectionType collType1 = aR[0].getCollectionType();
      CollectionType collType2 = aR[1].getCollectionType();
      type1 = collType1.getElementType();
      type2 = collType2.getElementType();

      if (collType1.getSimpleClassName().equals(collType2.getSimpleClassName())) {
        log("Both SelectResults are of the same Type i.e.--> " + collType1);
      } else {
        log("Collection type are : " + collType1 + "and  " + collType2);
        // test.fail("FAILED:Select results Collection Type is different in both the cases.
        // CollectionType1="+collType1 + " CollectionType2="+collType2);
        ok = false;
        break;
      }

      if (type1.equals(type2)) {
        log("Both SelectResults have same element Type i.e.--> " + type1);
      } else {
        log("Classes are :  type1=" + type1.getSimpleClassName() + " type2= "
            + type2.getSimpleClassName());
        // test.fail("FAILED:SelectResult Element Type is different in both the cases. Type1="+
        // type1 + " Type2="+ type2);
        ok = false;
        break;
      }

      if (collType1.equals(collType2)) {
        log("Both SelectResults are of the same Type i.e.--> " + collType1);
      } else {
        log("Collections are : " + collType1 + " " + collType2);
        // test.fail("FAILED:SelectResults Collection Type is different in both the cases.
        // CollType1="+ collType1 + " CollType2="+ collType2);
        ok = false;
        break;
      }

      if (aR[0].size() == aR[1].size()) {
        log("Both SelectResults are of Same Size i.e.  Size= " + aR[1].size());
      } else {
        // test.fail("FAILED:SelectResults size is different in both the cases. Size1=" +
        // r[j][0].size() + " Size2 = " + r[j][1].size());
        ok = false;
        break;
      }

      set2 = aR[1].asSet();
      set1 = aR[0].asSet();
      itert1 = set1.iterator();

      while (itert1.hasNext()) {
        Object p1 = itert1.next();
        itert2 = set2.iterator();

        boolean exactMatch = false;
        while (itert2.hasNext()) {
          Object p2 = itert2.next();
          if (p1 instanceof Struct) {
            Object[] values1 = ((Struct) p1).getFieldValues();
            Object[] values2 = ((Struct) p2).getFieldValues();
            if (values1.length != values2.length) {
              ok = false;
              break outer;
            }
            boolean elementEqual = true;
            for (int i = 0; i < values1.length; ++i) {
              if (values1[i] != null) {
                elementEqual =
                    elementEqual && (values1[i] == values2[i] || values1[i].equals(values2[i]));
              } else {
                elementEqual = elementEqual && values1[i] == values2[i];
              }
            }
            exactMatch = elementEqual;
          } else {
            exactMatch = p2 == p1 || p2.equals(p1);
          }
          if (exactMatch) {
            break;
          }
        }
        if (!exactMatch) {
          ok = false;
          break outer;
        }
      }
    }
    return ok;
  }

}
