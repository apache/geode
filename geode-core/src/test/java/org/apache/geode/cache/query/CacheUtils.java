/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
/*
 * Utils.java
 *
 * Created on March 8, 2005, 4:16 PM
 */
package org.apache.geode.cache.query;

import org.apache.geode.LogWriter;
import org.apache.geode.cache.*;
import org.apache.geode.cache.query.types.CollectionType;
import org.apache.geode.cache.query.types.ObjectType;
import org.apache.geode.distributed.DistributedSystem;
import org.apache.geode.internal.cache.GemFireCacheImpl;

import java.util.Iterator;
import java.util.Properties;
import java.util.Set;

import static org.apache.geode.distributed.ConfigurationProperties.MCAST_PORT;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.fail;

/**
 * 
 */
public class CacheUtils {

  static Properties props = new Properties();
  static DistributedSystem ds;
  static volatile Cache cache;
  static QueryService qs;
  static {
    try {
      init();
    }
    catch (Exception e) {
      e.printStackTrace();
    }
  }

  static void init() throws Exception {
    if (GemFireCacheImpl.getInstance() == null) {
      props.setProperty(MCAST_PORT, "0");
      cache = new CacheFactory(props).create();
    } else {
      cache = GemFireCacheImpl.getInstance();
    }
      ds = cache.getDistributedSystem();
      qs = cache.getQueryService();
  }

  public static Cache getCache() {
    return cache;
  }

  public static void startCache() {
    try {
      if (cache.isClosed()) {
        cache = new CacheFactory(props).create();
        ds = cache.getDistributedSystem();
        qs = cache.getQueryService();
      }
    }
    catch (Exception e) {
      e.printStackTrace();
    }
  }

  public static void closeCache() {
    try {
      if (!cache.isClosed()) {
        cache.close();
      }
    }
    catch (Exception e) {
      e.printStackTrace();
    }
  }

  public static void restartCache() {
    try {
      if (!cache.isClosed()) {
        cache.close();
      }
      cache = new CacheFactory(props).create();
      ds = cache.getDistributedSystem();
      qs = cache.getQueryService();
    }
    catch (Exception e) {
      e.printStackTrace();
    }
  }

  public static Region createRegion(String regionName, Class valueConstraint, Scope scope) {
    try {
      AttributesFactory attributesFactory = new AttributesFactory();
      attributesFactory.setValueConstraint(valueConstraint);
      if( scope != null) {
        attributesFactory.setScope( scope);
      }
      RegionAttributes regionAttributes = attributesFactory
          .create();
      Region region = cache.createRegion(regionName, regionAttributes);
      return region;
    }
    catch (Exception e) {
      e.printStackTrace();
    }
    return null;
  }
  
  public static Region createRegion(String regionName,
      RegionAttributes regionAttributes, boolean flag) {
    try {
      Region region = cache.createRegion(regionName, regionAttributes);
      return region;
    } catch (Exception e) {
      e.printStackTrace();
    }
    return null;
  }
  
  public static Region createRegion(String regionName, Class valueConstraint) {
    return createRegion(regionName, valueConstraint, null);
  }

  public static Region createRegion(String regionName, Class valueConstraint,
      boolean indexMaintenanceSynchronous) {
    try {
      AttributesFactory attributesFactory = new AttributesFactory();
      attributesFactory.setValueConstraint(valueConstraint);
      attributesFactory
          .setIndexMaintenanceSynchronous(indexMaintenanceSynchronous);
      RegionAttributes regionAttributes = attributesFactory
          .create();
      Region region = cache.createRegion(regionName, regionAttributes);
      return region;
    }
    catch (Exception e) {
      e.printStackTrace();
    }
    return null;
  } 

  public static Region createRegion(Region parentRegion, String regionName,
      Class valueConstraint) {
    try {
      AttributesFactory attributesFactory = new AttributesFactory();
      if (valueConstraint != null)
          attributesFactory.setValueConstraint(valueConstraint);
      RegionAttributes regionAttributes = attributesFactory
          .create();
      Region region = parentRegion
          .createSubregion(regionName, regionAttributes);
      return region;
    }
    catch (Exception e) {
      e.printStackTrace();
    }
    return null;
  }

  public static Region getRegion(String regionPath) {
    return cache.getRegion(regionPath);
  }

  public static QueryService getQueryService() {
    if (cache.isClosed()) startCache();
    return cache.getQueryService();
  }

  public static LogWriter getLogger() {
    if (cache == null) {
      return null;
    }
    return cache.getLogger();
  }

  public static void log(Object message) {
    Cache cache = GemFireCacheImpl.getInstance();
    if (cache != null) {
      cache.getLogger().fine(message.toString());
    }
  }

  public static CacheTransactionManager getCacheTranxnMgr() {
    return cache.getCacheTransactionManager();
  }
  
  public static void compareResultsOfWithAndWithoutIndex(SelectResults[][] r,
       Object test) {
    Set set1 = null;
    Set set2 = null;
    Iterator itert1 = null;
    Iterator itert2 = null;
    ObjectType type1, type2;
    for (int j = 0; j < r.length; j++) {
      CollectionType collType1 = r[j][0].getCollectionType();
      CollectionType collType2 = r[j][1].getCollectionType();
      type1 = collType1.getElementType();
      type2 = collType2.getElementType();
      if (collType1.getSimpleClassName().equals(collType2.getSimpleClassName())) {
        log("Both SelectResults are of the same Type i.e.--> "
            + collType1);
      }
      else {
        log("Collection type are : " + collType1 + "and  "
            + collType2);
        fail("FAILED:Select results Collection Type is different in both the cases. CollectionType1="+collType1 + " CollectionType2="+collType2);
      }
      if (type1.equals(type2)) {
        log("Both SelectResults have same element Type i.e.--> "
            + type1);
      }
      else {
        log("Classes are :  type1=" + type1.getSimpleClassName() + " type2= "
            + type2.getSimpleClassName());
        fail("FAILED:SelectResult Element Type is different in both the cases. Type1="+ type1 + " Type2="+ type2);
      }
      
      if (collType1.equals(collType2)) {
        log("Both SelectResults are of the same Type i.e.--> "
            + collType1);
      }
      else {
        log("Collections are : " + collType1 + " "
            + collType2);
        fail("FAILED:SelectResults Collection Type is different in both the cases. CollType1="+ collType1 + " CollType2="+ collType2);
      }
      if (r[j][0].size() == r[j][1].size()) {
        log("Both SelectResults are of Same Size i.e.  Size= "
            + r[j][1].size());
      }
      else {
        fail("FAILED:SelectResults size is different in both the cases. Size1="
                + r[j][0].size() + " Size2 = " + r[j][1].size());
      }
      set2 = ((r[j][1]).asSet());
      set1 = ((r[j][0]).asSet());
//      boolean pass = true;
      itert1 = set1.iterator();
      while (itert1.hasNext()) {
        Object p1 = itert1.next();
        itert2 = set2.iterator();

        boolean exactMatch = false;
        while (itert2.hasNext()) {
          Object p2 = itert2.next();
          if (p1 instanceof Struct) {
            Object[] values1 = ((Struct)p1).getFieldValues();
            Object[] values2 = ((Struct)p2).getFieldValues();
            assertEquals(values1.length, values2.length);
            boolean elementEqual = true;
            for (int i = 0; i < values1.length; ++i) {
              elementEqual = elementEqual
                  && ((values1[i] == values2[i]) || values1[i]
                      .equals(values2[i]));
            }
            exactMatch = elementEqual;
          }
          else {
            exactMatch = (p2 == p1) || p2.equals(p1);
          }
          if (exactMatch) {
            break;
          }
        }
        if (!exactMatch) {
          fail("Atleast one element in the pair of SelectResults supposedly identical, is not equal ");
        }
      }
    }
  }

  public static boolean compareResultsOfWithAndWithoutIndex(SelectResults[][] r ) { 
    boolean ok = true; 
    Set set1 = null; 
    Set set2 = null; 
    Iterator itert1 = null; 
    Iterator itert2 = null; 
    ObjectType type1, type2; 
    outer:  for (int j = 0; j < r.length; j++) { 
      CollectionType collType1 = r[j][0].getCollectionType(); 
      CollectionType collType2 = r[j][1].getCollectionType(); 
      type1 = collType1.getElementType(); 
      type2 = collType2.getElementType(); 

      if(collType1.getSimpleClassName().equals(collType2.getSimpleClassName())) { 
        log("Both SelectResults are of the same Type i.e.--> " + 
            collType1); 
      } else { 
        log("Collection type are : " + collType1 + "and  " 
            + collType2); 
        //test.fail("FAILED:Select results Collection Type is different in both the cases. CollectionType1="+collType1 + " CollectionType2="+collType2); 
        ok = false; 
        break; 
      } 
      if (type1.equals(type2)) { 
        log("Both SelectResults have same element Type i.e.--> " 
            + type1); 
      } else { 
        log("Classes are :  type1=" + type1.getSimpleClassName() + 
            " type2= " + type2.getSimpleClassName()); 
        //test.fail("FAILED:SelectResult Element Type is different in both the cases. Type1="+ type1 + " Type2="+ type2); 
        ok = false; 
        break; 
      } 

      if (collType1.equals(collType2)) { 
        log("Both SelectResults are of the same Type i.e.--> " 
            + collType1); 
      } 
      else { 
        log("Collections are : " + collType1 + " " 
            + collType2); 
        //test.fail("FAILED:SelectResults Collection Type is different in both the cases. CollType1="+ collType1 + " CollType2="+ collType2); 
        ok = false; 
        break; 
      } 
      if (r[j][0].size() == r[j][1].size()) { 
        log("Both SelectResults are of Same Size i.e.  Size= " 
            + r[j][1].size()); 
      } 
      else { 
        //test.fail("FAILED:SelectResults size is different in both the cases. Size1="  + r[j][0].size() + " Size2 = " + r[j][1].size()); 
        ok = false; 
        break; 
      } 
      set2 = (((SelectResults)r[j][1]).asSet()); 
      set1 = (((SelectResults)r[j][0]).asSet()); 
      boolean pass = true; 
      itert1 = set1.iterator(); 
      while (itert1.hasNext()) { 
        Object p1 = itert1.next(); 
        itert2 = set2.iterator(); 

        boolean exactMatch = false; 
        while (itert2.hasNext()) { 
          Object p2 = itert2.next(); 
          if (p1 instanceof Struct) { 
            Object[] values1 = ((Struct)p1).getFieldValues(); 
            Object[] values2 = ((Struct)p2).getFieldValues(); 
            //test.assertIndexDetailsEquals(values1.length, values2.length);
            if(values1.length != values2.length) { 
              ok = false; 
              break outer; 
            } 
            boolean elementEqual = true; 
            for (int i = 0; i < values1.length; ++i) { 
              if(values1[i] != null){
                elementEqual = elementEqual && ((values1[i] == values2[i]) || values1[i].equals(values2[i]));
              } else{
                elementEqual = elementEqual && ((values1[i] == values2[i]));
              }
            } 
            exactMatch = elementEqual; 
          } 
          else { 
            exactMatch = (p2 == p1) || p2.equals(p1); 
          } 
          if (exactMatch) { 
            break; 
          } 
        } 
        if (!exactMatch) { 
          //test.fail("Atleast one element in the pair of SelectResults supposedly identical, is not equal "); 
          ok = false; 
          break outer; 
        } 
      } 
    } 
    return ok; 
  } 
  
}
