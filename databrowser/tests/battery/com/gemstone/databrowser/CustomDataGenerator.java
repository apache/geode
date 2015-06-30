package com.gemstone.databrowser;

import hydra.BridgeHelper;
import hydra.CacheHelper;
import hydra.ConfigHashtable;
import hydra.ConfigPrms;
import hydra.DistributedSystemHelper;
import hydra.HydraVector;
import hydra.Log;
import hydra.PoolHelper;
import hydra.PoolPrms;
import hydra.RegionDescription;
import hydra.RegionHelper;
import hydra.RegionPrms;
import hydra.TestConfig;
import admin.jmx.RecyclePrms;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.Set;
import java.util.Vector;

import objects.ObjectHelper;
import query.QueryPrms;
import util.NameFactory;
import util.TestException;
import com.gemstone.gemfire.LogWriter;
import com.gemstone.gemfire.cache.AttributesFactory;
import com.gemstone.gemfire.cache.Cache;
import com.gemstone.gemfire.cache.CacheFactory;
import com.gemstone.gemfire.cache.DataPolicy;
import com.gemstone.gemfire.cache.Region;
import com.gemstone.gemfire.cache.RegionAttributes;
import com.gemstone.gemfire.cache.RegionExistsException;
import com.gemstone.gemfire.cache.client.Pool;
import com.gemstone.gemfire.cache.query.Query;
import com.gemstone.gemfire.cache.query.QueryService;
import com.gemstone.gemfire.cache.query.SelectResults;
import com.gemstone.gemfire.internal.cache.HARegion;
import com.gemstone.databrowser.Testutil;

/**
 * Test to pick and choose various methods for starting and creating regions of
 * a VM.
 * 
 * @author vreddy
 * 
 */
public class CustomDataGenerator {
  static ConfigHashtable conftab = TestConfig.tab();

  static LogWriter logger = Log.getLogWriter();

  protected static final String REGION_NAME = "";

  protected static int absoluteRegionCount;

  protected static String edgeRegionName;

  // start a cacheServer.
  public static void startServerCache() {
    String bridgeConfig = ConfigPrms.getBridgeConfig();
    startCache();
    BridgeHelper.startBridgeServer(bridgeConfig);
  }

  protected static void startCache() {
    String cacheConfig = ConfigPrms.getCacheConfig();
    Cache cache = CacheHelper.createCache(cacheConfig);
    createUserDefinedRegions(cache, true);
  }

  protected static void createUserDefinedRegions(Cache cache, boolean isServer) {
    Vector regionNames = TestConfig.tab().vecAt(RegionPrms.names, null);
    logger
        .fine("CustomDataGenerator:createRegion:: Number of more Regions to be created "
            + regionNames);
    for (int i = 0; i < regionNames.size(); i++) {
      String regionDescriptName = (String)(regionNames.get(i));
      logger.fine("createUserDefinedRegions ===>" + regionDescriptName);
      if (isServer && !regionDescriptName.startsWith("server")) {
        continue;
      }
      else if (!isServer && !regionDescriptName.startsWith("client")) {
        continue;
      }

      String regionName = RegionHelper.getRegionDescription(regionDescriptName)
          .getRegionName();
      try {

        AttributesFactory factory = RegionHelper
            .getAttributesFactory(regionDescriptName);
        RegionAttributes attributes = RegionHelper.getRegionAttributes(factory);
        Region reg = RegionHelper.createRegion(regionName, attributes);

        if (!attributes.getDataPolicy().equals(DataPolicy.PARTITION)) {
          int numsubRegions = conftab.intAt(RecyclePrms.regionDepth, 0);
          ArrayList subRegionsName = new ArrayList();
          Log.getLogWriter().fine(
              "The no of sub regions created for the Region are : "
                  + numsubRegions);
          int j = 0;
          while (j < numsubRegions) {
            String name = reg.getName() + "_" + j;
            j = j + 1;
            subRegionsName.add(name);
            Region subregion = reg.createSubregion(name, attributes);
            Log.getLogWriter().fine("subRegion : " + subregion);
          }
        }
        Log.getLogWriter().fine(
            "Created  region " + regionName + " with region descript name "
                + regionDescriptName);
      }
      catch (RegionExistsException e) {
        throw new TestException(e.getMessage());
      }
    }
  }

  public static void populateRegions() {
    Cache cache = CacheHelper.getCache();

    try {
      Set regions = cache.rootRegions();
      Iterator iter = regions.iterator();

      while (iter.hasNext()) {
        Region region = (Region)iter.next();
        String objectType = region.getAttributes().getValueConstraint()
            .getCanonicalName();
        populateRegion(region, objectType);
      }
    }
    catch (NullPointerException e) {
      Log.getLogWriter().error(e);
    }
  }

  public static void populateRegion(Region region, String objectType) {
    Log.getLogWriter().fine(
        "Populating region :" + region.getFullPath()
            + " with objects of type :" + objectType);
    boolean isPartitionRegion = false;
    if (region.getAttributes().getDataPolicy().equals(DataPolicy.PARTITION))
      isPartitionRegion = true;
    int noOfEntity = conftab.intAt(RecyclePrms.numberOfEntitiesInRegion, 0);
    Log.getLogWriter().fine(
        "num of entries put in the region is :::" + noOfEntity);
    for (int i = 0; i < noOfEntity; i++) {
      Object key = String.valueOf(i);

      Object value = ObjectHelper.createObject(objectType, i);
      if (isPartitionRegion)
        key = NameFactory.getNextPositiveObjectName();
      region.put(key, value);
    }
    Log.getLogWriter().fine(
        "Populated region :" + region.getFullPath() + " entrycount :"
            + region.size());
  }

  public static void RegionEntryUpdate() {

    int noOfEntity = conftab.intAt(RecyclePrms.numberOfEntitiesInRegion, 0);

    Cache cache = CacheFactory.getInstance(DistributedSystemHelper
        .getDistributedSystem());
    Object[] regionList = cache.rootRegions().toArray();
    int numRegs = regionList.length;

    for (int i = 0; i < numRegs; i++) {
      Region reg = (Region)regionList[i];
      if (!(reg instanceof HARegion)) {
        Set keys = reg.keySet();
        Iterator ite = keys.iterator();
        for (int j = 0; ite.hasNext() && j < noOfEntity; j++) {
          Object key = ite.next();
          Object value = reg.get(key);
          reg.put(key, value);
        }
      }
    }
  }

  public static void startEdgeClient() {
    String cacheConfig = RecyclePrms.getEdgeCacheConfig();
    Cache cache = CacheHelper.createCache(cacheConfig);
    createUserDefinedRegions(cache, false);
  }

  protected static void createRegion(String regionConfig, Cache cache) {
    RegionDescription description = RegionHelper
        .getRegionDescription(regionConfig);
    AttributesFactory factory = RegionHelper.getAttributesFactory(regionConfig);
    RegionAttributes attributes = RegionHelper.getRegionAttributes(factory);
    RegionHelper.createRegion(description.getRegionName(), attributes);
  }

  public static void startPeerCache() {
    startCache();
  }

  public static void performAndExecuteQuery() {
    HydraVector poolnames = TestConfig.tab().vecAt(PoolPrms.names, null);
    for (int i = 0; i < poolnames.size(); i++) {
      String poolDescriptName = (String)(poolnames.get(i));
      String poolName = PoolHelper.getPoolDescription(poolDescriptName)
          .getName();
      Pool pool = PoolHelper.getPool(poolName);
      QueryService qSvc = pool.getQueryService();
      HydraVector hquery = TestConfig.tab().vecAt(QueryPrms.queryStrings, null);
      Log.getLogWriter().fine(
          "The hquery Vector is written like this :::::" + hquery);
      if (hquery == null)
        throw new TestException("No Queries specified ");
      String queryStrings[] = new String[hquery.size()];
      hquery.copyInto(queryStrings);
      Query[] queries = new Query[queryStrings.length];
      int j;
      for (j = 0; j < queries.length; j++) {
        Log.getLogWriter().fine(
            "Query Strings that are performed :" + queryStrings[j]);
        queries[j] = qSvc.newQuery(queryStrings[j]);
        SelectResults result;
        try {
          result = (SelectResults)queries[j].execute();
          Iterator iter = result.iterator();
          Log.getLogWriter().fine(
              "Count ===> " + result.size() + " and result is ===> "
                  + result.toString() + " and also ====> " + result.isEmpty());
        }
        catch (Exception e1) {
          Log.getLogWriter().error("Exception is :", e1);
        }
      }
    }
  }
}
