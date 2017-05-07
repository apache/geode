/*=========================================================================
 * Copyright (c) 2010-2014 Pivotal Software, Inc. All Rights Reserved.
 * This product is protected by U.S. and international copyright
 * and intellectual property laws. Pivotal products are covered by
 * one or more patents listed at http://www.pivotal.io/patents.
 *=========================================================================
 */
package javaobject;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Properties;
import java.util.Set;
import java.util.Vector;
import com.gemstone.gemfire.cache.Declarable;
import com.gemstone.gemfire.cache.Region;
import com.gemstone.gemfire.cache.EntryNotFoundException;
import com.gemstone.gemfire.cache.execute.FunctionAdapter;
import com.gemstone.gemfire.cache.execute.FunctionContext;
import com.gemstone.gemfire.cache.execute.RegionFunctionContext;
import com.gemstone.gemfire.cache.partition.PartitionRegionHelper;
import com.gemstone.gemfire.internal.cache.PartitionedRegion;

public class RegionOperationsWithOutResultFunction extends FunctionAdapter implements
    Declarable {

  public void execute(FunctionContext context) {
    final boolean isRegionContext = context instanceof RegionFunctionContext;
    boolean isPartitionedRegionContext = false;
    RegionFunctionContext regionContext = null;
    if (isRegionContext) {
       regionContext = (RegionFunctionContext)context;
       isPartitionedRegionContext = 
         PartitionRegionHelper.isPartitionedRegion(regionContext.getDataSet());
    }

    if (isPartitionedRegionContext) {
      String argument = regionContext.getArguments().toString();
      if (argument.equalsIgnoreCase("addKey")) {
        Set keySet = regionContext.getFilter();
        PartitionedRegion pr = (PartitionedRegion)regionContext.getDataSet();
        Iterator iterator = keySet.iterator();
        while (iterator.hasNext()) {
          Object key = iterator.next();
          pr.put(key, key);
        }
      }
      else if (argument.equalsIgnoreCase("invalidate")) {
        Set keySet = regionContext.getFilter();
        PartitionedRegion pr = (PartitionedRegion)regionContext.getDataSet();

        Iterator iterator = keySet.iterator();
        while (iterator.hasNext()) {
          Object key = iterator.next();
          pr.invalidate(key);
        }
      }
      else if (argument.equalsIgnoreCase("destroy")) {
        Set keySet = regionContext.getFilter();
        PartitionedRegion pr = (PartitionedRegion)regionContext.getDataSet();
        Iterator iterator = keySet.iterator();
        while (iterator.hasNext()) {
          Object key = iterator.next();
          try {
            pr.destroy(key);
          } catch (EntryNotFoundException ignore) { /*ignored*/ }
        }

      }
      else if (argument.equalsIgnoreCase("update")) {
        Set keySet = regionContext.getFilter();
        PartitionedRegion pr = (PartitionedRegion)regionContext.getDataSet();

        Iterator iterator = keySet.iterator();
        while (iterator.hasNext()) {
          Object key = iterator.next();
          Object value = "update_" + key;
          pr.put(key, value);
        }

      }
      else if (argument.equalsIgnoreCase("get")) {
        Set keySet = regionContext.getFilter();
        Region region = PartitionRegionHelper.getLocalDataForContext(regionContext);

        Iterator iterator = keySet.iterator();
        while (iterator.hasNext()) {
          Object key = iterator.next();
          Object existingValue = null;
          existingValue = region.get(key);
        }
      }
      else if (argument.equalsIgnoreCase("localinvalidate")) {
        Set keySet = regionContext.getFilter();
        PartitionedRegion pr = (PartitionedRegion)regionContext.getDataSet();

        Iterator iterator = keySet.iterator();
        while (iterator.hasNext()) {
          Object key = iterator.next();
          pr.localInvalidate(key);
        }
      }
      else if (argument.equalsIgnoreCase("localdestroy")) {
        Set keySet = regionContext.getFilter();
        PartitionedRegion pr = (PartitionedRegion)regionContext.getDataSet();

        Iterator iterator = keySet.iterator();
        while (iterator.hasNext()) {
          Object key = iterator.next();
          try {
            pr.localDestroy(key);
          } catch (EntryNotFoundException ignore) { /*ignored*/ }
        }
      }
      else if (regionContext.getArguments() instanceof HashMap) {
        HashMap map = (HashMap)regionContext.getArguments();
        PartitionedRegion pr = (PartitionedRegion)regionContext.getDataSet();
        pr.putAll(map);

      }
    }
    else if (context instanceof RegionFunctionContext) {
      if( regionContext.getArguments() instanceof ArrayList){
      ArrayList argumentList = (ArrayList) regionContext.getArguments();
      String operation = (String)argumentList.remove(0);
      if (operation.equalsIgnoreCase("addKey")) {
        Region region = regionContext.getDataSet();

        Iterator iterator = argumentList.iterator();
        while (iterator.hasNext()) {
          Object key = iterator.next();
          region.put(key, key);
        }
      }
      else if (operation.equalsIgnoreCase("invalidate")) {
        Region region = regionContext.getDataSet();

        Iterator iterator = argumentList.iterator();
        while (iterator.hasNext()) {
          Object key = iterator.next();
          region.invalidate(key);
        }
      }
      else if (operation.equalsIgnoreCase("destroy")) {

        Region region = regionContext.getDataSet();

        Iterator iterator = argumentList.iterator();
        while (iterator.hasNext()) {
          Object key = iterator.next();
          try {
            region.destroy(key);
          } catch (EntryNotFoundException ignore) { /*ignored*/ }
        }

      }
      else if (operation.equalsIgnoreCase("update")) {
        Region region = regionContext.getDataSet();

        Iterator iterator = argumentList.iterator();
        while (iterator.hasNext()) {
          Object key = iterator.next();
          Object value = "update_" + key;
          region.put(key, value);
        }

      }
      else if (operation.equalsIgnoreCase("get")) {
        Region region = regionContext.getDataSet();

        Iterator iterator = argumentList.iterator();
        while (iterator.hasNext()) {
          Object key = iterator.next();
          Object existingValue = null;
          existingValue = region.get(key);
        }
      }
      else if (operation.equalsIgnoreCase("localinvalidate")) {
        Region region = regionContext.getDataSet();

        Iterator iterator = argumentList.iterator();
        while (iterator.hasNext()) {
          Object key = iterator.next();
          region.localInvalidate(key);
        }
      }
      else if (operation.equalsIgnoreCase("localdestroy")) {
        Region region = regionContext.getDataSet();

        Iterator iterator = argumentList.iterator();
        while (iterator.hasNext()) {
          Object key = iterator.next();
          try {
            region.localDestroy(key);
          } catch (EntryNotFoundException ignore) { /*ignored*/ }
        }
      }
      }
      else if (regionContext.getArguments() instanceof HashMap) {
        HashMap map = (HashMap)regionContext.getArguments();
        Region region = regionContext.getDataSet();
        region.putAll(map);

      }
    }    
  }
  
  

  public String getId() {
    return "RegionOperationsWithOutResultFunction";
  }
  
  public boolean optimizeForWrite() {
    return true;
  }
  public void init(Properties props) {
  }
  
  public boolean hasResult() {
	return false;
  }

  public boolean isHA() {
	return false;
  }

}
