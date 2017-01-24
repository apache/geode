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
package javaobject;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Properties;
import java.util.Set;
import java.util.Vector;
import org.apache.geode.cache.Declarable;
import org.apache.geode.cache.Region;
import org.apache.geode.cache.EntryNotFoundException;
import org.apache.geode.cache.execute.FunctionAdapter;
import org.apache.geode.cache.execute.FunctionContext;
import org.apache.geode.cache.execute.RegionFunctionContext;
import org.apache.geode.cache.partition.PartitionRegionHelper;
import org.apache.geode.internal.cache.PartitionedRegion;

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
