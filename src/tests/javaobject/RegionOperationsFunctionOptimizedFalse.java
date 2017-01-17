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
import java.util.HashSet;
import java.util.Random;
import PdxTests.PdxVersioned;
import org.apache.geode.cache.Declarable;
import org.apache.geode.cache.Region;
import org.apache.geode.cache.EntryNotFoundException;
import org.apache.geode.cache.execute.FunctionAdapter;
import org.apache.geode.cache.execute.FunctionContext;
import org.apache.geode.cache.execute.RegionFunctionContext;
import org.apache.geode.cache.partition.PartitionRegionHelper;
import org.apache.geode.internal.cache.PartitionedRegion;
import org.apache.geode.internal.cache.GemFireCacheImpl;
import org.apache.geode.pdx.PdxInstance;
import org.apache.geode.pdx.WritablePdxInstance;
import org.apache.geode.internal.cache.execute.PartitionedRegionFunctionResultSender;

public class RegionOperationsFunctionOptimizedFalse extends FunctionAdapter implements
    Declarable {

  public void execute(FunctionContext context) {
	  boolean ispdxObj = false;
    	 //GemFireCacheImpl.getInstance().getLogger().info("rjk:in RegionOperationsFunction ----221");
  		    Vector arguments = (Vector)context.getArguments();
  		  String operation = (String)arguments.get(0);
		  int i = 0;
		  if(arguments.size() > 2 && (arguments.get(1) instanceof Boolean )){
			//String pdxObj
			ispdxObj = (Boolean)arguments.get(1);
	   	    //ispdxObj = pdxObj.equalsIgnoreCase("pdxobject");
	   	    i=2;
		  }else
		  {
		    i=1;
		  }
    final boolean isRegionContext = context instanceof RegionFunctionContext;
    boolean isPartitionedRegionContext = false;
    RegionFunctionContext regionContext = null;
    if (isRegionContext) {
       regionContext = (RegionFunctionContext)context;
       isPartitionedRegionContext = 
         PartitionRegionHelper.isPartitionedRegion(regionContext.getDataSet());
    }

    if (isPartitionedRegionContext) {
      PartitionedRegionFunctionResultSender rs = (PartitionedRegionFunctionResultSender)regionContext.getResultSender();	
    
      if (operation.equalsIgnoreCase("addKey")) {
        Set keySet = regionContext.getFilter();
        PartitionedRegion pr = (PartitionedRegion)regionContext.getDataSet();

        Iterator iterator = keySet.iterator();
        Object value = null;
        while (iterator.hasNext()) {
          Object key = iterator.next();
          value = createObject(key,ispdxObj);
          pr.put(key, value);
        }
        context.getResultSender().lastResult(rs.isLocallyExecuted());
      }
      else if (operation.equalsIgnoreCase("invalidate")) {
        Set keySet = regionContext.getFilter();
        PartitionedRegion pr = (PartitionedRegion)regionContext.getDataSet();

        Iterator iterator = keySet.iterator();
        while (iterator.hasNext()) {
          Object key = iterator.next();
          pr.invalidate(key);
        }
        context.getResultSender().lastResult( rs.isLocallyExecuted());
      }
      else if (operation.equalsIgnoreCase("destroy")) {

        Set keySet = regionContext.getFilter();
        PartitionedRegion pr = (PartitionedRegion)regionContext.getDataSet();

        Iterator iterator = keySet.iterator();
        while (iterator.hasNext()) {
          Object key = iterator.next();
          try {
            pr.destroy(key);
          } catch (EntryNotFoundException ignore) { /*ignored*/ }
        }
        context.getResultSender().lastResult( rs.isLocallyExecuted());

      }
      else if (operation.equalsIgnoreCase("putAll")) {
        Set keySet = regionContext.getFilter();
        PartitionedRegion pr = (PartitionedRegion)regionContext.getDataSet();

        Iterator iterator = keySet.iterator();
        HashMap map = new HashMap();
        Object value = null;
        while (iterator.hasNext()) {
          Object key = iterator.next();
          value = createObject(key,ispdxObj);
          map.put(key, value);
        }
        pr.putAll(map);
        context.getResultSender().lastResult(rs.isLocallyExecuted());
      }
      else if (operation.equalsIgnoreCase("update")) {
        Set keySet = regionContext.getFilter();
        PartitionedRegion pr = (PartitionedRegion)regionContext.getDataSet();

        Iterator iterator = keySet.iterator();
        Object value = null;
        while (iterator.hasNext()) {
          Object key = iterator.next();
          Object newValue = createObject(key, ispdxObj);
          if(newValue instanceof PdxInstance) {
              PdxInstance pdxInst = (PdxInstance)newValue;
              WritablePdxInstance writable = pdxInst.createWriter();
              writable.setField("m_string", "updated_" + key);
              newValue = writable;
          }else
          {
        	  newValue = "updated_" + key;
          }
          value = pr.put(key, newValue);
        }
        context.getResultSender().lastResult(rs.isLocallyExecuted());
      }
      else if (operation.equalsIgnoreCase("get")) {
        Set keySet = regionContext.getFilter();
        PartitionedRegion pr = (PartitionedRegion)regionContext.getDataSet();
        
        Iterator iterator = keySet.iterator();
        Object existingValue = null;
        while (iterator.hasNext()) {
            
          Object key = iterator.next();
          existingValue = pr.get(key);
          if(existingValue != null && existingValue instanceof PdxInstance){
        	  PdxInstance pdxInst = (PdxInstance)existingValue;
              Object val = pdxInst.getField("m_string");
          }
          
        }
        context.getResultSender().lastResult(rs.isLocallyExecuted());
        
      }
      else if (operation.equalsIgnoreCase("localinvalidate")) {
        Set keySet = regionContext.getFilter();
        PartitionedRegion pr = (PartitionedRegion)regionContext.getDataSet();

        Iterator iterator = keySet.iterator();
        while (iterator.hasNext()) {
          Object key = iterator.next();
          pr.localInvalidate(key);
        }
        context.getResultSender().lastResult( rs.isLocallyExecuted());
      }
      else if (operation.equalsIgnoreCase("localdestroy")) {
        Set keySet = regionContext.getFilter();
        PartitionedRegion pr = (PartitionedRegion)regionContext.getDataSet();

        Iterator iterator = keySet.iterator();
        while (iterator.hasNext()) {
          Object key = iterator.next();
          try {
            pr.localDestroy(key);
          } catch (EntryNotFoundException ignore) { /*ignored*/ }
        }
        context.getResultSender().lastResult( rs.isLocallyExecuted());
      }
      else
        context.getResultSender().lastResult( rs.isLocallyExecuted());
    }
  }
  
  

  public String getId() {
    //return this.getClass().getName();
    return "RegionOperationsFunctionOptimizedFalse";
  }

  public boolean optimizeForWrite() {
    return false;
  }

  public void init(Properties props) {
  }
  protected Object createObject (Object key, boolean ispdx) {
	   if (ispdx) {
		   Random foo = new Random();
	       return new PdxTests.PdxVersioned(key.toString());
	    }
	    else {
	      return key;
	   }
 }
}
