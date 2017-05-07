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
import java.util.HashSet;
import java.util.Random;
import PdxTests.PdxVersioned;

import com.gemstone.gemfire.cache.Declarable;
import com.gemstone.gemfire.cache.Region;
import com.gemstone.gemfire.cache.EntryNotFoundException;
import com.gemstone.gemfire.cache.execute.FunctionAdapter;
import com.gemstone.gemfire.cache.execute.FunctionContext;
import com.gemstone.gemfire.cache.execute.RegionFunctionContext;
import com.gemstone.gemfire.cache.partition.PartitionRegionHelper;
import com.gemstone.gemfire.internal.cache.PartitionedRegion;
import com.gemstone.gemfire.internal.cache.GemFireCacheImpl;
import com.gemstone.gemfire.pdx.PdxInstance;
import com.gemstone.gemfire.pdx.WritablePdxInstance;
import com.gemstone.gemfire.pdx.internal.EnumInfo;
import com.gemstone.gemfire.pdx.internal.PdxField;
import com.gemstone.gemfire.pdx.internal.PdxType;
import com.gemstone.gemfire.pdx.internal.TypeRegistry;

public class RegionOperationsFunctionPdx extends FunctionAdapter implements
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
    /*)	ArrayList arguments = (ArrayList)(context.getArguments());
    	String operation = (String)arguments.get(0);
		  int i = 0;
		  if(arguments.size() > 2){
			String pdxObj = (String)arguments.get(1);
	   	    ispdxObj = pdxObj.equalsIgnoreCase("pdxobject");
	   	    i=2;
		  }else
		  {
		    i=1;
		  }*/
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
        context.getResultSender().lastResult(new Boolean(true));
      }
      else if (operation.equalsIgnoreCase("invalidate")) {
        Set keySet = regionContext.getFilter();
        PartitionedRegion pr = (PartitionedRegion)regionContext.getDataSet();

        Iterator iterator = keySet.iterator();
        while (iterator.hasNext()) {
          Object key = iterator.next();
          pr.invalidate(key);
        }
        context.getResultSender().lastResult( Boolean.TRUE);
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
        context.getResultSender().lastResult( Boolean.TRUE);

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
        context.getResultSender().lastResult(new Boolean(true));
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
        context.getResultSender().lastResult(new Boolean(true));
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
        context.getResultSender().lastResult(new Boolean(true));
        
      }
      else if (operation.equalsIgnoreCase("localinvalidate")) {
        Set keySet = regionContext.getFilter();
        PartitionedRegion pr = (PartitionedRegion)regionContext.getDataSet();

        Iterator iterator = keySet.iterator();
        while (iterator.hasNext()) {
          Object key = iterator.next();
          pr.localInvalidate(key);
        }
        context.getResultSender().lastResult( Boolean.TRUE);
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
        context.getResultSender().lastResult( Boolean.TRUE);
      }
      else
        context.getResultSender().lastResult( Boolean.FALSE);
    }
    else if (context instanceof RegionFunctionContext) {// for replicated region
      if (operation.equalsIgnoreCase("addKey")) {
        Region region = regionContext.getDataSet();
        Object value = null;
        for (int arg = i; arg < arguments.size(); arg++) {
            Object key = arguments.get(arg);
            value = createObject(key,ispdxObj);
          region.put(key, value);
        }
        context.getResultSender().lastResult(new Boolean(true));
      }
      else if (operation.equalsIgnoreCase("invalidate")) {
        Region region = regionContext.getDataSet();
        for (int arg = i; arg < arguments.size(); arg++) {
            Object key = arguments.get(arg);
          region.invalidate(key);
        }
        context.getResultSender().lastResult( Boolean.TRUE);
      }
      else if (operation.equalsIgnoreCase("destroy")) {

        Region region = regionContext.getDataSet();
        for (int arg = i; arg < arguments.size(); arg++) {
            Object key = arguments.get(arg);
          try {
            region.destroy(key);
          } catch (EntryNotFoundException ignore) { /*ignored*/ }
        }
        context.getResultSender().lastResult( Boolean.TRUE);

      }
      else if (operation.equalsIgnoreCase("putAll")) {
        Region region = regionContext.getDataSet();
        HashMap map = new HashMap();
        Object value = null;
        for (int arg = i; arg < arguments.size(); arg++) {
            Object key = arguments.get(arg);
           value = createObject(key,ispdxObj);
          map.put(key, value);
        }
        region.putAll(map);
        context.getResultSender().lastResult(new Boolean(true));
      }
      else if (operation.equalsIgnoreCase("update")) {
        Region region = regionContext.getDataSet();
        Object value = null;
        for (int arg = i; arg < arguments.size(); arg++) {
            Object key = arguments.get(arg);
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
          value = region.put(key, newValue);
        }
        context.getResultSender().lastResult(new Boolean(true));

      }
      else if (operation.equalsIgnoreCase("get")) {
        Region region = regionContext.getDataSet();
        Object existingValue = null;
        for (int arg = i; arg < arguments.size(); arg++) {
          Object key = arguments.get(arg);
          
          existingValue = region.get(key);
          if(existingValue != null && existingValue instanceof PdxInstance){
        	  PdxInstance pdxInst = (PdxInstance)existingValue;
              Object val = pdxInst.getField("m_string");
            }
          
        }
        context.getResultSender().lastResult(new Boolean(true));
      }
      else if (operation.equalsIgnoreCase("localinvalidate")) {
        Region region = regionContext.getDataSet();
        for (int arg = i; arg < arguments.size(); arg++) {
            Object key = arguments.get(arg);
          region.localInvalidate(key);
        }
        context.getResultSender().lastResult( Boolean.TRUE);
      }
      else if (operation.equalsIgnoreCase("localdestroy")) {
        Region region = regionContext.getDataSet();
        for (int arg = i; arg < arguments.size(); arg++) {
            Object key = arguments.get(arg);
          try {
            region.localDestroy(key);
          } catch (EntryNotFoundException ignore) { /*ignored*/ }
        }
        context.getResultSender().lastResult(Boolean.TRUE);
      }
      else{
        context.getResultSender().lastResult(Boolean.FALSE);
      }
    //}
    }    
  }
  
  

  public String getId() {
    //return this.getClass().getName();
    return "RegionOperationsFunctionPdx";
  }

  public boolean optimizeForWrite() {
    return true;
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
