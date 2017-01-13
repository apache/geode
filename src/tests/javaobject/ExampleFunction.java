/*=========================================================================
 * Copyright (c) 2010-2014 Pivotal Software, Inc. All Rights Reserved.
 * This product is protected by U.S. and international copyright
 * and intellectual property laws. Pivotal products are covered by
 * one or more patents listed at http://www.pivotal.io/patents.
 *=========================================================================
 */
package javaobject;


import java.util.*;
import java.io.*;
import org.apache.geode.*; // for DataSerializable
import org.apache.geode.cache.Declarable;

import org.apache.geode.cache.Region;
import org.apache.geode.cache.execute.FunctionAdapter;
import org.apache.geode.cache.execute.FunctionContext;
import org.apache.geode.cache.execute.ResultSender;
import org.apache.geode.cache.execute.RegionFunctionContext;
import org.apache.geode.cache.partition.PartitionRegionHelper;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.Properties;
import java.util.Set;

public class ExampleFunction extends FunctionAdapter implements Declarable {
  public static final String EXAMPLE_FUNCTION9 = "ExampleFunction9";
  public static final String EXAMPLE_FUNCTION_EXCEPTION = "ExampleFunctionException";
  public static final String EXAMPLE_FUNCTION_RESULT_SENDER = "ExampleFunctionResultSender";
  private static final String ID = "id";
  private static final String HAVE_RESULTS = "haveResults";
  private final Properties props;

  // Default constructor for Declarable purposes
  public ExampleFunction() {
    super();
    this.props = new Properties();
  }

  public ExampleFunction(boolean haveResults, String id) {
    this.props = new Properties();
    this.props.setProperty(HAVE_RESULTS, Boolean.toString(haveResults));
    this.props.setProperty(ID, id);
  }
  
  /**
   * Application execution implementation
   * 
   */
  public void execute(FunctionContext context) {
    String id = this.props.getProperty(ID); 

    if (id.equals(EXAMPLE_FUNCTION9)) {
      execute9(context);
    }
    else if (id.equals(EXAMPLE_FUNCTION_EXCEPTION)) {
      executeException(context);
    }
    else if (id.equals(EXAMPLE_FUNCTION_RESULT_SENDER)) {
      executeResultSender(context);
    }
    else {
      context.getResultSender().lastResult(Boolean.FALSE);
    }
  }  
  
  public void execute9(FunctionContext context) {
    if (context instanceof RegionFunctionContext) {
      RegionFunctionContext rfContext = (RegionFunctionContext)context;
      rfContext.getDataSet().getCache().getLogger().info(
          "Executing function :  ExampleFunction9.execute " + rfContext);
      if (rfContext.getArguments() instanceof Boolean) {
        rfContext.getResultSender().lastResult(rfContext.getArguments());
      }
      else if (rfContext.getArguments() instanceof String) {
        String key = (String)rfContext.getArguments();
        if(key.equals("TestingTimeOut")){  
          try{
            synchronized (this) {
              this.wait(2000);
            }
          }catch(InterruptedException e){
            rfContext.getDataSet().getCache().getLogger().warning(
                "Got Exception : Thread Interrupted" + e);
          }
        }
        if(context instanceof RegionFunctionContext){
          RegionFunctionContext prContext = (RegionFunctionContext)context;
          if (PartitionRegionHelper.isPartitionedRegion(prContext.getDataSet())) {
            rfContext.getResultSender().lastResult(
                (Serializable)PartitionRegionHelper.getLocalDataForContext(
                    prContext).get(key));
          }
        }
      }
      else if (rfContext.getArguments() instanceof Set) {
        Set origKeys = (Set)rfContext.getArguments();
        ArrayList vals = new ArrayList();
        for (Iterator i = origKeys.iterator(); i.hasNext();) {
          Object val = null;
          if(context instanceof RegionFunctionContext){
            RegionFunctionContext prContext = (RegionFunctionContext)context;
            val = PartitionRegionHelper.getLocalDataForContext(prContext).get(i.next());
          }else{
            val = rfContext.getDataSet().get(i.next());
          }  
          
          if(val != null)
            rfContext.getResultSender().lastResult((Serializable)val);
          
          if (val != null) {
            vals.add(val);
          }
        }
      }
      else if (rfContext.getArguments() instanceof HashMap) {
        HashMap putData = (HashMap)rfContext.getArguments();
        for (Iterator i = putData.entrySet().iterator(); i.hasNext();) {
          Map.Entry me = (Map.Entry)i.next();
          rfContext.getDataSet().put(me.getKey(), me.getValue());
        }
        rfContext.getResultSender().lastResult(Boolean.TRUE);
      }
      else {
        rfContext.getResultSender().lastResult(Boolean.TRUE);
      }
    }else{
      context.getResultSender().lastResult(Boolean.TRUE);
    }
  }
  
  private Serializable executeException(FunctionContext context) {
    if (context.getArguments() instanceof Boolean) {
      throw new NullPointerException("I have been thrown from ExampleFunction");
    }
    else if (context.getArguments() instanceof String) {
      String key = (String)context.getArguments();
      return key;
    }
    else if (context.getArguments() instanceof Set) {
      Set origKeys = (Set)context.getArguments();
      ArrayList vals = new ArrayList();
      for (Iterator i = origKeys.iterator(); i.hasNext();) {
        Object val = i.next();
        if (val != null) {
          vals.add(val);
        }
      }
      return vals;
    }
    else {
      return Boolean.FALSE;
    }
  }
   
  private Serializable executeResultSender(FunctionContext context) {
    ResultSender resultSender = context.getResultSender();
    if (context instanceof RegionFunctionContext) {
      RegionFunctionContext rfContext = (RegionFunctionContext)context;
      rfContext.getDataSet().getCache().getLogger().info(
          "Executing function :  ExampleFunctionexecuteResultSender.execute " + rfContext);
      if (rfContext.getArguments() instanceof Boolean) {
        if (this.hasResult()) {
          resultSender.lastResult(rfContext.getArguments());
        }
      }
      else if (rfContext.getArguments() instanceof Set) {
        Set origKeys = (Set)rfContext.getArguments();
        Object[] objectArray = origKeys.toArray();
        int size = objectArray.length;
        int i = 0;
        for( ;i < (size-1) ;i++){
          Object val = PartitionRegionHelper.getLocalDataForContext(rfContext).get(objectArray[i]);
          if (val != null) {
            resultSender.sendResult((Serializable)val);
          }
        }
        resultSender.lastResult((Serializable)objectArray[i]);
      }
      else {
        resultSender.lastResult(Boolean.FALSE);
      }
    }else{
      resultSender.lastResult(Boolean.FALSE);
    }
     return null;
  }
  
  /**
   * Get the function identifier, used by clients to invoke this function
   * 
   * @return an object identifying this function
   */
  public String getId() {
    return this.props.getProperty(ID);
  }

  public boolean equals(Object obj) {
    if (this == obj)
      return true;
    if (!(obj instanceof ExampleFunction)) {
      return false;      
    }
    ExampleFunction function = (ExampleFunction)obj;
    if (!this.props.equals(function.getConfig())) {
      return false;
    }
    return true;
  }

  public boolean hasResult() {
    return Boolean.valueOf(this.props.getProperty(HAVE_RESULTS)).booleanValue();
  }

  public Properties getConfig() {
    return this.props;
  }

  /* (non-Javadoc)
   * @see org.apache.geode.cache.Declarable#init(java.util.Properties)
   */
  public void init(Properties props) {
    this.props.putAll(props);
  }
}
