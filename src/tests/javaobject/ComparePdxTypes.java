/*=========================================================================
 * Copyright (c) 2010-2014 Pivotal Software, Inc. All Rights Reserved.
 * This product is protected by U.S. and international copyright
 * and intellectual property laws. Pivotal products are covered by
 * one or more patents listed at http://www.pivotal.io/patents.
 *=========================================================================
 */
package javaobject;
import java.util.HashMap;
import java.util.Properties;

import PdxTests.PdxTypes1;
import PdxTests.PdxTypes10;
import PdxTests.PdxTypes2;
import PdxTests.PdxTypes3;
import PdxTests.PdxTypes4;
import PdxTests.PdxTypes5;
import PdxTests.PdxTypes6;
import PdxTests.PdxTypes7;
import PdxTests.PdxTypes8;
import PdxTests.PdxTypes9;

import org.apache.geode.cache.Cache;
import org.apache.geode.cache.CacheFactory;
import org.apache.geode.cache.Declarable;
import org.apache.geode.cache.Region;
import org.apache.geode.cache.execute.FunctionAdapter;
import org.apache.geode.cache.execute.FunctionContext;
import org.apache.geode.cache.execute.RegionFunctionContext;
import org.apache.geode.internal.cache.GemFireCacheImpl;
import org.apache.geode.pdx.internal.EnumInfo;
import org.apache.geode.pdx.internal.PdxField;
import org.apache.geode.pdx.internal.PdxType;
import org.apache.geode.pdx.internal.TypeRegistry;


public class ComparePdxTypes extends FunctionAdapter implements Declarable{

  static HashMap javaClassnameVsPdxTypes = new HashMap();
  @Override
  public void execute(FunctionContext context) {
    RegionFunctionContext rfc = (RegionFunctionContext)context;
    String op = (String)rfc.getArguments();
    try
    {
      if(op.equals("saveAllJavaPdxTypes")) {
        putAllPdxTypes();
        saveAllJavaPdxTypes();
        context.getResultSender().lastResult(true);
      } else if(op.equals("compareDotNETPdxTypes")){
        compareDotNETPdxTypes();
        context.getResultSender().lastResult(true);
      } else {
        context.getResultSender().lastResult(false);
      }
    }catch(Exception ex) {
      CacheFactory.getAnyInstance().getLoggerI18n().info(ex);
      context.getResultSender().sendException(ex);
    }
  }

  @Override
  public String getId() {
    // TODO Auto-generated method stub
    return "ComparePdxTypes";
  }

  @Override
  public void init(Properties props) {
    
  }
  
  private void putAllPdxTypes() {
    Region r = CacheFactory.getAnyInstance().getRegion("DistRegionAck");
    PdxTypes1 p1 = new PdxTypes1();
    r.put(PdxTypes1.class.getName(), p1);
    
    PdxTypes2 p2 = new PdxTypes2();
    r.put(PdxTypes2.class.getName(), p2);
    
    PdxTypes3 p3 = new PdxTypes3();
    r.put(p3.getClass().getName(), p3);
    
    PdxTypes4 p4 = new PdxTypes4();
    r.put(p4.getClass().getName(), p4);
    
    PdxTypes5 p5 = new PdxTypes5();
    r.put(p5.getClass().getName(), p5);
    
    PdxTypes6 p6 = new PdxTypes6();
    r.put(p6.getClass().getName(), p6);
    
    PdxTypes7 p7 = new PdxTypes7();
    r.put(p7.getClass().getName(), p7);
    
    PdxTypes8 p8 = new PdxTypes8();
    r.put(p8.getClass().getName(), p8);
    
    PdxTypes9 p9 = new PdxTypes9();
    r.put(p9.getClass().getName(), p9);
    
    PdxTypes10 p10 = new PdxTypes10();
    r.put(p10.getClass().getName(), p10);
  }
  
  private void getAllPdxTypes() {
    Region r = CacheFactory.getAnyInstance().getRegion("DistRegionAck");
    PdxTypes1 p1 = new PdxTypes1();
    r.get(PdxTypes1.class.getName());
    
    PdxTypes2 p2 = new PdxTypes2();
    r.get(PdxTypes2.class.getName(), p2);
    
    PdxTypes3 p3 = new PdxTypes3();
    r.get(p3.getClass().getName(), p3);
    
    PdxTypes4 p4 = new PdxTypes4();
    r.put(p4.getClass().getName(), p4);
    
    PdxTypes5 p5 = new PdxTypes5();
    r.put(p5.getClass().getName(), p5);
    
    PdxTypes6 p6 = new PdxTypes6();
    r.put(p6.getClass().getName(), p6);
    
    PdxTypes7 p7 = new PdxTypes7();
    r.put(p7.getClass().getName(), p7);
    
    PdxTypes8 p8 = new PdxTypes8();
    r.put(p8.getClass().getName(), p8);
    
    PdxTypes9 p9 = new PdxTypes9();
    r.put(p9.getClass().getName(), p9);
    
    PdxTypes10 p10 = new PdxTypes10();
    r.put(p10.getClass().getName(), p10);
  }

  private void saveAllJavaPdxTypes() {
    Region r = CacheFactory.getAnyInstance().getRegion("PdxTypes");
    
    for(Object k : r.keySet()) {
      Object obj =r.get(k);
      
      if(obj instanceof EnumInfo) {
        EnumInfo ei = (EnumInfo)obj;
        javaClassnameVsPdxTypes.put(ei.toString().substring(5), ei); 
      }else {
        PdxType pt = (PdxType)obj;
        javaClassnameVsPdxTypes.put(pt.getClassName(), pt);
      }
      
    }
    
    if(javaClassnameVsPdxTypes.size() != 11) {
      throw new IllegalStateException("Java pdx types size are not equal to 10; and it is = " + javaClassnameVsPdxTypes.size());
    }
    
    //now clear all java pdx types
   /* for(Object k : r.keySet()) {
      r.destroy(k);
    }*/
    
    GemFireCacheImpl cache = (GemFireCacheImpl) CacheFactory.getAnyInstance();
    TypeRegistry registry = cache.getPdxRegistry();
    
    //this will clear pdx registry
    registry.testClearTypeRegistry();
    
    if(r.keySet().size() != 0) {
      throw new IllegalStateException("Java pdx types still has PdxTypes enteries " + r.keySet().size());
    }
  }
  
  private void compareDotNETPdxTypes() {
   
    Region r = CacheFactory.getAnyInstance().getRegion("PdxTypes");
    
    if(r.keySet().size() != 11) {
      throw new IllegalStateException("compareDotNETPdxTypes Java pdx types should have PdxTypes enteries " + r.keySet().size());
    }
    for(Object k : r.keySet()) {
      Object obj = r.get(k);
      if(obj instanceof EnumInfo) {
        EnumInfo ei = (EnumInfo)obj;
        EnumInfo jei =(EnumInfo) javaClassnameVsPdxTypes.get(ei.toString().substring(5));
        if(!ei.equals(jei)) {
          throw new IllegalStateException("Pdxfield not matched DotNet:" + ei + " Java:" + jei);
        }
        continue; 
      }
      PdxType dotNetPt = (PdxType)obj;
      PdxType javaPt = (PdxType)javaClassnameVsPdxTypes.get(dotNetPt.getClassName());
      
      if(dotNetPt.getFieldCount() != javaPt.getFieldCount()) {
        
      }
      
      for(PdxField dnPf : dotNetPt.getFields()) {
        boolean matched = false;
        for(PdxField jPf : javaPt.getFields()) {
          if(dnPf.equals(jPf)) {
            if(dnPf.getRelativeOffset() == jPf.getRelativeOffset()) {
              if(dnPf.getVlfOffsetIndex() == jPf.getVlfOffsetIndex()) {
                matched = true;
              }
            }
            if(!matched) {
              throw new IllegalStateException("Pdxfield not matched DotNet:" + dnPf + " Java:" + jPf);
            }
            break;
          }
        }
        if(!matched) {
          throw new IllegalStateException("Pdxfield not matched DotNet:" + dnPf );
        }
      }
    }
  }
}
