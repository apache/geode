/*=========================================================================
 * Copyright (c) 2010-2014 Pivotal Software, Inc. All Rights Reserved.
 * This product is protected by U.S. and international copyright
 * and intellectual property laws. Pivotal products are covered by
 * one or more patents listed at http://www.pivotal.io/patents.
 *=========================================================================
 */
/*
 * Utils.java
 *
 * Created on March 11, 2005, 12:34 PM
 */

package com.gemstone.gemfire.cache.query;

import java.util.Collection;
import java.util.Iterator;

/**
 *
 * @author vaibhav
 */
public class Utils {
  public static String printResult(Object r){
    StringBuffer sb = new StringBuffer();
    sb.append("Search Results\n");
    if(r == null){
      sb.append("Result is NULL");
      return sb.toString();
    } else if(r == QueryService.UNDEFINED){
      sb.append("Result is UNDEFINED");
      return sb.toString();
    }
    sb.append("Type = "+r.getClass().getName()).append("\n");
    if(r instanceof Collection){
      sb.append("Size = "+((Collection)r).size()+"\n");
      int cnt = 1 ;
      Iterator iter = ((Collection)r).iterator();
      while(iter.hasNext()){
        Object value = iter.next();
        sb.append((cnt++)+" type = "+value.getClass().getName()).append("\n");
        sb.append("  "+value+"\n");
      }
    } else
      sb.append(r);
    return sb.toString();
  }
}
