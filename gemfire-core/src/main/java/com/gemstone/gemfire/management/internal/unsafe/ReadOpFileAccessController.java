/*=========================================================================
 * Copyright (c) 2010-2014 Pivotal Software, Inc. All Rights Reserved.
 * This product is protected by U.S. and international copyright
 * and intellectual property laws. Pivotal products are covered by
 * one or more patents listed at http://www.pivotal.io/patents.
 *=========================================================================
 */
package com.gemstone.gemfire.management.internal.unsafe;

import java.io.IOException;

import javax.management.InstanceNotFoundException;
import javax.management.MBeanException;
import javax.management.ObjectName;
import javax.management.ReflectionException;

import com.sun.jmx.remote.security.MBeanServerFileAccessController;

/**
 * This class extends existing properties file based accessController in order
 * to implement read only operations. In JMX all operations assume to be modifying
 * the system hence requires read-write access. But in GemFire some of the attributes
 * are implemented as operations in order to reduce federation cost due to large
 * payloads of those attribute. These attributes are exposed as operations but require
 * readOnly access. This class filter those methods for read access.
 * 
 * @author tushark
 */


public class ReadOpFileAccessController extends MBeanServerFileAccessController{
  
  public static final String readOnlyJMXOperations = "(^list.*|^fetch.*|^view.*|^show.*|^queryData.*)";

  public ReadOpFileAccessController(String accessFileName) throws IOException {
    super(accessFileName);
  }
  
  @Override
  public Object invoke(ObjectName name, String operationName, Object params[],
      String signature[]) throws InstanceNotFoundException, MBeanException,
      ReflectionException {
    
    
    if(operationName.matches(readOnlyJMXOperations)) {
      checkRead();
      return getMBeanServer().invoke(name, operationName, params, signature);
    } else {
      return super.invoke(name, operationName, params, signature);
    }
    
  }  

}
