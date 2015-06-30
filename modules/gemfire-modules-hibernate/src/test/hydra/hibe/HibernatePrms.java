/*=========================================================================
 * Copyright (c) 2010-2014 Pivotal Software, Inc. All Rights Reserved.
 * This product is protected by U.S. and international copyright
 * and intellectual property laws. Pivotal products are covered by
 * one or more patents listed at http://www.pivotal.io/patents.
 *=========================================================================
 */
package hibe;

import hydra.*;

public class HibernatePrms extends BasePrms {

/** (boolean) True if the test execute operations within a single transaction
 *  Defaults to false
 */
public static Long useTransactions;  
public static boolean useTransactions() {
  Long key = useTransactions;
  return tasktab().booleanAt( key, tab().booleanAt( key, false ));
}


public static Long persistenceXml;  
public static String getPersistenceXml() {
  Long key = persistenceXml;
  return tasktab().stringAt( key, tab().stringAt( key, null ));
}



public static Long cachingStrategy;  
public static String getCachingStrategy() {
  Long key = cachingStrategy;
  return tasktab().stringAt( key, tab().stringAt( key, null ));
}


// ================================================================================
static {
   BasePrms.setValues(HibernatePrms.class);
}

}
