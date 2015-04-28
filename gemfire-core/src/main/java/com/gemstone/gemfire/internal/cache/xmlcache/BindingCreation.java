/*=========================================================================
 * Copyright (c) 2010-2014 Pivotal Software, Inc. All Rights Reserved.
 * This product is protected by U.S. and international copyright
 * and intellectual property laws. Pivotal products are covered by
 * one or more patents listed at http://www.pivotal.io/patents.
 *=========================================================================
 */
/*
 * Created on Apr 18, 2005
 *
 * 
 */
package com.gemstone.gemfire.internal.cache.xmlcache;

import java.util.*;

/**
 * @author asifs This class represents the data given for binding a DataSource
 *         to JNDI tree. It encapsulates to Map objects , one for gemfire jndi
 *         tree specific data & another for vendor specific data. This object
 *         will get created for every <jndi-binding></jndi-binding>
 *  
 */
public class BindingCreation  {

  private Map gfSpecific = null;
  // private Map vendorSpecific = null;
  private List vendorSpecificList = null;

  public BindingCreation(Map gfSpecific, List vendorSpecific) {
    this.gfSpecific = gfSpecific;
    this.vendorSpecificList = vendorSpecific;
  }

  /**
   * This function returns the VendorSpecific data Map
   * 
   * @return List
   */
  List getVendorSpecificList() {
    return this.vendorSpecificList;
  }

  /**
   * This function returns the Gemfire Specific data Map
   * 
   * @return Map
   */
  Map getGFSpecificMap() {
    return this.gfSpecific;
  }
}
