/*
 *  =========================================================================
 *  Copyright (c) 2002-2014 Pivotal Software, Inc. All Rights Reserved.
 * This product is protected by U.S. and international copyright
 * and intellectual property laws. Pivotal products are covered by
 * more patents listed at http://www.pivotal.io/patents.
 *  ========================================================================
 */
package com.gemstone.gemfire.admin.jmx.internal;

import com.gemstone.gemfire.admin.Statistic;
import com.gemstone.gemfire.internal.Assert;

import javax.management.Descriptor;
import javax.management.modelmbean.DescriptorSupport;
import javax.management.modelmbean.ModelMBeanAttributeInfo;

/** 
 * Subclass of AttributeInfo with {@link com.gemstone.gemfire.admin.Statistic} 
 * added for use as the {@link 
 * javax.management.modelmbean.ModelMBeanAttributeInfo} descriptor's 
 * <i>targetObject</i> value.
 *
 * @author    Kirk Lund
 * @since     3.5
 *
 */
class StatisticAttributeInfo extends org.apache.commons.modeler.AttributeInfo {
  private static final long serialVersionUID = 28022387514935560L;
    
  private Statistic stat;
  
  public StatisticAttributeInfo() {
    super();
  }
  
  public Statistic getStat() {
    return this.stat;
  }
  public void setStat(Statistic stat) {
    //System.out.println(">> stat = " + stat);
    Assert.assertTrue(stat != null, "Attempting to set stat to null");
    this.stat = stat;
  }
  
  @Override
  public ModelMBeanAttributeInfo createAttributeInfo() {
    Descriptor desc = new DescriptorSupport(
        new String[] {
        "name=" + this.displayName,
        "descriptorType=attribute",
        "currencyTimeLimit=-1", // always stale
        "displayName=" + this.displayName,
        "getMethod=getValue" });

    Assert.assertTrue(this.stat != null, "Stat target object is null!");
    desc.setField("targetObject", this.stat);

    ModelMBeanAttributeInfo info = new ModelMBeanAttributeInfo(
        this.displayName, // name
        this.type,        // type
        this.description, // description
        this.readable,    // isReadable
        this.writeable,   // isWritable
        this.is,          // isIs
        desc);
        
    return info;
  }
}

