/*
 *  =========================================================================
 *  Copyright (c) 2002-2014 Pivotal Software, Inc. All Rights Reserved.
 * This product is protected by U.S. and international copyright
 * and intellectual property laws. Pivotal products are covered by
 * more patents listed at http://www.pivotal.io/patents.
 *  ========================================================================
 */
package com.gemstone.gemfire.admin.jmx.internal;

//import com.gemstone.gemfire.admin.ConfigurationParameter;
import com.gemstone.gemfire.internal.Assert;

import javax.management.Descriptor;
import javax.management.modelmbean.DescriptorSupport;
import javax.management.modelmbean.ModelMBeanAttributeInfo;

/** 
 * Subclass of AttributeInfo with {@link 
 * com.gemstone.gemfire.admin.ConfigurationParameter} added for use as the 
 * {@link javax.management.modelmbean.ModelMBeanAttributeInfo} descriptor's 
 * <i>targetObject</i> value.
 *
 * @author    Kirk Lund
 * @since     3.5
 *
 */
class ConfigAttributeInfo extends org.apache.commons.modeler.AttributeInfo {
  private static final long serialVersionUID = -1918437700841687078L;
  
  private final ConfigurationParameterJmxImpl config;
  
  public ConfigAttributeInfo(ConfigurationParameterJmxImpl config) {
    super();
    this.config = config;
  }
  
  public ConfigurationParameterJmxImpl getConfig() {
    return this.config;
  }

  @Override
  public ModelMBeanAttributeInfo createAttributeInfo() {
    Descriptor desc = new DescriptorSupport(
        new String[] {
        "name=" + this.displayName,
        "descriptorType=attribute",
        "currencyTimeLimit=-1", // always stale
        "displayName=" + this.displayName,
        "getMethod=getJmxValue",
        "setMethod=setJmxValue" 
        });
        
    Assert.assertTrue(this.config != null, "Config target object is null!");
    desc.setField("targetObject", this.config);

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

