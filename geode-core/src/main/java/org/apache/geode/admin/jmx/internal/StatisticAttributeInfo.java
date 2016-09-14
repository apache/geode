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
package org.apache.geode.admin.jmx.internal;

import org.apache.geode.admin.Statistic;
import org.apache.geode.internal.Assert;

import javax.management.Descriptor;
import javax.management.modelmbean.DescriptorSupport;
import javax.management.modelmbean.ModelMBeanAttributeInfo;

/** 
 * Subclass of AttributeInfo with {@link org.apache.geode.admin.Statistic} 
 * added for use as the {@link 
 * javax.management.modelmbean.ModelMBeanAttributeInfo} descriptor's 
 * <i>targetObject</i> value.
 *
 * @since GemFire     3.5
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

