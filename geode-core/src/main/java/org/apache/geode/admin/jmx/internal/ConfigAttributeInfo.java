/*
 * Licensed to the Apache Software Foundation (ASF) under one or more contributor license
 * agreements. See the NOTICE file distributed with this work for additional information regarding
 * copyright ownership. The ASF licenses this file to You under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance with the License. You may obtain a
 * copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software distributed under the License
 * is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express
 * or implied. See the License for the specific language governing permissions and limitations under
 * the License.
 */
package org.apache.geode.admin.jmx.internal;

import javax.management.Descriptor;
import javax.management.modelmbean.DescriptorSupport;
import javax.management.modelmbean.ModelMBeanAttributeInfo;

import org.apache.geode.internal.Assert;

/**
 * Subclass of AttributeInfo with {@link org.apache.geode.admin.ConfigurationParameter} added for
 * use as the {@link javax.management.modelmbean.ModelMBeanAttributeInfo} descriptor's
 * <i>targetObject</i> value.
 *
 * @since GemFire 3.5
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
    return config;
  }

  @Override
  public ModelMBeanAttributeInfo createAttributeInfo() {
    Descriptor desc = new DescriptorSupport("name=" + displayName,
        "descriptorType=attribute", "currencyTimeLimit=-1", // always stale
        "displayName=" + displayName, "getMethod=getJmxValue", "setMethod=setJmxValue");

    Assert.assertTrue(config != null, "Config target object is null!");
    desc.setField("targetObject", config);

    ModelMBeanAttributeInfo info = new ModelMBeanAttributeInfo(displayName, // name
        type, // type
        description, // description
        readable, // isReadable
        writeable, // isWritable
        is, // isIs
        desc);

    return info;
  }
}
