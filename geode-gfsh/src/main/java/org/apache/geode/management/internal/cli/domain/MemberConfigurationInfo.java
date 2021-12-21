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
package org.apache.geode.management.internal.cli.domain;

import java.io.Serializable;
import java.lang.management.ManagementFactory;
import java.lang.management.RuntimeMXBean;
import java.util.List;
import java.util.Map;
import java.util.Properties;

import org.apache.geode.internal.util.ArgumentRedactor;

public class MemberConfigurationInfo implements Serializable {

  /**
   * JVM arguments can potentially contain sensitive information. If these arguments are ever to be
   * displayed, remember to apply
   * {@link org.apache.geode.internal.util.ArgumentRedactor#redact(String)}
   * to each argument.
   */
  private List<String> jvmInputArguments;
  private Properties systemProperties;
  private Map<String, String> gfePropsSetUsingApi;
  private Map<String, String> gfePropsRuntime;
  private Map<String, String> gfePropsSetWithDefaults;
  private Map<String, String> gfePropsSetFromFile;
  private Map<String, String> cacheAttributes;
  private List<Map<String, String>> cacheServerAttributes;
  private Map<String, String> pdxAttributes;

  public MemberConfigurationInfo() {
    RuntimeMXBean runtimeBean = ManagementFactory.getRuntimeMXBean();
    setJvmInputArguments(ArgumentRedactor.redactEachInList(runtimeBean.getInputArguments()));

  }

  public List<String> getJvmInputArguments() {
    return jvmInputArguments;
  }

  public void setJvmInputArguments(List<String> jvmInputArguments) {
    this.jvmInputArguments = jvmInputArguments;
  }


  public Properties getSystemProperties() {
    return systemProperties;
  }

  public void setSystemProperties(Properties systemProperties) {
    this.systemProperties = systemProperties;
  }

  public Map<String, String> getGfePropsSetUsingApi() {
    return gfePropsSetUsingApi;
  }

  public void setGfePropsSetUsingApi(Map<String, String> gfePropsSetUsingApi) {
    this.gfePropsSetUsingApi = gfePropsSetUsingApi;
  }

  public Map<String, String> getGfePropsRuntime() {
    return gfePropsRuntime;
  }

  public void setGfePropsRuntime(Map<String, String> gfePropsRuntime) {
    this.gfePropsRuntime = gfePropsRuntime;
  }

  public Map<String, String> getGfePropsSetWithDefaults() {
    return gfePropsSetWithDefaults;
  }

  public void setGfePropsSetWithDefaults(Map<String, String> gfePropsSetWithDefaults) {
    this.gfePropsSetWithDefaults = gfePropsSetWithDefaults;
  }

  public Map<String, String> getGfePropsSetFromFile() {
    return gfePropsSetFromFile;
  }

  public void setGfePropsSetFromFile(Map<String, String> gfePropsSetFromFile) {
    this.gfePropsSetFromFile = gfePropsSetFromFile;
  }


  public Map<String, String> getCacheAttributes() {
    return cacheAttributes;
  }

  public void setCacheAttributes(Map<String, String> cacheAttributes) {
    this.cacheAttributes = cacheAttributes;
  }

  public List<Map<String, String>> getCacheServerAttributes() {
    return cacheServerAttributes;
  }

  public void setCacheServerAttributes(List<Map<String, String>> cacheServerAttributes) {
    this.cacheServerAttributes = cacheServerAttributes;
  }

  public Map<String, String> getPdxAttrributes() {
    return pdxAttributes;
  }

  public void setPdxAttrributes(Map<String, String> pdxAttrributes) {
    pdxAttributes = pdxAttrributes;
  }
}
