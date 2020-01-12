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
package org.apache.geode.management.internal.cli.dto;

import java.io.Serializable;
import java.util.List;
import java.util.Map;
import java.util.Set;

public class Car implements Serializable {

  private String make;
  private String model;
  private List<String> colors;
  private Map<String, String> attributes;
  private Set<String> attributeSet;
  private String[] attributeArray;

  public String getMake() {
    return make;
  }

  public void setMake(String make) {
    this.make = make;
  }

  public String getModel() {
    return model;
  }

  public void setModel(String model) {
    this.model = model;
  }

  public List<String> getColors() {
    return colors;
  }

  public void setColors(List<String> colors) {
    this.colors = colors;
  }

  public Map<String, String> getAttributes() {
    return attributes;
  }

  public void setAttributes(Map<String, String> attributes) {
    this.attributes = attributes;
  }

  public Set<String> getAttributeSet() {
    return attributeSet;
  }

  public void setAttributeSet(Set<String> attributeSet) {
    this.attributeSet = attributeSet;
  }

  public String[] getAttributeArray() {
    return attributeArray;
  }

  public void setAttributeArray(String[] attributeArray) {
    this.attributeArray = attributeArray;
  }



}
