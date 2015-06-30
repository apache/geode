package com.gemstone.gemfire.management.internal.cli.dto;

import java.io.Serializable;
import java.util.List;
import java.util.Map;
import java.util.Set;

public class Car implements Serializable {
  
  private String make;
  private String model;
  private List<String> colors;
  private Map<String,String> attributes;
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
