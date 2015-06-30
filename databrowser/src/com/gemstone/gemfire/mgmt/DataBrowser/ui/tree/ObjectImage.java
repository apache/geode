package com.gemstone.gemfire.mgmt.DataBrowser.ui.tree;

import java.util.ArrayList;
import java.util.List;

public class ObjectImage {
  private String name;
  private Object value;
  private int type;
  private String typeName;
  private ObjectImage parent;
  private List<ObjectImage> children;
  
  public ObjectImage(String n, int t, Object v) {
    super();
    name = n;
    type = t;
    value = v;
    children = new ArrayList<ObjectImage>();
  }
  
  public void setTypeName(String typeNm) {
    typeName = typeNm;
  }
  
  public String getTypeName() {
    return typeName;
  }
  
  public void addChild(ObjectImage child) {
   children.add(child); 
  }
  
  public List<ObjectImage> getChildren() {
    return children;
  }
  
  public void setParent(ObjectImage prnt) {
    parent = prnt;
  }
  
  public ObjectImage getParent() {
    return parent;
  }
  
  public int getType() {
    return type;
  }
  
  public Object getValue() {
    return value;
  }
  
  public String getName() {
    return name;
  }
}
