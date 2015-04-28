/*
 * =========================================================================
 *  Copyright (c) 2002-2014 Pivotal Software, Inc. All Rights Reserved.
 *  This product is protected by U.S. and international copyright
 *  and intellectual property laws. Pivotal products are covered by
 *  more patents listed at http://www.pivotal.io/patents.
 * ========================================================================
 */
package com.gemstone.gemfire.management.internal.cli.help.format;

import java.util.List;

/**
 * @author Nikhil Jadhav
 *
 */
public class DataNode {
  String data;
  List<DataNode> children;

  public DataNode(String data, List<DataNode> dataNode) {
    this.data = data;
    this.children = dataNode;
  }

  public String getData() {
    return data;
  }
  
  public List<DataNode> getChildren() {
    return children;
  }
  
  public boolean addChild(DataNode dataNode) {
    if (this.children != null) {
      this.children.add(dataNode);
      return true;
    } else {
      return false;
    }
  }

}
