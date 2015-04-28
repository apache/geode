/*
 * =========================================================================
 *  Copyright (c) 2002-2014 Pivotal Software, Inc. All Rights Reserved.
 *  This product is protected by U.S. and international copyright
 *  and intellectual property laws. Pivotal products are covered by
 *  more patents listed at http://www.pivotal.io/patents.
 * ========================================================================
 */
package com.gemstone.gemfire.management.internal.cli.help.format;

import com.gemstone.gemfire.management.internal.cli.GfshParser;
import com.gemstone.gemfire.management.internal.cli.shell.Gfsh;

/**
 * 
 * @author Nikhil Jadhav
 * @since 7.0
 */
public class NewHelp {
  DataNode root;

  public NewHelp(DataNode root) {
    this.root = root;
  }
  
  @Override
  public String toString() {
    StringBuilder builder = new StringBuilder();
    append(builder, root, 0);
    return builder.toString();
  }

  private void append(StringBuilder builder, DataNode dataNode, int level) {
    if (dataNode != null && builder != null) {
      String data = dataNode.getData();
      if (data != null && !data.equals("")) {
        builder.append(Gfsh.wrapText(data, level-1));
        builder.append(GfshParser.LINE_SEPARATOR);
      }
      if (dataNode.getChildren() != null && dataNode.getChildren().size() > 0) {
        for (DataNode child : dataNode.getChildren()) {
          append(builder, child, level+1);
        }
      }
    }
  }
}
