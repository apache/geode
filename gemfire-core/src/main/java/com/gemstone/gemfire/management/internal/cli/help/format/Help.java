/*
 * =========================================================================
 *  Copyright (c) 2002-2014 Pivotal Software, Inc. All Rights Reserved.
 *  This product is protected by U.S. and international copyright
 *  and intellectual property laws. Pivotal products are covered by
 *  more patents listed at http://www.pivotal.io/patents.
 * ========================================================================
 */
package com.gemstone.gemfire.management.internal.cli.help.format;

/**
 * @author Nikhil Jadhav
 *
 */
public class Help {
  private Block[] blocks;

  public Block[] getBlocks() {
    return blocks;
  }

  public Help setBlocks(Block[] block) {
    this.blocks = block;
    return this;
  }

  @Override
  public String toString() {
    StringBuffer buffer = new StringBuffer();
    for(Block block:blocks){
      buffer.append(block.getHeading()+"\n");
      for(Row row:block.getRows()){
        buffer.append("\t"+row.getInfo()[0]+"\n");
      }
      buffer.append("\n");
    }
    return buffer.toString();
  }
}
