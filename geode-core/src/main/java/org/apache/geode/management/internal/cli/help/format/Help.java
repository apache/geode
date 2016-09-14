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
package org.apache.geode.management.internal.cli.help.format;

/**
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
