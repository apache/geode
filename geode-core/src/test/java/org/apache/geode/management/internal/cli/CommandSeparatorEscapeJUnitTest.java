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
package org.apache.geode.management.internal.cli;

import static org.apache.geode.management.internal.cli.shell.MultiCommandHelper.*;
import static org.junit.Assert.*;

import java.util.List;

import org.junit.Test;
import org.junit.experimental.categories.Category;

import org.apache.geode.test.junit.categories.UnitTest;

@Category(UnitTest.class)
public class CommandSeparatorEscapeJUnitTest {

  //testcases : single command
  //testcases : multiple commands with cmdSeparator
  //testcases : single command with comma-value
  //testcases : multiplecommand with comma-value : first value
  //testcases : multiplecommand with comma-value : last value
  //testcases : multiplecommand with comma-value : middle value

  @Test
  public void testEmptyCommand() {
    String input = ";";
    //System.out.println("I >> " + input);
    List<String> split = getMultipleCommands(input);
    /*for(String s : split){
      System.out.println("O >> " + s);
    }*/
    assertEquals(0,split.size());    
  }

  @Test
  public void testSingleCommand() {
    String input = "stop server";
    //System.out.println("I >> " + input);
    List<String> split = getMultipleCommands(input);
    for(String s : split){
      System.out.println("O >> " + s);
    }
    assertEquals(1,split.size());
    assertEquals("stop server", split.get(0));
  }

  @Test
  public void testMultiCommand() {
    String input = "stop server1 --option1=value1; stop server2;stop server3 ";
    //System.out.println("I >> " + input);
    List<String> split = getMultipleCommands(input);
    /*for(String s : split){
      System.out.println("O >> " + s);
    }*/
    assertEquals(3,split.size());
    assertEquals("stop server1 --option1=value1", split.get(0));
    assertEquals(" stop server2", split.get(1));
    assertEquals("stop server3 ", split.get(2));
  }

  @Test
  public void testMultiCommandWithCmdSep() {
    String input = "put --region=/region1 --key='key1\\;part' --value='value1\\;part2';put --region=/region1 --key='key2\\;part' --value='value2\\;part2'";
    //System.out.println("I >> " + input);
    List<String> split = getMultipleCommands(input);
    /*for(String s : split){
      System.out.println("O >> " + s);
    }*/
    assertEquals(2,split.size());
    assertEquals("put --region=/region1 --key='key1;part' --value='value1;part2'", split.get(0));
    assertEquals("put --region=/region1 --key='key2;part' --value='value2;part2'", split.get(1));
  }

  @Test
  public void testSingleCommandWithComma() {
    String input = "put --region=/region1 --key='key\\;part' --value='value\\;part2'";
    //System.out.println("I >> " + input);
    List<String> split = getMultipleCommands(input);
    /*for(String s : split){
      System.out.println("O >> " + s);
    }*/
    assertEquals(1,split.size());
    assertEquals("put --region=/region1 --key='key;part' --value='value;part2'", split.get(0));
  }

  @Test
  public void testMultiCmdCommaValueFirst() {
    String input = "put --region=/region1 --key='key\\;part' --value='value\\;part2';stop server";
    //System.out.println("I >> " + input);
    List<String> split = getMultipleCommands(input);
    /*for(String s : split){
      System.out.println("O >> " + s);
    }*/
    assertEquals(2,split.size());
    assertEquals("put --region=/region1 --key='key;part' --value='value;part2'", split.get(0));
    assertEquals("stop server", split.get(1));
  }

  @Test
  public void testMultiCmdCommaValueLast() {
    String input = "stop server;put --region=/region1 --key='key\\;part' --value='value\\;part2'";
    //System.out.println("I >> " + input);
    List<String> split = getMultipleCommands(input);
    /*for(String s : split){
      System.out.println("O >> " + s);
    }*/
    assertEquals(2,split.size());
    assertEquals("stop server", split.get(0));
    assertEquals("put --region=/region1 --key='key;part' --value='value;part2'", split.get(1));    
  }

  @Test
  public void testMultiCmdCommaValueMiddle() {
    String input = "stop server1;put --region=/region1 --key='key\\;part' --value='value\\;part2';stop server2;stop server3";
    //System.out.println("I >> " + input);
    List<String> split = getMultipleCommands(input);
    /*for(String s : split){
      System.out.println("O >> " + s);
    }*/
    assertEquals(4,split.size());
    assertEquals("stop server1", split.get(0));
    assertEquals("put --region=/region1 --key='key;part' --value='value;part2'", split.get(1));
    assertEquals("stop server2", split.get(2));
    assertEquals("stop server3", split.get(3));
  }

}
