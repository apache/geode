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
/*
 * Main.java
 *
 * Created on January 19, 2005, 2:18 PM
 */

package org.apache.geode.cache.query.internal.parse;

import java.io.InputStreamReader;
import java.io.Reader;
import java.io.StringReader;

import antlr.LLkParser;
import antlr.ParserSharedInputState;
import antlr.TokenBuffer;
import antlr.TokenStream;
import antlr.collections.AST;
import antlr.debug.misc.ASTFrame;

public class UtilParser extends LLkParser {

  public UtilParser(int k_) {
    super(k_);
  }

  public UtilParser(ParserSharedInputState state, int k_) {
    super(state, k_);
  }

  public UtilParser(TokenBuffer tokenBuf, int k_) {
    super(tokenBuf, k_);
  }

  public UtilParser(TokenStream lexer, int k_) {
    super(lexer, k_);
  }

  /**
   * Parse a query string. Gets the string from stdin unless cmd line has a string in it that
   * doesn't start with "-", in which case it parses that string instead. A cmd line arg that starts
   * with "-f" causes out put to be put into a GUI tree widget thingy; otherwise, the output is a
   * LISP-like string to stdout
   */
  public static void main(String[] args) throws Exception {
    boolean useFrame = false;
    Reader reader = new InputStreamReader(System.in);
    if (args.length > 0 && args[0].startsWith("-f")) {
      useFrame = true;
    }
    for (int i = 0; i < args.length; i++) {
      if (!args[i].startsWith("-")) {
        reader = new StringReader(args[i]);
        System.out.println("Parsing: \"" + args[i] + "\"");
        break;
      }
    }
    OQLLexer lexer = new OQLLexer(reader);
    OQLParser parser = new OQLParser(lexer);
    // by default use Unsupported AST class, overridden for supported
    // operators in the grammer proper
    parser.setASTNodeClass("org.apache.geode.cache.query.internal.parse.ASTUnsupported");
    parser.queryProgram();
    AST t = parser.getAST();
    if (useFrame) {
      ASTFrame frame = new ASTFrame("OQL Example", t);
      frame.setVisible(true);
    } else {
      if (t == null) {
        System.out.println("AST is NULL");
      } else {
        System.out.println(t.toStringTree());
      }
    }
  }

}
