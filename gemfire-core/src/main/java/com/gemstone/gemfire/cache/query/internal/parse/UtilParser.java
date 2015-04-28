/*=========================================================================
 * Copyright (c) 2010-2014 Pivotal Software, Inc. All Rights Reserved.
 * This product is protected by U.S. and international copyright
 * and intellectual property laws. Pivotal products are covered by
 * one or more patents listed at http://www.pivotal.io/patents.
 *=========================================================================
 */
/*
 * Main.java
 *
 * Created on January 19, 2005, 2:18 PM
 */

package com.gemstone.gemfire.cache.query.internal.parse;

import antlr.*;
import antlr.collections.*;
import antlr.debug.misc.ASTFrame;
import java.io.*;

/**
 *
 * @author ericz
 */
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
     *  Parse a query string. Gets the string from stdin unless
     *  cmd line has a string in it that doesn't start with "-",
     *  in which case it parses that string instead.
     *  A cmd line arg  that starts with "-f" causes out put
     *  to be put into a GUI tree widget thingy; otherwise,
     *  the output is a LISP-like string to stdout
     */
    public static void main (String[] args) throws Exception {        
        boolean useFrame = false;
        Reader reader = new InputStreamReader(System.in);
        if (args.length > 0 && args[0].startsWith("-f")) useFrame = true;
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
        parser.setASTNodeClass("com.gemstone.gemfire.cache.query.internal.parse.ASTUnsupported");
        parser.queryProgram();
        AST t = parser.getAST();
        if (useFrame) {
            ASTFrame frame = new ASTFrame("OQL Example", t);
            frame.setVisible(true);
        }
        else {
            if (t == null) {
                System.out.println("AST is NULL");
            }
            else {
                System.out.println(t.toStringTree());
            }
        }
    }
    
}
