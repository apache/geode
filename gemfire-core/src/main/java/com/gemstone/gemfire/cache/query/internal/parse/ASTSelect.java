/*=========================================================================
 * Copyright (c) 2005-2014 Pivotal Software, Inc. All Rights Reserved.
 * This product is protected by U.S. and international copyright
 * and intellectual property laws. Pivotal products are covered by
 * more patents listed at http://www.pivotal.io/patents.
 *========================================================================
 */


package com.gemstone.gemfire.cache.query.internal.parse;

import antlr.*;
import antlr.collections.*;
import com.gemstone.gemfire.cache.query.internal.QCompiler;

public class ASTSelect extends GemFireAST {
  private static final long serialVersionUID = 1389351692304773456L;
  
  
  public ASTSelect() { }
  
  
  public ASTSelect(Token t) {
    super(t);
  }
  
  
  @Override
  public void compile(QCompiler compiler) {
  	AST child = getFirstChild();
        int clauseType = child.getType();
  	if(clauseType == OQLLexerTokenTypes.LITERAL_hint) {
          ((GemFireAST)child).compile(compiler);  
          child = child.getNextSibling();
        }else {
          //hint is null
          compiler.pushNull();
        }
  	
    // check for DISTINCT or ALL token
    // if DISTINCT, push "DISTINCT" onto stack, otherwise push null
    // if found, then advance to next sibling, otherwise this child
    // must be projection
    if (child.getType() == OQLLexerTokenTypes.LITERAL_distinct) {
      compiler.push("DISTINCT"); // anything non-null works here for distinct
      child = child.getNextSibling();
    }
    else if (child.getType() == OQLLexerTokenTypes.LITERAL_all) {
      compiler.pushNull();
      child = child.getNextSibling();
    } 
    else {
      compiler.pushNull(); // let child be next in line
    }
    
    //Count(*) expression
    if (child.getType() == OQLLexerTokenTypes.LITERAL_count) {
      ((ASTCount)child).compile(compiler);
      compiler.pushNull(); //For No projectionAttributes
    } else {
      compiler.pushNull();
      // projectionAttributes
      if (child.getType() == OQLLexerTokenTypes.TOK_STAR) {
        compiler.pushNull();
      }
      else {
        // child is ASTCombination; compile it
        ((ASTCombination)child).compile(compiler);
      }
    }

    // fromClause
    child = child.getNextSibling(); 
    ((GemFireAST)child).compile(compiler);
   
      
	/*If WHERE clause ,order by clause as well as Limit clause is missing, then push 3 null as a placeholder */ 
    if (child.getNextSibling() == null) {
      compiler.pushNull();
      compiler.pushNull();
      //Asif: This placeholder is for limit 
      compiler.pushNull();
    }
    else { 
       child = child.getNextSibling();      
       clauseType = child.getType();
       if( clauseType != OQLLexerTokenTypes.LITERAL_order && clauseType != OQLLexerTokenTypes.LIMIT ) {
         //  where is present , order by & limit may present |  may !present
         ((GemFireAST)child).compile(compiler);
         child = child.getNextSibling();
         if(child != null) {
           clauseType = child.getType();
         }
         
       }else {
         //Where clause is null
         compiler.pushNull();         
       }
       if(clauseType == OQLLexerTokenTypes.LITERAL_order) {
         ((GemFireAST)child).compile(compiler);
         child = child.getNextSibling();
         if(child != null) {
           clauseType = child.getType();
         } 
       }else {
         //Order by clause is null
         compiler.pushNull();
       }
       
       if(clauseType == OQLLexerTokenTypes.LIMIT) {
         ((GemFireAST)child).compile(compiler);  
         child = child.getNextSibling();
         if(child != null) {
           clauseType = child.getType();
         } 
       }else {
         //Limit clause is null
         compiler.pushNull();
       }
    }    
    compiler.select();
  }
  
  
}
