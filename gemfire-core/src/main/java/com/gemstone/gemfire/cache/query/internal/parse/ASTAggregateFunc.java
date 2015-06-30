package com.gemstone.gemfire.cache.query.internal.parse;

import antlr.Token;

import com.gemstone.gemfire.cache.query.QueryInvalidException;
import com.gemstone.gemfire.cache.query.internal.CompiledValue;
import com.gemstone.gemfire.cache.query.internal.QCompiler;

/**
 * 
 * @author ashahid
 *
 */
public class ASTAggregateFunc extends GemFireAST {
  private static final long serialVersionUID = 8713004765228379685L;
  private int  aggFunctionType;
  private boolean  distinctOnly = false;
  
  
  public ASTAggregateFunc() { 
    
  }
  
  
  public ASTAggregateFunc(Token t) {
    super(t);    
  }
  
  public void setAggregateFunctionType(int type) {
    this.aggFunctionType = type;
  }
  
  public void setDistinctOnly(boolean distinctOnly) {
    this.distinctOnly = distinctOnly;
  }
  
  @Override
  public void compile(QCompiler compiler) {
    super.compile(compiler);
    Object expr = compiler.pop();
    if(expr instanceof String) {
      if(((String)expr).equals("*")) {
        expr = null;
      }else {
        throw new QueryInvalidException("invalid parameter to aggregate function");
      }
    }
    compiler.aggregateFunction((CompiledValue)expr, this.aggFunctionType, this.distinctOnly);
  }
}
