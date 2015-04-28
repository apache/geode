/*=========================================================================
 * Copyright (c) 2010-2014 Pivotal Software, Inc. All Rights Reserved.
 * This product is protected by U.S. and international copyright
 * and intellectual property laws. Pivotal products are covered by
 * one or more patents listed at http://www.pivotal.io/patents.
 *=========================================================================
 */
/*
 * Created on Nov 18, 2005
 *
 * 
 */
package com.gemstone.gemfire.cache.query.internal;

/**
 * Helper class object which gets created during organization of operands in a
 * GroupJunction or a CompiledJunction. The filterOperand refers to a filter
 * evaluatable group of conditions. The group itself may be a single
 * GroupJunction or an AllGroupJunction or a CompositeGroupJunction. It may also
 * be a CompiledJunction, if there exists a filter evaluatable subtree of
 * CompiledJunction along with any of the above mentioned three structures . The
 * iterateOperand refers to a single or group of conditions which are not filter
 * evaluatable.
 * 
 * @author asif
 * 
 *  
 */
class OrganizedOperands  {

  Filter filterOperand;
  CompiledValue iterateOperand;
  /**
   * Asif : This field indicates if there exists only a Single Filter operand in
   * a GroupJunction ( implying a CompiledComparison or CompiledUndefined.) or
   * CompiledJunction ( implying a GroupJunction or an AllGroupJunction or a
   * CompositeGroupJunction ).Inside a GroupJunction , if the boolean is true
   * then iter operands can be passed as a parameter which can be evaluated
   * during index result expansion/truncation etc . Also the filterEvaluate
   * method will be invoked directly . If there exists multiple filter ops (
   * multiple CompiledComparisons ) then a Dummy GroupJunction will get created &
   * the boolean will be false. In that case , the iter ops passed will be null,
   * as evident from the the auxFilterEvauate function of this class.
   */
  boolean isSingleFilter;
}
