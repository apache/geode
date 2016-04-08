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
