/*
 * =========================================================================
 *  Copyright (c) 2002-2014 Pivotal Software, Inc. All Rights Reserved.
 *  This product is protected by U.S. and international copyright
 *  and intellectual property laws. Pivotal products are covered by
 *  more patents listed at http://www.pivotal.io/patents.
 * ========================================================================
 */
package com.gemstone.gemfire.management.internal.cli.parser;

/**
 * Object used for ordering method parameters
 * 
 * @author Nikhil Jadhav
 * @since 7.0
 * 
 */
public class MethodParameter {
  private final Object parameter;
  private final int parameterNo;

  public MethodParameter(Object parameter, int parameterNo) {
    this.parameter = parameter;
    this.parameterNo = parameterNo;
  }

  public Object getParameter() {
    return parameter;
  }

  public int getParameterNo() {
    return parameterNo;
  }
}
