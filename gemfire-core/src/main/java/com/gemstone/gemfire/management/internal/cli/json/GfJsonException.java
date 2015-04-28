/*
 * =========================================================================
 * Copyright (c) 2002-2014 Pivotal Software, Inc. All Rights Reserved.
 * This product is protected by U.S. and international copyright
 * and intellectual property laws. Pivotal products are covered by
 * more patents listed at http://www.pivotal.io/patents.
 * ========================================================================
 */
package com.gemstone.gemfire.management.internal.cli.json;

/**
 * Wraps GemFire JSON Exceptions
 * 
 * 
 * @author Abhishek Chaudhari
 * 
 * @since 7.0
 */
public class GfJsonException extends Exception {

  private static final long serialVersionUID = 36449998984143318L;

  public GfJsonException(String message) {
    super(message);
  }
  public GfJsonException(Exception e) {
    super(e);
  }
}