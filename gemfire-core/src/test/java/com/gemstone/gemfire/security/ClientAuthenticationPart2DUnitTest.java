/*=========================================================================
 * Copyright (c) 2010-2014 Pivotal Software, Inc. All Rights Reserved.
 * This product is protected by U.S. and international copyright
 * and intellectual property laws. Pivotal products are covered by
 * one or more patents listed at http://www.pivotal.io/patents.
 *=========================================================================
 */
/**
 * 
 */
package com.gemstone.gemfire.security;

/**
 * this class contains test methods that used to be in its superclass but
 * that test started taking too long and caused dunit runs to hang
 */
public class ClientAuthenticationPart2DUnitTest extends
    ClientAuthenticationDUnitTest {

  /** constructor */
  public ClientAuthenticationPart2DUnitTest(String name) {
    super(name);
  }

  // override inherited tests so they aren't executed again
  
  @Override
  public void testValidCredentials() {  }
  @Override
  public void testNoCredentials() {  }
  @Override
  public void testInvalidCredentials() {  }
  @Override
  public void testInvalidAuthInit() {  }
  @Override
  public void testNoAuthInitWithCredentials() {  }
  @Override
  public void testInvalidAuthenticator() {  }
  @Override
  public void testNoAuthenticatorWithCredentials() {  }
  @Override
  public void testCredentialsWithFailover() {  }
  @Override
  public void testCredentialsForNotifications() {  }
  //@Override
  public void testValidCredentialsForMultipleUsers() {  }


  
  
  
  public void testNoCredentialsForMultipleUsers() {
    itestNoCredentials(Boolean.TRUE);
  }
  public void testInvalidCredentialsForMultipleUsers() {
    itestInvalidCredentials(Boolean.TRUE);
  }
  public void testInvalidAuthInitForMultipleUsers() {
    itestInvalidAuthInit(Boolean.TRUE);
  }
  public void testNoAuthInitWithCredentialsForMultipleUsers() {
    itestNoAuthInitWithCredentials(Boolean.TRUE);
  }
  public void testInvalidAuthenitcatorForMultipleUsers() {
    itestInvalidAuthenticator(Boolean.TRUE);
  }
  public void testNoAuthenticatorWithCredentialsForMultipleUsers() {
    itestNoAuthenticatorWithCredentials(Boolean.TRUE);
  }
  public void disabled_testCredentialsWithFailoverForMultipleUsers() {
    itestCredentialsWithFailover(Boolean.TRUE);
  }
  public void __testCredentialsForNotificationsForMultipleUsers() {
    itestCredentialsForNotifications(Boolean.TRUE);
  }

}
