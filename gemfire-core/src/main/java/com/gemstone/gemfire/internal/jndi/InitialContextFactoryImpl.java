/*=========================================================================
 * Copyright (c) 2010-2014 Pivotal Software, Inc. All Rights Reserved.
 * This product is protected by U.S. and international copyright
 * and intellectual property laws. Pivotal products are covered by
 * one or more patents listed at http://www.pivotal.io/patents.
 *=========================================================================
 */
package com.gemstone.gemfire.internal.jndi;

import java.util.HashMap;
import java.util.Hashtable;
import java.util.Map;
import java.util.Iterator;
import javax.naming.NamingException;
import javax.naming.spi.InitialContextFactory;
import javax.naming.Context;

/**
 * Factory class to creates ContextImpl. Provides implementation for
 * InitialContextFactory.
 * 
 * Optionally, this also facilitates the backup of original system property
 * which can be restored later.
 * 
 * @author Nand Kishor Jha
 */
public class InitialContextFactoryImpl implements InitialContextFactory {

  private static Map oldSystemProps = new HashMap();
//  private static Hashtable env;
  private static Context ctx;

  /**
   * Singleton for initial context. Instantiates and returns root/initial
   * ContextImpl object that will be used as starting point for all naming
   * operations. ContextImpl is then used by javax.naming.InitialContext
   * object. InitialContextFactoryImpl caches the context once it's created.
   * 
   * @param environment Hashtable, contains the Context property to set to get
   *            the instance of this context.
   * @return ContextImpl object.
   */
  public synchronized Context getInitialContext(Hashtable environment)
      throws NamingException {
    if (ctx == null) {
      ctx = new ContextImpl();
    }
    return ctx;
  }

  /**
   * Sets the InitialContextFactoryImpl as the initial context factory. This
   * helper method sets the Context.INITIAL_CONTEXT_FACTORY system properties.
   * The method also saves the current values of these properties so they can
   * be restored later on using revertSetAsInitial. This method can be called
   * from from setup. These properties can be set directly directly too or
   * through application resource file (jndi.properties).
   * java.naming.factory.initial=com.gemstone.gemfire.internal.jndi.InitialContextFactoryImpl
   * 
   * @throws NamingException
   */
  public void setAsInitial() throws NamingException {
    // Preserve current set system props
    String key = Context.INITIAL_CONTEXT_FACTORY;
    oldSystemProps.put(key, System.getProperty(key));
    key = Context.URL_PKG_PREFIXES;
    oldSystemProps.put(key, System.getProperty(key));
    System.setProperty(Context.INITIAL_CONTEXT_FACTORY,
        InitialContextFactoryImpl.class.getName());
    System.setProperty(Context.URL_PKG_PREFIXES,
        "com.gemstone.gemfire.internal.jndi");
  }

  /**
   * Restores the properties changed by setAsInitial(). This method should be
   * called in tearDown()to clean up all changes to the environment in case if
   * the test is running in the app server.
   */
  public static void revertSetAsInitial() {
    Iterator i = oldSystemProps.entrySet().iterator();
    while (i.hasNext()) {
      Map.Entry entry = (Map.Entry) i.next();
      restoreSystemProperty((String) entry.getKey(), (String) entry.getValue());
    }
  }

  private static void restoreSystemProperty(String key, String value) {
    if (value != null)
      System.setProperty(key, value);
    else
      System.getProperties().remove(key);
  }
}
