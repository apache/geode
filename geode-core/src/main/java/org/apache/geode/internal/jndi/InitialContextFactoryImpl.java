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
package org.apache.geode.internal.jndi;

import java.util.HashMap;
import java.util.Hashtable;
import java.util.Map;

import javax.naming.Context;
import javax.naming.NamingException;
import javax.naming.spi.InitialContextFactory;

import org.apache.geode.annotations.internal.MakeNotStatic;

/**
 * Factory class to creates ContextImpl. Provides implementation for InitialContextFactory.
 *
 * Optionally, this also facilitates the backup of original system property which can be restored
 * later.
 *
 */
public class InitialContextFactoryImpl implements InitialContextFactory {

  @MakeNotStatic
  private static final Map oldSystemProps = new HashMap();
  // private static Hashtable env;
  @MakeNotStatic
  private static Context ctx;

  /**
   * Singleton for initial context. Instantiates and returns root/initial ContextImpl object that
   * will be used as starting point for all naming operations. ContextImpl is then used by
   * javax.naming.InitialContext object. InitialContextFactoryImpl caches the context once it's
   * created.
   *
   * @param environment Hashtable, contains the Context property to set to get the instance of this
   *        context.
   * @return ContextImpl object.
   */
  @Override
  public synchronized Context getInitialContext(Hashtable environment) throws NamingException {
    if (ctx == null) {
      ctx = new ContextImpl();
    }
    return ctx;
  }

  /**
   * Sets the InitialContextFactoryImpl as the initial context factory. This helper method sets the
   * Context.INITIAL_CONTEXT_FACTORY system properties. The method also saves the current values of
   * these properties so they can be restored later on using revertSetAsInitial. This method can be
   * called from from setup. These properties can be set directly directly too or through
   * application resource file (jndi.properties).
   * java.naming.factory.initial=org.apache.geode.internal.jndi.InitialContextFactoryImpl
   *
   */
  public void setAsInitial() throws NamingException {
    // Preserve current set system props
    String key = Context.INITIAL_CONTEXT_FACTORY;
    oldSystemProps.put(key, System.getProperty(key));
    key = Context.URL_PKG_PREFIXES;
    oldSystemProps.put(key, System.getProperty(key));
    System.setProperty(Context.INITIAL_CONTEXT_FACTORY, InitialContextFactoryImpl.class.getName());
    System.setProperty(Context.URL_PKG_PREFIXES, "org.apache.geode.internal.jndi");
  }

  /**
   * Restores the properties changed by setAsInitial(). This method should be called in tearDown()to
   * clean up all changes to the environment in case if the test is running in the app server.
   */
  public static void revertSetAsInitial() {
    for (final Object o : oldSystemProps.entrySet()) {
      Map.Entry entry = (Map.Entry) o;
      restoreSystemProperty((String) entry.getKey(), (String) entry.getValue());
    }
  }

  private static void restoreSystemProperty(String key, String value) {
    if (value != null) {
      System.setProperty(key, value);
    } else {
      System.getProperties().remove(key);
    }
  }
}
