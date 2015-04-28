/*=========================================================================
 * Copyright (c) 2010-2014 Pivotal Software, Inc. All Rights Reserved.
 * This product is protected by U.S. and international copyright
 * and intellectual property laws. Pivotal products are covered by
 * one or more patents listed at http://www.pivotal.io/patents.
 *=========================================================================
 */
package com.gemstone.gemfire.internal.jndi;

import javax.naming.NameParser;
import javax.naming.Name;
import javax.naming.NamingException;
import javax.naming.CompoundName;
import java.util.Properties;

/**
 * ContextImpl name parser.
 * 
 * @author Nand Kishor Jha
 */
class NameParserImpl implements NameParser {

  private static final Properties syntax = new Properties();
  static {
    syntax.put("jndi.syntax.direction", "left_to_right");
    syntax.put("jndi.syntax.separator", "/");
    syntax.put("jndi.syntax.ignorecase", "false");
    syntax.put("jndi.syntax.trimblanks", "yes");
  }

  /**
   * Parses name into CompoundName using the following CompoundName properties:
   * <p>
   * jndi.syntax.direction = "left_to_right" jndi.syntax.separator = "/"
   * jndi.syntax.ignorecase = "false" jndi.syntax.trimblanks = "yes"
   * <p>
   * Any characters '.' in the name <code>name</code> will be replaced with
   * the separator character specified above, before parsing.
   * 
   * @param name name to parse
   * @throws NamingException if naming error occurrs
   */
  public Name parse(String name) throws NamingException {
    return new CompoundName(name.replace('.', '/'), syntax);
  }
}
