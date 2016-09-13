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
package com.gemstone.gemfire.internal.jndi;

import javax.naming.NameParser;
import javax.naming.Name;
import javax.naming.NamingException;
import javax.naming.CompoundName;
import java.util.Properties;

/**
 * ContextImpl name parser.
 * 
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
