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

package com.gemstone.gemfire.internal.cache.xmlcache;

import java.util.ServiceLoader;
import java.util.Stack;

import org.xml.sax.ContentHandler;

import com.gemstone.gemfire.LogWriter;
import com.gemstone.gemfire.i18n.LogWriterI18n;

/**
 * Interface for configuration XML parsers. Used by {@link CacheXmlParser} to
 * parse entities defined in the XML Namespace returned by
 * {@link #getNamspaceUri()} .
 * 
 * Loaded by {@link ServiceLoader} on {@link XmlParser} class. See file
 * <code>META-INF/services/com.gemstone.gemfire.internal.cache.xmlcache.XmlParser</code>
 * 
 *
 * @since 8.1
 */
public interface XmlParser extends ContentHandler {

  /**
   * Get XML Namespace this parser is responsible for.
   * 
   * @return XML Namespace.
   * @since 8.1
   */
  String getNamspaceUri();

  /**
   * Sets the XML config stack on this parser.
   * 
   * @param stack
   *          current XML config stack.
   * @since 8.1
   */
  void setStack(Stack<Object> stack);
}
