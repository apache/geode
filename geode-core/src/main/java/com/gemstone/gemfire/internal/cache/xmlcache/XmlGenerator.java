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

import org.xml.sax.SAXException;

/**
 * Interface for configuration XML generators. Used by {@link CacheXmlGenerator}
 * to generate entities defined in the XML Namespace returned by
 * {@link #getNamspaceUri()} .
 * 
 *
 * @since GemFire 8.1
 */
public interface XmlGenerator<T> {

  /**
   * Get XML Namespace this parser is responsible for.
   * 
   * @return XML Namespace.
   * @since GemFire 8.1
   */
  String getNamspaceUri();

  // TODO jbarrett - investigate new logging.
  // /**
  // * Sets the XML config {@link LogWriter} on this parser.
  // *
  // * @param logWriter
  // * current XML config {@link LogWriter}.
  // * @since GemFire 8.1
  // */
  // void setLogWriter(LogWriterI18n logWriter);
  //

  /**
   * Generate XML configuration to the given {@link CacheXmlGenerator}.
   * 
   * @param cacheXmlGenerator
   *          to generate configuration to.
   * @throws SAXException
   * @since GemFire 8.1
   */
  void generate(CacheXmlGenerator cacheXmlGenerator) throws SAXException;

}
