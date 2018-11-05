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
package org.apache.geode.admin.internal;

import java.io.InputStream;

import org.xml.sax.EntityResolver;
import org.xml.sax.ErrorHandler;
import org.xml.sax.InputSource;
import org.xml.sax.SAXException;
import org.xml.sax.SAXNotRecognizedException;
import org.xml.sax.SAXParseException;

import org.apache.geode.distributed.ConfigurationProperties;
import org.apache.geode.internal.ClassPathLoader;

/**
 * The abstract superclass of classes that convert XML into a
 * {@link org.apache.geode.admin.DistributedSystemConfig} and vice versa. It provides helper methods
 * and constants.
 *
 * @since GemFire 4.0
 */
abstract class ManagedEntityConfigXml implements EntityResolver, ErrorHandler {

  /** The location of the DTD file */
  protected static final String DTD_LOCATION = "/org/apache/geode/admin/doc-files/ds5_0.dtd";

  /** The URL for the DTD */
  protected static final String SYSTEM_ID = "http://www.gemstone.com/dtd/ds5_0.dtd";

  /** The public ID for the DTD */
  protected static final String PUBLIC_ID =
      "-//GemStone Systems, Inc.//GemFire Distributed System 5.0//EN";

  /** The name of the <code>distributed-system</code> element. */
  public static final String DISTRIBUTED_SYSTEM = "distributed-system";

  /** The name of the <code>id</code> attribute. */
  public static final String ID = "id";

  /** The name of the <code>disable-tcp</code> attribute. */
  public static final String DISABLE_TCP = "disable-tcp";

  /** The name of the <code>remote-command</code> element. */
  public static final String REMOTE_COMMAND = "remote-command";

  /** The name of the <code>locators</code> element. */
  public static final String LOCATORS = ConfigurationProperties.LOCATORS;

  /** The name of the <code>ssl</code> element. */
  public static final String SSL = "ssl";

  /** The name of the <code>cache-server</code> element */
  public static final String CACHE_SERVER = "cache-server";

  /** The name of the <code>multicast</code> element */
  public static final String MULTICAST = "multicast";

  /** The name of the <code>locator</code> element */
  public static final String LOCATOR = "locator";

  /** The name of the <code>port</code> attribute */
  public static final String PORT = "port";

  /** The name of the <code>address</code> attribute */
  public static final String ADDRESS = "address";

  /** The name of the <code>host</code> element. */
  public static final String HOST = "host";

  /** The name of the <code>working-directory</code> element */
  public static final String WORKING_DIRECTORY = "working-directory";

  /** The name of the <code>product-directory</code> element */
  public static final String PRODUCT_DIRECTORY = "product-directory";

  /** The name of the <code>protocols</code> element */
  public static final String PROTOCOLS = "protocols";

  /** The name of the <code>ciphers</code> element */
  public static final String CIPHERS = "ciphers";

  /** The name of the <code>property</code> element */
  public static final String PROPERTY = "property";

  /** Name of the <code>authentication-required</code> attribute */
  public static final String AUTHENTICATION_REQUIRED = "authentication-required";

  /** The name of the <code>key</code> element */
  public static final String KEY = "key";

  /** The name of the <code>value</code> element */
  public static final String VALUE = "value";

  /** The name of the <code>classpath</code> element */
  public static final String CLASSPATH = "classpath";

  /////////////////////// Instance Methods ///////////////////////

  /**
   * Given a public id, attempt to resolve it to a DTD. Returns an <code>InputSoure</code> for the
   * DTD.
   */
  public InputSource resolveEntity(String publicId, String systemId) throws SAXException {

    if (publicId == null || systemId == null) {
      throw new SAXException(String.format("Public Id: %s System Id: %s",
          new Object[] {publicId, systemId}));
    }

    // Figure out the location for the publicId.
    String location = DTD_LOCATION;

    InputSource result;
    {
      InputStream stream = ClassPathLoader.getLatest().getResourceAsStream(getClass(), location);
      if (stream != null) {
        result = new InputSource(stream);
      } else {
        throw new SAXNotRecognizedException(
            String.format("DTD not found: %s", location));
      }
    }

    return result;
  }

  /**
   * Warnings are ignored
   */
  public void warning(SAXParseException ex) throws SAXException {

  }

  /**
   * Throws a {@link org.apache.geode.cache.CacheXmlException}
   */
  public void error(SAXParseException ex) throws SAXException {
    IllegalArgumentException ex2 = new IllegalArgumentException(
        "Error while parsing XML.");
    ex2.initCause(ex);
    throw ex2;
  }

  /**
   * Throws a {@link org.apache.geode.cache.CacheXmlException}
   */
  public void fatalError(SAXParseException ex) throws SAXException {
    IllegalArgumentException ex2 = new IllegalArgumentException(
        "Fatal error while parsing XML.");
    ex2.initCause(ex);
    throw ex2;
  }

}
