/*=========================================================================
 * Copyright (c) 2002-2014 Pivotal Software, Inc. All Rights Reserved.
 * This product is protected by U.S. and international copyright
 * and intellectual property laws. Pivotal products are covered by
 * more patents listed at http://www.pivotal.io/patents.
 *=========================================================================
 */

package com.gemstone.gemfire.internal.cache.xmlcache;

import java.io.IOException;
import java.util.ServiceLoader;

import org.xml.sax.InputSource;
import org.xml.sax.SAXException;
import org.xml.sax.ext.EntityResolver2;

/**
 * Resolves entities for XSDs or DTDs with SYSTEM IDs rooted at
 * http://www.pivotal.io/xml/ns from the classpath at
 * /META-INF/schemas/schema.pivotal.io/.
 * 
 * Loaded by {@link ServiceLoader} on {@link EntityResolver2} class. See file
 * <code>META-INF/services/org.xml.sax.ext.EntityResolver2</code>
 * 
 * @author jbarrett@pivotal.io
 * 
 * @since 8.1
 */
public final class GeodeEntityResolver extends DefaultEntityResolver2 {

  private static final String SYSTEM_ID_ROOT = "http://geode.incubator.apache.org/schema";

  private static final String CLASSPATH_ROOT = "/META-INF/schemas/geode.incubator.apache.org/";

  @Override
  public InputSource resolveEntity(final String name, final String publicId, final String baseURI, final String systemId) throws SAXException, IOException {
    if (null == systemId) {
      return null;
    }

    if (systemId.startsWith(SYSTEM_ID_ROOT)) {
      return getClassPathInputSource(publicId, systemId, CLASSPATH_ROOT + systemId.substring(SYSTEM_ID_ROOT.length()));
    }

    return null;
  }

}
