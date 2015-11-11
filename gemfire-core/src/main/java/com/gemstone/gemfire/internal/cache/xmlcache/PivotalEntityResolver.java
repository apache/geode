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
public final class PivotalEntityResolver extends DefaultEntityResolver2 {

  private static final String SYSTEM_ID_ROOT = "http://schema.pivotal.io/";

  private static final String CLASSPATH_ROOT = "/META-INF/schemas/schema.pivotal.io/";

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
