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

package org.apache.geode.internal.cache.xmlcache;

import java.io.IOException;
import java.util.ServiceLoader;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.xml.sax.InputSource;
import org.xml.sax.SAXException;

import org.apache.geode.services.module.ModuleService;

/**
 * Resolves entities for XSDs or DTDs with SYSTEM IDs rooted at http(s)://schema.pivotal.io/ from
 * the classpath at /META-INF/schemas/schema.pivotal.io/.
 *
 * Loaded by {@link ServiceLoader} on {@link GeodeEntityResolver2} class. See file
 * <code>META-INF/services/org.apache.geode.internal.cache.xmlcache.GeodeEntityResolver2</code>
 *
 *
 * @since GemFire 8.1
 */
public class PivotalEntityResolver extends DefaultEntityResolver2 {

  private static final Pattern SYSTEM_ID_ROOT = Pattern.compile("^https?://schema.pivotal.io/");

  private static final String CLASSPATH_ROOT = "/META-INF/schemas/schema.pivotal.io/";
  private ModuleService moduleService;

  @Override
  public InputSource resolveEntity(final String name, final String publicId, final String baseURI,
      final String systemId) throws SAXException, IOException {
    if (null == systemId) {
      return null;
    }

    Matcher matcher = SYSTEM_ID_ROOT.matcher(systemId);
    if (matcher.find()) {
      return getClassPathInputSource(publicId, systemId, matcher.replaceFirst(CLASSPATH_ROOT),
          moduleService);
    }

    return null;
  }

  @Override
  public void init(ModuleService moduleService) {
    this.moduleService = moduleService;
  }
}
