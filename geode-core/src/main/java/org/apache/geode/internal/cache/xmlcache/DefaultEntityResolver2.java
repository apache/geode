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
import java.io.InputStream;

import org.xml.sax.InputSource;
import org.xml.sax.SAXException;
import org.xml.sax.ext.EntityResolver2;

import org.apache.geode.internal.classloader.ClassPathLoader;

/**
 * Default behavior for EntityResolver2 implementations.
 * <p>
 * UnitTest PivotalEntityResolverJUnitTest and DefaultEntityResolver2Test
 *
 * @since GemFire 8.1
 */
public abstract class DefaultEntityResolver2 implements EntityResolver2 {

  @Override
  public InputSource resolveEntity(final String publicId, final String systemId)
      throws SAXException, IOException {
    return resolveEntity(null, publicId, null, systemId);
  }

  @Override
  public InputSource getExternalSubset(final String name, final String baseURI)
      throws SAXException, IOException {
    return null;
  }

  @Override
  public InputSource resolveEntity(final String name, final String publicId, final String baseURI,
      final String systemId) throws SAXException, IOException {
    return null;
  }

  /**
   * Get {@link InputSource} for path in class path.
   *
   * @param path to resource to get {@link InputSource} for.
   * @return InputSource if resource found, otherwise null.
   * @since GemFire 8.1
   */
  protected InputSource getClassPathInputSource(final String publicId, final String systemId,
      final String path) {
    final InputStream stream = ClassPathLoader.getLatest().getResourceAsStream(getClass(), path);
    if (null == stream) {
      return null;
    }

    final InputSource inputSource = new InputSource(stream);
    inputSource.setPublicId(publicId);
    inputSource.setSystemId(systemId);

    return inputSource;
  }

}
