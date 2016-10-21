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
package org.apache.geode.distributed.internal;

import static org.apache.geode.management.internal.configuration.utils.XmlConstants.*;
import static javax.xml.XMLConstants.*;
import static org.junit.Assert.*;

import org.apache.commons.io.IOUtils;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.w3c.dom.Document;

import org.apache.geode.internal.cache.xmlcache.CacheXml;
import org.apache.geode.management.internal.configuration.utils.XmlUtils;
import org.apache.geode.test.junit.categories.UnitTest;

/**
 * Unit tests for {@link SharedConfiguration}.
 *
 * @since GemFire 8.1
 */
@Category(UnitTest.class)
public class SharedConfigurationJUnitTest {

  /**
   * Test {@link SharedConfiguration#createAndUpgradeDocumentFromXml(String)}
   */
  @Test
  public void testCreateAndUpgradeDocumentFromXml() throws Exception {
    Document doc = SharedConfiguration.createAndUpgradeDocumentFromXml(
        IOUtils.toString(this.getClass().getResourceAsStream("SharedConfigurationJUnitTest.xml")));

    String schemaLocation = XmlUtils.getAttribute(doc.getDocumentElement(),
        W3C_XML_SCHEMA_INSTANCE_ATTRIBUTE_SCHEMA_LOCATION, W3C_XML_SCHEMA_INSTANCE_NS_URI);

    assertNotNull(schemaLocation);
    assertEquals(CacheXml.GEODE_NAMESPACE + " " + CacheXml.LATEST_SCHEMA_LOCATION, schemaLocation);

    assertEquals(CacheXml.VERSION_LATEST,
        XmlUtils.getAttribute(doc.getDocumentElement(), "version"));
  }

}
