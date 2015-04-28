/*=========================================================================
 * Copyright (c) 2002-2014 Pivotal Software, Inc. All Rights Reserved.
 * This product is protected by U.S. and international copyright
 * and intellectual property laws. Pivotal products are covered by
 * more patents listed at http://www.pivotal.io/patents.
 *=========================================================================
 */
package com.gemstone.gemfire.distributed.internal;

import static com.gemstone.gemfire.management.internal.configuration.utils.XmlConstants.*;
import static javax.xml.XMLConstants.*;
import static org.junit.Assert.*;

import org.apache.commons.io.IOUtils;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.w3c.dom.Document;

import com.gemstone.gemfire.internal.cache.xmlcache.CacheXml;
import com.gemstone.gemfire.management.internal.configuration.utils.XmlUtils;
import com.gemstone.junit.UnitTest;

/**
 * Unit tests for {@link SharedConfiguration}.
 * 
 * @author jbarrett@pivotal.io
 *
 * @since 8.1
 */
@Category(UnitTest.class)
public class SharedConfigurationJUnitTest {

  /**
   * Test {@link SharedConfiguration#createAndUpgradeDocumentFromXml()}
   */
  @Test
  public void testCreateAndUpgradeDocumentFromXml() throws Exception {
    Document doc = SharedConfiguration.createAndUpgradeDocumentFromXml(IOUtils.toString(this.getClass().getResourceAsStream("SharedConfigurationJUnitTest.xml")));

    String schemaLocation = XmlUtils.getAttribute(doc.getDocumentElement(), W3C_XML_SCHEMA_INSTANCE_ATTRIBUTE_SCHEMA_LOCATION, W3C_XML_SCHEMA_INSTANCE_NS_URI);

    assertNotNull(schemaLocation);
    assertEquals(CacheXml.NAMESPACE + " " + CacheXml.LATEST_SCHEMA_LOCATION, schemaLocation);

    assertEquals(CacheXml.VERSION_LATEST, XmlUtils.getAttribute(doc.getDocumentElement(), "version"));
  }

}
