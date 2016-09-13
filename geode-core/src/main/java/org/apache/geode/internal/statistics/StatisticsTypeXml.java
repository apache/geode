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
package com.gemstone.gemfire.internal.statistics;

import com.gemstone.gemfire.*;
import com.gemstone.gemfire.internal.Assert;
import com.gemstone.gemfire.internal.ClassPathLoader;
import com.gemstone.gemfire.internal.i18n.LocalizedStrings;

import java.io.*;
import java.util.*;
import javax.xml.parsers.*;
//import javax.xml.transform.*;
//import javax.xml.transform.dom.*;
//import javax.xml.transform.stream.*;
import org.w3c.dom.*;
import org.xml.sax.*;

// @todo davidw Use a SAX parser instead of DOM
/**
 * This is an internal helper class for dealing with the
 * SessionFactory XML configuration files.
 */
public class StatisticsTypeXml 
  implements EntityResolver, ErrorHandler {

  /** The name of the DTD file */
  static final String DTD = "statisticsType.dtd";

  static final String systemId = "http://www.gemstone.com/dtd/" + DTD;
  static final String publicId = 
    "-//GemStone Systems, Inc.//GemFire StatisticsType//EN";

  /////////////////////  Interface methods  ///////////////////////

  /**
   * Given a publicId, attempts to resolve it to a DTD.  Returns an
   * <code>InputSource</code> for the DTD.
   */
  public InputSource resolveEntity (String publicId, String systemId)
    throws SAXException {

    // Figure out the location for the publicId.  Be tolerant of other
    // versions of the dtd
    if(publicId.equals(StatisticsTypeXml.publicId) ||
       systemId.equals(StatisticsTypeXml.systemId) ||
       systemId.endsWith(DTD)) {

      // Public ID for system config DTD
      String location = "/com/gemstone/gemfire/" + DTD;
      InputStream stream = ClassPathLoader.getLatest().getResourceAsStream(getClass(), location);
      if (stream != null) {
        return new InputSource(stream);

      } else {
        throw new SAXNotRecognizedException(LocalizedStrings.StatisticsTypeXml_DTD_NOT_FOUND_0.toLocalizedString(location));
      }

    } else {
      throw new SAXNotRecognizedException(LocalizedStrings.StatisticsTypeXml_INVALID_PUBLIC_ID_0.toLocalizedString(publicId)); 
    }
  }

  public void warning(SAXParseException exception) throws SAXException
  { 
    // We don't want to thrown an exception.  We want to log it!!
    // FIXME
//    String s = "SAX warning while working with XML";
  }

  public void error(SAXParseException exception) throws SAXException
  {
    throw new GemFireConfigException(LocalizedStrings.StatisticsTypeXml_SAX_ERROR_WHILE_WORKING_WITH_XML.toLocalizedString(), exception);
  }
  
  public void fatalError(SAXParseException exception) throws SAXException
  {
    throw new GemFireConfigException(LocalizedStrings.StatisticsTypeXml_SAX_FATAL_ERROR_WHILE_WORKING_WITH_XML.toLocalizedString(), exception);
  }

  //////////////////////  Parsing XML File  ////////////////////////

  /**
   * Parses the contents of XML data and from it creates one or more
   * <code>StatisticsType</code> instances.
   */
  public StatisticsType[] read( Reader reader, StatisticsTypeFactory statFactory) {
    DocumentBuilderFactory factory =
      DocumentBuilderFactory.newInstance();
//     factory.setValidating(validate);

    DocumentBuilder parser = null;
    try {
      parser = factory.newDocumentBuilder();

    } catch (ParserConfigurationException ex) {
      throw new GemFireConfigException(LocalizedStrings.StatisticsTypeXml_FAILED_PARSING_XML.toLocalizedString(), ex);
    }

    parser.setErrorHandler(this);
    parser.setEntityResolver(this);
    Document doc;
    try {
      doc = parser.parse(new InputSource(reader));
    } catch (SAXException se) {
      throw new GemFireConfigException(LocalizedStrings.StatisticsTypeXml_FAILED_PARSING_XML.toLocalizedString(), se);
    } catch (IOException io) {
      throw new GemFireConfigException(LocalizedStrings.StatisticsTypeXml_FAILED_READING_XML_DATA.toLocalizedString(), io);
    }

    if (doc == null) {
      throw new GemFireConfigException(LocalizedStrings.StatisticsTypeXml_FAILED_READING_XML_DATA_NO_DOCUMENT.toLocalizedString());
    }
    Element root = doc.getDocumentElement();
    if (root == null) {
      throw new GemFireConfigException(LocalizedStrings.StatisticsTypeXml_FAILED_READING_XML_DATA_NO_ROOT_ELEMENT.toLocalizedString());
    }
    return extractStatistics(root, statFactory);
  }

  /*
   * <!ELEMENT statistics (type)+>
   */
  private StatisticsType[] extractStatistics(Element root, StatisticsTypeFactory statFactory) {
    Assert.assertTrue(root.getTagName().equals("statistics"));

    ArrayList types = new ArrayList();
    NodeList typeNodes = root.getElementsByTagName("type");
    for (int i = 0; i < typeNodes.getLength(); i++) {
      Element typeNode = (Element) typeNodes.item(i);
      types.add(extractType(typeNode, statFactory));
    }
    return (StatisticsType[])types.toArray(new StatisticsType[types.size()]);
  }
  /**
   * <!ELEMENT type (description?, (stat)+)>
   * <!ATTLIST type  name CDATA #REQUIRED>
   */
  private StatisticsType extractType(Element typeNode, StatisticsTypeFactory statFactory) {
    Assert.assertTrue(typeNode.getTagName().equals("type"));
    Assert.assertTrue(typeNode.hasAttribute("name"));

    final String typeName = typeNode.getAttribute("name");
    ArrayList stats = new ArrayList();
    NodeList statNodes = typeNode.getElementsByTagName("stat");
    for (int i = 0; i < statNodes.getLength(); i++) {
      Element statNode = (Element) statNodes.item(i);
      stats.add(extractStat(statNode, statFactory));
    }
    StatisticDescriptor[] descriptors =
      (StatisticDescriptor[])stats.toArray(new StatisticDescriptor[stats.size()]);
    String description = "";
    {
      NodeList descriptionNodes = typeNode.getElementsByTagName("description");
      if (descriptionNodes.getLength() > 0) {
        // descriptionNodes will contain the both our description, if it exists,
        // and any nested stat descriptions. Ours will always be first
        Element descriptionNode = (Element) descriptionNodes.item(0);
        // but make sure the first one belongs to our node
        if (descriptionNode.getParentNode().getNodeName().equals(typeNode.getNodeName())) {
          description = extractDescription(descriptionNode);
        }
      }
    }

    return statFactory.createType(typeName, description, descriptors);
  }
  private final static int INT_STORAGE = 0;
  private final static int LONG_STORAGE = 1;
  private final static int DOUBLE_STORAGE = 2;
  /**
   * <!ELEMENT stat (description?, unit?)>
   * <!ATTLIST stat
   *   name CDATA #REQUIRED
   *   counter (true | false) #IMPLIED
   *   largerBetter (true | false) #IMPLIED
   *   storage (int | long | double) #IMPLIED 
   * >
   */
  private StatisticDescriptor extractStat(Element statNode, StatisticsTypeFactory statFactory) {
    Assert.assertTrue(statNode.getTagName().equals("stat"));
    Assert.assertTrue(statNode.hasAttribute("name"));

    final String statName = statNode.getAttribute("name");
    String description = "";
    String unit = "";
    boolean isCounter = true;
    boolean largerBetter;
    int storage = INT_STORAGE;
    
    if ( statNode.hasAttribute("counter")) {
      String value = statNode.getAttribute("counter");
      Assert.assertTrue(value.equalsIgnoreCase("true") ||
                    value.equalsIgnoreCase("false"));
      isCounter = Boolean.valueOf(value).booleanValue();
    }
    largerBetter = isCounter; // default
    if ( statNode.hasAttribute("largerBetter")) {
      String value = statNode.getAttribute("largerBetter");
      Assert.assertTrue(value.equalsIgnoreCase("true") ||
                    value.equalsIgnoreCase("false"));
      largerBetter = Boolean.valueOf(value).booleanValue();
    }
    if ( statNode.hasAttribute("storage")) {
      String value = statNode.getAttribute("storage");
      if (value.equalsIgnoreCase("int")) {
        storage = INT_STORAGE;
      } else if (value.equalsIgnoreCase("long")) {
        storage = LONG_STORAGE;
      } else {
        Assert.assertTrue(value.equalsIgnoreCase("double"));
        storage = DOUBLE_STORAGE;
      }
    }
    {
      NodeList descriptionNodes =
        statNode.getElementsByTagName("description");
      Assert.assertTrue(descriptionNodes.getLength() <= 1);
      if (descriptionNodes.getLength() == 1) {
        Element descriptionNode = (Element) descriptionNodes.item(0);
        description = extractDescription(descriptionNode);
      }
    }

    {
      NodeList unitNodes =
        statNode.getElementsByTagName("unit");
      Assert.assertTrue(unitNodes.getLength() <= 1);
      if (unitNodes.getLength() == 1) {
        Element unitNode = (Element) unitNodes.item(0);
        unit = extractUnit(unitNode);
      }
    }
    if (isCounter) {
      switch (storage) {
      case INT_STORAGE: return statFactory.createIntCounter(statName, description, unit, largerBetter);
      case LONG_STORAGE: return statFactory.createLongCounter(statName, description, unit, largerBetter);
      case DOUBLE_STORAGE: return statFactory.createDoubleCounter(statName, description, unit, largerBetter);
      default: throw new RuntimeException(LocalizedStrings.StatisticsTypeXml_UNEXPECTED_STORAGE_TYPE_0.toLocalizedString(Integer.valueOf(storage)));
      }
    } else {
      switch (storage) {
      case INT_STORAGE: return statFactory.createIntGauge(statName, description, unit, largerBetter);
      case LONG_STORAGE: return statFactory.createLongGauge(statName, description, unit, largerBetter);
      case DOUBLE_STORAGE: return statFactory.createDoubleGauge(statName, description, unit, largerBetter);
      default: throw new RuntimeException(LocalizedStrings.StatisticsTypeXml_UNEXPECTED_STORAGE_TYPE_0.toLocalizedString(Integer.valueOf(storage)));
      }
    }
  }
  /**
   * <!ELEMENT description (#PCDATA)>
   */
  private String extractDescription(Element descriptionNode) {
    Assert.assertTrue(descriptionNode.getTagName().equals("description"));
    return extractText(descriptionNode);
  }
  /**
   * <!ELEMENT unit (#PCDATA)>
   */
  private String extractUnit(Element unitNode) {
    Assert.assertTrue(unitNode.getTagName().equals("unit"));
    return extractText(unitNode);
  }
  private String extractText(Element element) {
    Text text = (Text) element.getFirstChild();
    return((text == null ? "" : text.getData()));
  }
}
