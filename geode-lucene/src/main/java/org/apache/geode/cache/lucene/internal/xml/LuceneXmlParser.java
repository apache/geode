/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 * 
 *   http://www.apache.org/licenses/LICENSE-2.0
 * 
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.apache.geode.cache.lucene.internal.xml;

import static org.apache.geode.cache.lucene.internal.xml.LuceneXmlConstants.*;

import org.apache.geode.cache.CacheXmlException;
import org.apache.geode.internal.InternalDataSerializer;
import org.apache.geode.internal.i18n.LocalizedStrings;
import org.apache.lucene.analysis.Analyzer;
import org.xml.sax.Attributes;
import org.xml.sax.SAXException;

import org.apache.geode.cache.lucene.internal.LuceneServiceImpl;
import org.apache.geode.internal.cache.xmlcache.AbstractXmlParser;
import org.apache.geode.internal.cache.xmlcache.RegionAttributesCreation;
import org.apache.geode.internal.cache.xmlcache.RegionCreation;

public class LuceneXmlParser extends AbstractXmlParser {

  @Override
  public String getNamspaceUri() {
    return NAMESPACE;
  }

  @Override
  public void startElement(String uri, String localName, String qName,
      Attributes atts) throws SAXException {
    
    if(!NAMESPACE.equals(uri)) {
      return;
    }
    if(INDEX.equals(localName)) {
      startIndex(atts);
    }
    if(FIELD.equals(localName)) {
      startField(atts);
    }
  }

  private void startField(Attributes atts) {
    //Ignore any whitespace noise between fields
    if(stack.peek() instanceof StringBuffer) {
      stack.pop();
    }
    LuceneIndexCreation creation = (LuceneIndexCreation) stack.peek();
    String name = atts.getValue(NAME);
    String className = atts.getValue(ANALYZER);
    if (className == null) {
      creation.addField(name);
    } else {
      Analyzer analyzer = createAnalyzer(className);
      creation.addFieldAndAnalyzer(name, analyzer);
    }
  }

  private void startIndex(Attributes atts) {
    final RegionCreation region = (RegionCreation) stack.peek();
    String name = atts.getValue(NAME);
    LuceneIndexCreation indexCreation = new LuceneIndexCreation();
    indexCreation.setName(name);
    indexCreation.setRegion(region);
    region.getExtensionPoint().addExtension(indexCreation);
    stack.push(indexCreation);
  }

  @Override
  public void endElement(String uri, String localName, String qName)
      throws SAXException {
    if(!NAMESPACE.equals(uri)) {
      return;
    }
    if(INDEX.equals(localName)) {
      endIndex();
    }
  }

  private void endIndex() {
    //Ignore any whitespace noise between fields
    if(stack.peek() instanceof StringBuffer) {
      stack.pop();
    }
    
    //Remove the index creation from the stack
    stack.pop();
  }

  private Analyzer createAnalyzer(String className) {
    Object obj;
    try {
      Class c = InternalDataSerializer.getCachedClass(className);
      obj = c.newInstance();
    }
    catch (Exception ex) {
      throw new CacheXmlException(LocalizedStrings.CacheXmlParser_WHILE_INSTANTIATING_A_0.toLocalizedString(className), ex);
    }
    if (!(obj instanceof Analyzer)) {
      throw new CacheXmlException(LocalizedStrings.LuceneXmlParser_CLASS_0_IS_NOT_AN_INSTANCE_OF_ANALYZER.toLocalizedString(className));
    }
    return (Analyzer) obj;
  }
}
