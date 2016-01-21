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

package com.gemstone.gemfire.cache.lucene.internal.xml;

import static com.gemstone.gemfire.cache.lucene.internal.xml.LuceneXmlConstants.*;

import org.xml.sax.Attributes;
import org.xml.sax.SAXException;

import com.gemstone.gemfire.cache.lucene.internal.LuceneServiceImpl;
import com.gemstone.gemfire.internal.cache.xmlcache.AbstractXmlParser;
import com.gemstone.gemfire.internal.cache.xmlcache.RegionAttributesCreation;
import com.gemstone.gemfire.internal.cache.xmlcache.RegionCreation;

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
    creation.addField(name);
  }

  private void startIndex(Attributes atts) {
    final RegionCreation region = (RegionCreation) stack.peek();
    RegionAttributesCreation rac = (RegionAttributesCreation) region.getAttributes();
    String name = atts.getValue(NAME);
    rac.addAsyncEventQueueId(LuceneServiceImpl.getUniqueIndexName(name, region.getFullPath()));
    
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
}
