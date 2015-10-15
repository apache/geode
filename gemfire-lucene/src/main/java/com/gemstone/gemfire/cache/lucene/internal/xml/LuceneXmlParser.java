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
