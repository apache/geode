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
  }

  private void startIndex(Attributes atts) {
    final RegionCreation region = (RegionCreation) stack.peek();
    RegionAttributesCreation rac = (RegionAttributesCreation) region.getAttributes();
    String name = atts.getValue(NAME);
    String[] fields = atts.getValue(FIELDS).split(" *, *");
    rac.addAsyncEventQueueId(LuceneServiceImpl.getUniqueIndexName(name, region.getFullPath()));
    
    
    LuceneIndexCreation indexCreation = new LuceneIndexCreation();
    indexCreation.setName(name);
    indexCreation.setFieldNames(fields);
    indexCreation.setRegion(region);
    region.getExtensionPoint().addExtension(indexCreation, indexCreation);
    //TODO support nested field objects by adding the creation object to the stack
    //stack.push(indexCreation)
  }

  @Override
  public void endElement(String uri, String localName, String qName)
      throws SAXException {
    //Nothing to do.
  }
}
