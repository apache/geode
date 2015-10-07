package com.gemstone.gemfire.cache.lucene.internal.xml;

import static com.gemstone.gemfire.cache.lucene.internal.xml.LuceneXmlConstants.*;

import org.xml.sax.ContentHandler;
import org.xml.sax.SAXException;
import org.xml.sax.helpers.AttributesImpl;

import com.gemstone.gemfire.cache.Region;
import com.gemstone.gemfire.cache.lucene.LuceneIndex;
import com.gemstone.gemfire.internal.cache.xmlcache.CacheXmlGenerator;
import com.gemstone.gemfire.internal.cache.xmlcache.XmlGenerator;
import com.gemstone.gemfire.internal.cache.xmlcache.XmlGeneratorUtils;

public class LuceneIndexXmlGenerator implements XmlGenerator<Region<?, ?>> {
  private final LuceneIndex index;

  public LuceneIndexXmlGenerator(LuceneIndex index) {
    this.index = index;
  }

  @Override
  public String getNamspaceUri() {
    return NAMESPACE;
  }

  @Override
  public void generate(CacheXmlGenerator cacheXmlGenerator)
      throws SAXException {
    final ContentHandler handler = cacheXmlGenerator.getContentHandler();

    handler.startPrefixMapping(PREFIX, NAMESPACE);

    AttributesImpl attr = new AttributesImpl();
    //TODO - should the type be xs:string ?
    XmlGeneratorUtils.addAttribute(attr, NAME, index.getName());
    XmlGeneratorUtils.addAttribute(attr, FIELDS, String.join(",", index.getFieldNames()));
    XmlGeneratorUtils.emptyElement(cacheXmlGenerator.getContentHandler(), PREFIX, INDEX, attr);
  }

}
