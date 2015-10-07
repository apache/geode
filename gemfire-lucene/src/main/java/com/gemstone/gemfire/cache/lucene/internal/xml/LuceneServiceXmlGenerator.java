package com.gemstone.gemfire.cache.lucene.internal.xml;

import org.xml.sax.SAXException;

import com.gemstone.gemfire.cache.Cache;
import com.gemstone.gemfire.internal.cache.xmlcache.CacheXmlGenerator;
import com.gemstone.gemfire.internal.cache.xmlcache.XmlGenerator;

public final class LuceneServiceXmlGenerator implements XmlGenerator<Cache> {
  @Override
  public String getNamspaceUri() {
    return LuceneXmlConstants.NAMESPACE;
  }

  @Override
  public void generate(CacheXmlGenerator cacheXmlGenerator)
      throws SAXException {
    //Nothing to to the xml at the service level at the moment.
  }
}