package com.gemstone.gemfire.cache.lucene.internal;

import org.apache.logging.log4j.Logger;
import org.apache.lucene.analysis.core.SimpleAnalyzer;
import org.apache.lucene.queryparser.classic.MultiFieldQueryParser;
import org.apache.lucene.queryparser.classic.ParseException;
import org.apache.lucene.queryparser.classic.QueryParser;
import org.apache.lucene.search.Query;

import com.gemstone.gemfire.cache.lucene.LuceneIndex;
import com.gemstone.gemfire.cache.lucene.LuceneQueryProvider;
import com.gemstone.gemfire.cache.query.QueryException;
import com.gemstone.gemfire.internal.logging.LogService;

public class StringQueryProvider implements LuceneQueryProvider {
  private static final long serialVersionUID = 1L;

  private Query luceneQuery;
  private static final Logger logger = LogService.getLogger();
  
  final QueryParser parser;
  final String query;

  public StringQueryProvider(String query) {
    this(null, query);
  }

  public StringQueryProvider(LuceneIndex index, String query) {
    String[] fields = null;
    
    this.query = query;
    if (index != null) {
      fields = index.getFieldNames();
    }
    parser = new MultiFieldQueryParser(fields, new SimpleAnalyzer());
  }
  
  @Override
  public synchronized Query getQuery() throws QueryException {
    if (luceneQuery == null) {
      try {
        luceneQuery = parser.parse(query);
      } catch (ParseException e) {
        logger.debug("Malformed lucene query: " + query, e);
        throw new QueryException(e);
      }
    }
    return luceneQuery;
  }
}
