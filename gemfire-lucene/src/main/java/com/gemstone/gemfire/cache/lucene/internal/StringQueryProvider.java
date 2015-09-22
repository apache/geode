package com.gemstone.gemfire.cache.lucene.internal;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.logging.log4j.Logger;
import org.apache.lucene.analysis.core.SimpleAnalyzer;
import org.apache.lucene.queryparser.classic.MultiFieldQueryParser;
import org.apache.lucene.queryparser.classic.ParseException;
import org.apache.lucene.search.Query;

import com.gemstone.gemfire.DataSerializer;
import com.gemstone.gemfire.cache.lucene.LuceneIndex;
import com.gemstone.gemfire.cache.lucene.LuceneQueryProvider;
import com.gemstone.gemfire.cache.query.QueryException;
import com.gemstone.gemfire.internal.DataSerializableFixedID;
import com.gemstone.gemfire.internal.Version;
import com.gemstone.gemfire.internal.logging.LogService;

/**
 * Constructs a Lucene Query object by parsing a search string. The class uses {@link MultiFieldQueryParser}. It sets
 * searchable fields in a {@link LuceneIndex} as default fields.
 */
public class StringQueryProvider implements LuceneQueryProvider, DataSerializableFixedID {
  private static final long serialVersionUID = 1L;

  private static final Logger logger = LogService.getLogger();

  // the following members hold input data and needs to be sent on wire
  private String query;
  private String indexName;
  private String regionPath;

  // the following members hold derived objects and need not be serialized
  private Query luceneQuery;
  private LuceneIndex index;

  public StringQueryProvider() {
    this(null, null);
  }

  public StringQueryProvider(LuceneIndex index, String query) {
    this.query = query;
    this.index = index;
    if (index != null) {
      this.indexName = index.getName();
      this.regionPath = index.getRegionPath();
    }
  }

  @Override
  public synchronized Query getQuery() throws QueryException {
    if (luceneQuery == null) {
      String[] fields = null;
      if (index != null) {
        fields = index.getFieldNames();
      } else {
        // TODO get index from lucene query service
      }

      MultiFieldQueryParser parser = new MultiFieldQueryParser(fields, new SimpleAnalyzer());
      try {
        luceneQuery = parser.parse(query);
      } catch (ParseException e) {
        logger.debug("Malformed lucene query: " + query, e);
        throw new QueryException(e);
      }
    }
    return luceneQuery;
  }

  /**
   * @return the query string used to construct this query provider
   */
  public String getQueryString() {
    return query;
  }

  /**
   * @return name of the index on which this query will be executed
   */
  public String getIndexName() {
    return indexName;
  }

  /**
   * @return path of the region on which this query will be executed
   */
  public String getRegionPath() {
    return regionPath;
  }

  @Override
  public Version[] getSerializationVersions() {
    return null;
  }

  @Override
  public int getDSFID() {
    return LUCENE_STRING_QUERY_PROVIDER;
  }

  @Override
  public void toData(DataOutput out) throws IOException {
    DataSerializer.writeString(query, out);
    DataSerializer.writeString(index.getName(), out);
    DataSerializer.writeString(index.getRegionPath(), out);
  }

  @Override
  public void fromData(DataInput in) throws IOException, ClassNotFoundException {
    query = DataSerializer.readString(in);
    this.indexName = DataSerializer.readString(in);
    this.regionPath = DataSerializer.readString(in);
  }
}
