/*
 * Licensed to the Apache Software Foundation (ASF) under one or more contributor license
 * agreements. See the NOTICE file distributed with this work for additional information regarding
 * copyright ownership. The ASF licenses this file to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance with the License. You may obtain a
 * copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software distributed under the License
 * is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express
 * or implied. See the License for the specific language governing permissions and limitations under
 * the License.
 */

package org.apache.geode.cache.lucene.internal;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.logging.log4j.Logger;
import org.apache.lucene.queryparser.flexible.core.QueryNodeException;
import org.apache.lucene.queryparser.flexible.standard.StandardQueryParser;
import org.apache.lucene.search.Query;

import org.apache.geode.DataSerializer;
import org.apache.geode.cache.lucene.LuceneIndex;
import org.apache.geode.cache.lucene.LuceneQueryException;
import org.apache.geode.cache.lucene.LuceneQueryProvider;
import org.apache.geode.internal.DataSerializableFixedID;
import org.apache.geode.internal.Version;
import org.apache.geode.internal.logging.LogService;

/**
 * Constructs a Lucene Query object by parsing a search string. The class uses
 * {@link StandardQueryParser}. It sets searchable fields in a {@link LuceneIndex} as default
 * fields.
 */
public class StringQueryProvider implements LuceneQueryProvider, DataSerializableFixedID {

  private static final long serialVersionUID = 1L;

  private static final Logger logger = LogService.getLogger();

  // the following members hold input data and needs to be sent on wire
  private String query;

  // the following members hold derived objects and need not be serialized
  private transient Query luceneQuery;

  private String defaultField;

  public StringQueryProvider() {
    this(null, null);
  }

  public StringQueryProvider(String query, String defaultField) {
    this.query = query;
    this.defaultField = defaultField;
  }

  @Override
  public synchronized Query getQuery(LuceneIndex index) throws LuceneQueryException {
    if (luceneQuery == null) {
      LuceneIndexImpl indexImpl = (LuceneIndexImpl) index;
      StandardQueryParser parser = new StandardQueryParser(indexImpl.getAnalyzer());
      parser.setAllowLeadingWildcard(true);
      try {
        luceneQuery = parser.parse(query, defaultField);
        if (logger.isDebugEnabled()) {
          logger.debug("User query " + query + " is parsed to be: " + luceneQuery);
        }
      } catch (QueryNodeException e) {
        logger.warn("Caught the following exception attempting parse query '" + query + "': ", e);
        throw new LuceneQueryException(
            String.format("Parsing query %s failed due to: %s",
                "'" + query + "'", e.getMessage()));
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
    DataSerializer.writeString(defaultField, out);
  }

  @Override
  public void fromData(DataInput in) throws IOException, ClassNotFoundException {
    query = DataSerializer.readString(in);
    defaultField = DataSerializer.readString(in);
  }
}
