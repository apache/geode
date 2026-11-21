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

package org.apache.geode.cache.lucene.internal.vector;

import java.util.Locale;
import java.util.Map;
import java.util.function.Predicate;

import org.apache.lucene.index.Term;
import org.apache.lucene.search.BooleanClause;
import org.apache.lucene.search.BooleanQuery;
import org.apache.lucene.search.MatchAllDocsQuery;
import org.apache.lucene.search.MatchNoDocsQuery;
import org.apache.lucene.search.PhraseQuery;
import org.apache.lucene.search.PointRangeQuery;
import org.apache.lucene.search.PrefixQuery;
import org.apache.lucene.search.Query;
import org.apache.lucene.search.TermQuery;
import org.apache.lucene.util.NumericUtils;

/**
 * Evaluates structured metadata equality predicates and parsed Lucene filter {@link Query} ASTs
 * against {@link Tier0VectorDeltaBuffer} entry metadata maps so that Tier-0 exact scans enforce
 * the same filter predicates pushed into Lucene {@code KnnFloatVectorQuery}.
 */
public final class Tier0MetadataMatcher {

  private static final String POINT_SUFFIX = "_point";

  private Tier0MetadataMatcher() {}

  /**
   * Builds a combined metadata {@link Predicate} from structured key=value filters and an optional
   * parsed Lucene filter {@link Query}. Returns {@code null} if no filter is specified.
   */
  public static Predicate<Map<String, Object>> buildPredicate(
      Map<String, String> metadataEqualityFilters, Query parsedFilterQuery) {
    boolean hasEquality = metadataEqualityFilters != null && !metadataEqualityFilters.isEmpty();
    boolean hasQuery = parsedFilterQuery != null;
    if (!hasEquality && !hasQuery) {
      return null;
    }

    return metadata -> {
      Map<String, Object> safeMeta = metadata == null ? Map.of() : metadata;
      if (hasEquality) {
        for (Map.Entry<String, String> filter : metadataEqualityFilters.entrySet()) {
          if (!matchesEquality(safeMeta, filter.getKey(), filter.getValue())) {
            return false;
          }
        }
      }
      if (hasQuery && !matchesQuery(parsedFilterQuery, safeMeta)) {
        return false;
      }
      return true;
    };
  }

  /**
   * Evaluates a parsed Lucene {@link Query} against an entry's metadata map.
   */
  public static boolean matchesQuery(Query query, Map<String, Object> metadata) {
    if (query == null || query instanceof MatchAllDocsQuery) {
      return true;
    }
    if (query instanceof MatchNoDocsQuery) {
      return false;
    }
    Map<String, Object> safeMeta = metadata == null ? Map.of() : metadata;

    if (query instanceof TermQuery) {
      Term term = ((TermQuery) query).getTerm();
      return matchesTerm(safeMeta, term.field(), term.text());
    }

    if (query instanceof PrefixQuery) {
      Term prefix = ((PrefixQuery) query).getPrefix();
      Object val = safeMeta.get(prefix.field());
      if (val == null) {
        return false;
      }
      return val.toString().toLowerCase(Locale.ROOT)
          .startsWith(prefix.text().toLowerCase(Locale.ROOT));
    }

    if (query instanceof PhraseQuery) {
      PhraseQuery pq = (PhraseQuery) query;
      Term[] terms = pq.getTerms();
      if (terms.length == 0) {
        return true;
      }
      String field = terms[0].field();
      Object val = safeMeta.get(field);
      if (val == null) {
        return false;
      }
      String haystack = val.toString().toLowerCase(Locale.ROOT);
      StringBuilder phrase = new StringBuilder();
      for (int i = 0; i < terms.length; i++) {
        if (i > 0) {
          phrase.append(' ');
        }
        phrase.append(terms[i].text().toLowerCase(Locale.ROOT));
      }
      return haystack.contains(phrase.toString());
    }

    if (query instanceof PointRangeQuery) {
      return matchesPointRange((PointRangeQuery) query, safeMeta);
    }

    if (query instanceof BooleanQuery) {
      return matchesBoolean((BooleanQuery) query, safeMeta);
    }

    return true;
  }

  private static boolean matchesBoolean(BooleanQuery bq, Map<String, Object> metadata) {
    int shouldCount = 0;
    int shouldMatched = 0;
    boolean hasRequired = false;

    for (BooleanClause clause : bq.clauses()) {
      BooleanClause.Occur occur = clause.getOccur();
      boolean matched = matchesQuery(clause.getQuery(), metadata);
      switch (occur) {
        case MUST:
        case FILTER:
          hasRequired = true;
          if (!matched) {
            return false;
          }
          break;
        case MUST_NOT:
          if (matched) {
            return false;
          }
          break;
        case SHOULD:
          shouldCount++;
          if (matched) {
            shouldMatched++;
          }
          break;
        default:
          break;
      }
    }

    if (shouldCount > 0) {
      int minShould = bq.getMinimumNumberShouldMatch();
      if (minShould <= 0 && !hasRequired) {
        minShould = 1;
      }
      return shouldMatched >= minShould;
    }
    return true;
  }

  private static boolean matchesEquality(Map<String, Object> metadata, String field,
      String expected) {
    if (expected == null) {
      return false;
    }
    Object actual = metadata.get(field);
    if (actual == null) {
      return false;
    }
    return expected.equals(actual.toString());
  }

  private static boolean matchesTerm(Map<String, Object> metadata, String field, String termText) {
    Object actual = metadata.get(field);
    if (actual == null) {
      return false;
    }
    String actualStr = actual.toString();
    if (actualStr.equals(termText) || actualStr.equalsIgnoreCase(termText)) {
      return true;
    }
    // Support tokenized text metadata fields (e.g. "red cotton shirt" matching "cotton")
    String lowerTerm = termText.toLowerCase(Locale.ROOT);
    for (String token : actualStr.toLowerCase(Locale.ROOT).split("[^a-z0-9_]+")) {
      if (token.equals(lowerTerm)) {
        return true;
      }
    }
    return false;
  }

  private static boolean matchesPointRange(PointRangeQuery prq, Map<String, Object> metadata) {
    if (prq.getNumDims() != 1) {
      return true;
    }
    String field = prq.getField();
    Object raw = metadata.get(field);
    if (raw == null && field.endsWith(POINT_SUFFIX)) {
      raw = metadata.get(field.substring(0, field.length() - POINT_SUFFIX.length()));
    }
    if (!(raw instanceof Number)) {
      return false;
    }
    Number number = (Number) raw;
    byte[] lower = prq.getLowerPoint();
    byte[] upper = prq.getUpperPoint();
    int bytesPerDim = prq.getBytesPerDim();

    if (bytesPerDim == Integer.BYTES) {
      if (raw instanceof Float) {
        float low = NumericUtils.sortableIntToFloat(NumericUtils.sortableBytesToInt(lower, 0));
        float high = NumericUtils.sortableIntToFloat(NumericUtils.sortableBytesToInt(upper, 0));
        float val = number.floatValue();
        return Float.compare(val, low) >= 0 && Float.compare(val, high) <= 0;
      }
      int low = NumericUtils.sortableBytesToInt(lower, 0);
      int high = NumericUtils.sortableBytesToInt(upper, 0);
      int val = number.intValue();
      return val >= low && val <= high;
    }
    if (bytesPerDim == Long.BYTES) {
      if (raw instanceof Double) {
        double low = NumericUtils.sortableLongToDouble(NumericUtils.sortableBytesToLong(lower, 0));
        double high = NumericUtils.sortableLongToDouble(NumericUtils.sortableBytesToLong(upper, 0));
        double val = number.doubleValue();
        return Double.compare(val, low) >= 0 && Double.compare(val, high) <= 0;
      }
      long low = NumericUtils.sortableBytesToLong(lower, 0);
      long high = NumericUtils.sortableBytesToLong(upper, 0);
      long val = number.longValue();
      return val >= low && val <= high;
    }
    return true;
  }
}
