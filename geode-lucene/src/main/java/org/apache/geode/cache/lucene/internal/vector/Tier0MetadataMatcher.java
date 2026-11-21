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

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.function.Predicate;

import org.apache.lucene.document.DoublePoint;
import org.apache.lucene.document.FloatPoint;
import org.apache.lucene.document.IntPoint;
import org.apache.lucene.document.LongPoint;
import org.apache.lucene.index.Term;
import org.apache.lucene.search.AutomatonQuery;
import org.apache.lucene.search.BooleanClause;
import org.apache.lucene.search.BooleanQuery;
import org.apache.lucene.search.BoostQuery;
import org.apache.lucene.search.ConstantScoreQuery;
import org.apache.lucene.search.MatchAllDocsQuery;
import org.apache.lucene.search.MatchNoDocsQuery;
import org.apache.lucene.search.PhraseQuery;
import org.apache.lucene.search.PointRangeQuery;
import org.apache.lucene.search.PrefixQuery;
import org.apache.lucene.search.Query;
import org.apache.lucene.search.TermInSetQuery;
import org.apache.lucene.search.TermQuery;
import org.apache.lucene.search.TermRangeQuery;
import org.apache.lucene.util.BytesRef;
import org.apache.lucene.util.BytesRefIterator;
import org.apache.lucene.util.automaton.Automaton;
import org.apache.lucene.util.automaton.ByteRunAutomaton;
import org.apache.lucene.util.automaton.CharacterRunAutomaton;

/**
 * Evaluates structured metadata equality predicates and parsed Lucene filter {@link Query} ASTs
 * against {@link Tier0VectorDeltaBuffer} entry metadata maps so that Tier-0 exact scans enforce
 * the same filter semantics pushed into Lucene {@code KnnFloatVectorQuery}.
 *
 * <p>
 * Fails <strong>closed</strong> ({@code false}) on any unsupported Lucene {@link Query} subclass
 * or multi-dimensional/unrecognized {@link PointRangeQuery} so Tier-0 scans never leak
 * false-positive filter bypasses.
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
   *
   * <p>
   * Unhandled query subclasses fail closed ({@code false}) rather than failing open.
   */
  public static boolean matchesQuery(Query query, Map<String, Object> metadata) {
    if (query == null || query instanceof MatchAllDocsQuery) {
      return true;
    }
    if (query instanceof MatchNoDocsQuery) {
      return false;
    }
    if (query instanceof BoostQuery) {
      return matchesQuery(((BoostQuery) query).getQuery(), metadata);
    }
    if (query instanceof ConstantScoreQuery) {
      return matchesQuery(((ConstantScoreQuery) query).getQuery(), metadata);
    }

    Map<String, Object> safeMeta = metadata == null ? Map.of() : metadata;

    if (query instanceof TermQuery) {
      Term term = ((TermQuery) query).getTerm();
      return matchesTerm(safeMeta, term.field(), term.text());
    }

    if (query instanceof TermRangeQuery) {
      return matchesTermRange((TermRangeQuery) query, safeMeta);
    }

    if (query instanceof PrefixQuery) {
      Term prefix = ((PrefixQuery) query).getPrefix();
      return matchesPrefix(safeMeta, prefix.field(), prefix.text());
    }

    if (query instanceof AutomatonQuery) {
      // Covers WildcardQuery, RegexpQuery, and any other AutomatonQuery subclasses
      return matchesAutomaton((AutomatonQuery) query, safeMeta);
    }

    if (query instanceof TermInSetQuery) {
      return matchesTermInSet((TermInSetQuery) query, safeMeta);
    }

    if (query instanceof PhraseQuery) {
      return matchesPhrase((PhraseQuery) query, safeMeta);
    }

    if (query instanceof PointRangeQuery) {
      return matchesPointRange((PointRangeQuery) query, safeMeta);
    }

    if (query instanceof BooleanQuery) {
      return matchesBoolean((BooleanQuery) query, safeMeta);
    }

    // Fail closed on unsupported Lucene query subclasses
    return false;
  }

  /**
   * Evaluates a {@link BooleanQuery} matching Lucene {@code IndexSearcher} semantics: empty
   * queries and pure-{@code MUST_NOT} queries (no positive {@code MUST}/{@code FILTER}/
   * {@code SHOULD} clause) match zero documents and return {@code false}.
   */
  private static boolean matchesBoolean(BooleanQuery bq, Map<String, Object> metadata) {
    List<BooleanClause> clauses = bq.clauses();
    if (clauses.isEmpty()) {
      return false;
    }

    int shouldCount = 0;
    int shouldMatched = 0;
    boolean hasRequired = false;

    for (BooleanClause clause : clauses) {
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
          return false;
      }
    }

    // Pure MUST_NOT BooleanQuery (no positive MUST/FILTER/SHOULD clauses) matches 0 docs in Lucene
    if (!hasRequired && shouldCount == 0) {
      return false;
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
    String lowerTerm = termText.toLowerCase(Locale.ROOT);
    for (String token : tokenize(actualStr)) {
      if (token.equals(lowerTerm)) {
        return true;
      }
    }
    return false;
  }

  private static boolean matchesPrefix(Map<String, Object> metadata, String field,
      String prefixText) {
    Object actual = metadata.get(field);
    if (actual == null) {
      return false;
    }
    String actualStr = actual.toString();
    String lowerPrefix = prefixText.toLowerCase(Locale.ROOT);
    if (actualStr.startsWith(prefixText)
        || actualStr.toLowerCase(Locale.ROOT).startsWith(lowerPrefix)) {
      return true;
    }
    for (String token : tokenize(actualStr)) {
      if (token.startsWith(lowerPrefix)) {
        return true;
      }
    }
    return false;
  }

  private static boolean matchesTermRange(TermRangeQuery trq, Map<String, Object> metadata) {
    Object actual = metadata.get(trq.getField());
    if (actual == null) {
      return false;
    }
    BytesRef lower = trq.getLowerTerm();
    BytesRef upper = trq.getUpperTerm();
    boolean incLower = trq.includesLower();
    boolean incUpper = trq.includesUpper();

    String actualStr = actual.toString();
    if (inBytesRange(new BytesRef(actualStr), lower, upper, incLower, incUpper)) {
      return true;
    }
    String lowerCase = actualStr.toLowerCase(Locale.ROOT);
    if (!lowerCase.equals(actualStr)
        && inBytesRange(new BytesRef(lowerCase), lower, upper, incLower, incUpper)) {
      return true;
    }
    for (String token : tokenize(actualStr)) {
      if (inBytesRange(new BytesRef(token), lower, upper, incLower, incUpper)) {
        return true;
      }
    }
    return false;
  }

  private static boolean inBytesRange(BytesRef candidate, BytesRef lower, BytesRef upper,
      boolean incLower, boolean incUpper) {
    if (lower != null) {
      int cmp = candidate.compareTo(lower);
      if (incLower ? cmp < 0 : cmp <= 0) {
        return false;
      }
    }
    if (upper != null) {
      int cmp = candidate.compareTo(upper);
      if (incUpper ? cmp > 0 : cmp >= 0) {
        return false;
      }
    }
    return true;
  }

  private static boolean matchesAutomaton(AutomatonQuery aq, Map<String, Object> metadata) {
    Object actual = metadata.get(aq.getField());
    if (actual == null) {
      return false;
    }
    Automaton automaton = aq.getAutomaton();
    if (automaton == null) {
      return false;
    }

    String actualStr = actual.toString();
    List<String> candidates = candidateTerms(actualStr);

    if (aq.isAutomatonBinary()) {
      ByteRunAutomaton runner = new ByteRunAutomaton(automaton);
      for (String candidate : candidates) {
        byte[] bytes = candidate.getBytes(StandardCharsets.UTF_8);
        if (runner.run(bytes, 0, bytes.length)) {
          return true;
        }
      }
      return false;
    }

    CharacterRunAutomaton runner = new CharacterRunAutomaton(automaton);
    for (String candidate : candidates) {
      if (runner.run(candidate)) {
        return true;
      }
    }
    return false;
  }

  private static boolean matchesTermInSet(TermInSetQuery tq, Map<String, Object> metadata) {
    String field = tq.getField();
    if (metadata.get(field) == null) {
      return false;
    }
    try {
      BytesRefIterator iter = tq.getBytesRefIterator();
      BytesRef term;
      while ((term = iter.next()) != null) {
        if (matchesTerm(metadata, field, term.utf8ToString())) {
          return true;
        }
      }
      return false;
    } catch (IOException e) {
      return false;
    }
  }

  private static boolean matchesPhrase(PhraseQuery pq, Map<String, Object> metadata) {
    Term[] terms = pq.getTerms();
    if (terms.length == 0) {
      return false;
    }
    String field = terms[0].field();
    Object val = metadata.get(field);
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

  /**
   * Evaluates 1D Lucene {@link PointRangeQuery} instances by identifying the exact Lucene point
   * encoder ({@link IntPoint}, {@link FloatPoint}, {@link LongPoint}, {@link DoublePoint}) via
   * enclosing class rather than guessing from the metadata value's runtime type. Fails closed
   * ({@code false}) for multi-dimensional or unrecognized point encodings.
   */
  private static boolean matchesPointRange(PointRangeQuery prq, Map<String, Object> metadata) {
    if (prq.getNumDims() != 1) {
      return false;
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
    Class<?> enclosing = prq.getClass().getEnclosingClass();

    if (enclosing == IntPoint.class && bytesPerDim == Integer.BYTES) {
      int low = IntPoint.decodeDimension(lower, 0);
      int high = IntPoint.decodeDimension(upper, 0);
      return isNumberInIntegralRange(number, low, high);
    }
    if (enclosing == FloatPoint.class && bytesPerDim == Float.BYTES) {
      float low = FloatPoint.decodeDimension(lower, 0);
      float high = FloatPoint.decodeDimension(upper, 0);
      return isNumberInFloatingRange(number, low, high);
    }
    if (enclosing == LongPoint.class && bytesPerDim == Long.BYTES) {
      long low = LongPoint.decodeDimension(lower, 0);
      long high = LongPoint.decodeDimension(upper, 0);
      return isNumberInIntegralRange(number, low, high);
    }
    if (enclosing == DoublePoint.class && bytesPerDim == Double.BYTES) {
      double low = DoublePoint.decodeDimension(lower, 0);
      double high = DoublePoint.decodeDimension(upper, 0);
      return isNumberInFloatingRange(number, low, high);
    }

    // Fail closed for unknown or non-standard point encodings
    return false;
  }

  private static boolean isNumberInIntegralRange(Number number, long low, long high) {
    if (number instanceof Float || number instanceof Double) {
      double val = number.doubleValue();
      if (Double.isNaN(val)) {
        return false;
      }
      return Double.compare(val, (double) low) >= 0 && Double.compare(val, (double) high) <= 0;
    }
    long val = number.longValue();
    return val >= low && val <= high;
  }

  private static boolean isNumberInFloatingRange(Number number, double low, double high) {
    double val = number.doubleValue();
    if (Double.isNaN(val)) {
      return false;
    }
    return Double.compare(val, low) >= 0 && Double.compare(val, high) <= 0;
  }

  private static List<String> candidateTerms(String actualStr) {
    List<String> list = new ArrayList<>();
    list.add(actualStr);
    String lower = actualStr.toLowerCase(Locale.ROOT);
    if (!lower.equals(actualStr)) {
      list.add(lower);
    }
    for (String token : tokenize(actualStr)) {
      if (!token.isEmpty() && !token.equals(lower) && !token.equals(actualStr)) {
        list.add(token);
      }
    }
    return list;
  }

  private static String[] tokenize(String text) {
    return text.toLowerCase(Locale.ROOT).split("[^a-z0-9_]+");
  }
}
