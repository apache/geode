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

import static org.assertj.core.api.Assertions.assertThat;

import java.io.IOException;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.apache.lucene.analysis.standard.StandardAnalyzer;
import org.apache.lucene.document.Document;
import org.apache.lucene.document.DoublePoint;
import org.apache.lucene.document.Field;
import org.apache.lucene.document.FloatPoint;
import org.apache.lucene.document.IntPoint;
import org.apache.lucene.document.LongPoint;
import org.apache.lucene.document.StringField;
import org.apache.lucene.index.DirectoryReader;
import org.apache.lucene.index.IndexWriter;
import org.apache.lucene.index.IndexWriterConfig;
import org.apache.lucene.index.Term;
import org.apache.lucene.search.BooleanClause;
import org.apache.lucene.search.BooleanQuery;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.search.MatchAllDocsQuery;
import org.apache.lucene.search.MatchNoDocsQuery;
import org.apache.lucene.search.PrefixQuery;
import org.apache.lucene.search.Query;
import org.apache.lucene.search.QueryVisitor;
import org.apache.lucene.search.RegexpQuery;
import org.apache.lucene.search.ScoreDoc;
import org.apache.lucene.search.TermInSetQuery;
import org.apache.lucene.search.TermQuery;
import org.apache.lucene.search.TermRangeQuery;
import org.apache.lucene.search.TopDocs;
import org.apache.lucene.search.WildcardQuery;
import org.apache.lucene.store.ByteBuffersDirectory;
import org.apache.lucene.util.BytesRef;
import org.junit.Test;
import org.junit.experimental.categories.Category;

import org.apache.geode.cache.lucene.internal.repository.serializer.SerializerUtil;
import org.apache.geode.test.junit.categories.LuceneTest;

/**
 * Lucene {@link IndexSearcher} parity tests for {@link Tier0MetadataMatcher}, covering
 * {@code PR2-B2} (TermRange, Wildcard, Prefix, Regexp, TermInSet, MatchNoDocs, fail-closed on
 * unsupported queries, cross-type PointRangeQuery decoding) and {@code PR2-B5} (pure-MUST_NOT
 * BooleanQuery semantics).
 */
@Category({LuceneTest.class})
public class Tier0MetadataMatcherParityJUnitTest {

  @Test
  public void matchesLuceneIndexSearcherAcrossAllSupportedQueryTypes() throws IOException {
    Map<String, Map<String, Object>> docs = new LinkedHashMap<>();
    docs.put("d1", Map.of("category", "books", "sku", "bk100", "code", "alpha"));
    docs.put("d2", Map.of("category", "boot", "sku", "bk250", "code", "beta"));
    docs.put("d3", Map.of("category", "music", "sku", "mu500", "code", "gamma"));
    docs.put("d4", Map.of("category", "spam", "sku", "sp999", "code", "delta"));

    try (ByteBuffersDirectory dir = new ByteBuffersDirectory();
        IndexWriter writer = new IndexWriter(dir, new IndexWriterConfig(new StandardAnalyzer()))) {
      for (Map.Entry<String, Map<String, Object>> entry : docs.entrySet()) {
        Document doc = new Document();
        SerializerUtil.addKey(entry.getKey(), doc);
        for (Map.Entry<String, Object> f : entry.getValue().entrySet()) {
          doc.add(new StringField(f.getKey(), f.getValue().toString(), Field.Store.NO));
        }
        writer.addDocument(doc);
      }
      writer.commit();

      try (DirectoryReader reader = DirectoryReader.open(dir)) {
        IndexSearcher searcher = new IndexSearcher(reader);

        List<Query> queries = List.of(
            TermRangeQuery.newStringRange("sku", "bk100", "bk300", true, true),
            TermRangeQuery.newStringRange("sku", "bk100", "bk250", false, false),
            new WildcardQuery(new Term("category", "boo*")),
            new WildcardQuery(new Term("category", "boo?")),
            new PrefixQuery(new Term("sku", "bk")),
            new RegexpQuery(new Term("code", "(alpha|gamma)")),
            new TermInSetQuery("category", new BytesRef("books"), new BytesRef("music")),
            new MatchNoDocsQuery(),
            new MatchAllDocsQuery());

        for (Query q : queries) {
          Set<String> luceneMatches = searchKeys(searcher, q);
          Set<String> tier0Matches = new HashSet<>();
          for (Map.Entry<String, Map<String, Object>> e : docs.entrySet()) {
            if (Tier0MetadataMatcher.matchesQuery(q, e.getValue())) {
              tier0Matches.add(e.getKey());
            }
          }
          assertThat(tier0Matches)
              .as("Tier0MetadataMatcher parity with Lucene IndexSearcher for query: %s", q)
              .containsExactlyInAnyOrderElementsOf(luceneMatches);
        }
      }
    }
  }

  @Test
  public void pureMustNotBooleanQueryMatchesZeroDocsLikeLuceneIndexSearcher() throws IOException {
    Map<String, Map<String, Object>> docs = new LinkedHashMap<>();
    docs.put("d1", Map.of("category", "books"));
    docs.put("d2", Map.of("category", "spam"));
    docs.put("d3", Map.of("category", "music"));

    try (ByteBuffersDirectory dir = new ByteBuffersDirectory();
        IndexWriter writer = new IndexWriter(dir, new IndexWriterConfig(new StandardAnalyzer()))) {
      for (Map.Entry<String, Map<String, Object>> entry : docs.entrySet()) {
        Document doc = new Document();
        SerializerUtil.addKey(entry.getKey(), doc);
        doc.add(new StringField("category", entry.getValue().get("category").toString(),
            Field.Store.NO));
        writer.addDocument(doc);
      }
      writer.commit();

      try (DirectoryReader reader = DirectoryReader.open(dir)) {
        IndexSearcher searcher = new IndexSearcher(reader);

        // Pure MUST_NOT: -category:spam -> Lucene returns 0 docs
        BooleanQuery pureMustNot = new BooleanQuery.Builder()
            .add(new TermQuery(new Term("category", "spam")), BooleanClause.Occur.MUST_NOT)
            .build();
        assertThat(searchKeys(searcher, pureMustNot)).isEmpty();
        for (Map.Entry<String, Map<String, Object>> e : docs.entrySet()) {
          assertThat(Tier0MetadataMatcher.matchesQuery(pureMustNot, e.getValue()))
              .as("Pure MUST_NOT must return false for %s", e.getKey())
              .isFalse();
        }

        // Empty BooleanQuery -> 0 docs
        BooleanQuery emptyBool = new BooleanQuery.Builder().build();
        assertThat(searchKeys(searcher, emptyBool)).isEmpty();
        assertThat(Tier0MetadataMatcher.matchesQuery(emptyBool, docs.get("d1"))).isFalse();

        // MatchAllDocsQuery + MUST_NOT -> matches non-spam docs (d1, d3)
        BooleanQuery matchAllMinusSpam = new BooleanQuery.Builder()
            .add(new MatchAllDocsQuery(), BooleanClause.Occur.FILTER)
            .add(new TermQuery(new Term("category", "spam")), BooleanClause.Occur.MUST_NOT)
            .build();
        Set<String> luceneExpected = searchKeys(searcher, matchAllMinusSpam);
        assertThat(luceneExpected).containsExactlyInAnyOrder("d1", "d3");

        Set<String> tier0Actual = new HashSet<>();
        for (Map.Entry<String, Map<String, Object>> e : docs.entrySet()) {
          if (Tier0MetadataMatcher.matchesQuery(matchAllMinusSpam, e.getValue())) {
            tier0Actual.add(e.getKey());
          }
        }
        assertThat(tier0Actual).containsExactlyInAnyOrderElementsOf(luceneExpected);
      }
    }
  }

  @Test
  public void unsupportedQuerySubclassAndNon1DPointQueryFailClosed() {
    Query unsupported = new Query() {
      @Override
      public String toString(String field) {
        return "CustomUnsupportedQuery";
      }

      @Override
      public void visit(QueryVisitor visitor) {}

      @Override
      public boolean equals(Object obj) {
        return this == obj;
      }

      @Override
      public int hashCode() {
        return 42;
      }
    };

    Map<String, Object> meta = Map.of("category", "books", "price", 25, "coords", 5);
    assertThat(Tier0MetadataMatcher.matchesQuery(unsupported, meta)).isFalse();

    // Multi-dimensional PointRangeQuery must fail closed (false), never open (true)
    Query point2d = IntPoint.newRangeQuery("coords", new int[] {0, 0}, new int[] {100, 100});
    assertThat(Tier0MetadataMatcher.matchesQuery(point2d, meta)).isFalse();
  }

  @Test
  public void crossTypePointRangeQueriesDecodeExactLuceneEncodingAndMatchGroundTruth()
      throws IOException {
    // Store mixed numeric metadata types (e.g. Integer metadata queried by FloatPoint /
    // DoublePoint,
    // Long metadata queried by IntPoint / DoublePoint, Double metadata queried by FloatPoint)
    Map<String, Map<String, Object>> docs = new LinkedHashMap<>();
    docs.put("low", Map.of("iVal", 5L, "fVal", 5, "lVal", 5, "dVal", 5.0f));
    docs.put("mid", Map.of("iVal", 25L, "fVal", 25, "lVal", 25, "dVal", 25.5f));
    docs.put("high", Map.of("iVal", 100L, "fVal", 100, "lVal", 100, "dVal", 100.0f));
    docs.put("nan", Map.of("iVal", Double.NaN, "fVal", Float.NaN, "lVal", Double.NaN,
        "dVal", Double.NaN));

    try (ByteBuffersDirectory dir = new ByteBuffersDirectory();
        IndexWriter writer = new IndexWriter(dir, new IndexWriterConfig(new StandardAnalyzer()))) {
      Map<String, int[]> intVals = Map.of("low", new int[] {5}, "mid", new int[] {25},
          "high", new int[] {100});
      Map<String, float[]> floatVals = Map.of("low", new float[] {5f}, "mid", new float[] {25f},
          "high", new float[] {100f});
      Map<String, long[]> longVals = Map.of("low", new long[] {5L}, "mid", new long[] {25L},
          "high", new long[] {100L});
      Map<String, double[]> doubleVals = Map.of("low", new double[] {5.0},
          "mid", new double[] {25.5}, "high", new double[] {100.0});

      for (String key : List.of("low", "mid", "high")) {
        Document doc = new Document();
        SerializerUtil.addKey(key, doc);
        doc.add(new IntPoint("iVal_point", intVals.get(key)[0]));
        doc.add(new FloatPoint("fVal_point", floatVals.get(key)[0]));
        doc.add(new LongPoint("lVal_point", longVals.get(key)[0]));
        doc.add(new DoublePoint("dVal_point", doubleVals.get(key)[0]));
        writer.addDocument(doc);
      }
      writer.commit();

      try (DirectoryReader reader = DirectoryReader.open(dir)) {
        IndexSearcher searcher = new IndexSearcher(reader);

        List<Query> pointQueries = List.of(
            IntPoint.newRangeQuery("iVal_point", 10, 50),
            FloatPoint.newRangeQuery("fVal_point", 10.0f, 50.0f),
            LongPoint.newRangeQuery("lVal_point", 10L, 50L),
            DoublePoint.newRangeQuery("dVal_point", 10.0, 50.0));

        for (Query q : pointQueries) {
          Set<String> luceneMatches = searchKeys(searcher, q);
          assertThat(luceneMatches).containsExactly("mid");

          Set<String> tier0Matches = new HashSet<>();
          for (Map.Entry<String, Map<String, Object>> e : docs.entrySet()) {
            if (Tier0MetadataMatcher.matchesQuery(q, e.getValue())) {
              tier0Matches.add(e.getKey());
            }
          }
          assertThat(tier0Matches)
              .as("Cross-type PointRangeQuery parity for %s", q)
              .containsExactlyInAnyOrderElementsOf(luceneMatches);
        }
      }
    }
  }

  private static Set<String> searchKeys(IndexSearcher searcher, Query query) throws IOException {
    TopDocs td = searcher.search(query, 100);
    Set<String> keys = new HashSet<>();
    for (ScoreDoc sd : td.scoreDocs) {
      keys.add((String) SerializerUtil.getKey(searcher.storedFields().document(sd.doc)));
    }
    return keys;
  }
}
