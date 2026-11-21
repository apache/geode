/*
 * Licensed to the Apache Software Foundation (ASF) under one or more contributor license
 * agreements. See the NOTICE file distributed with this work for additional information regarding
 * copyright ownership. The ASF licenses this file to You under the Apache License, Version 2.0 (the
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

import static org.apache.geode.cache.Region.SEPARATOR;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.io.ByteArrayInputStream;
import java.io.DataInputStream;
import java.lang.reflect.Field;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.Map;

import org.apache.lucene.analysis.standard.StandardAnalyzer;
import org.apache.lucene.codecs.Codec;
import org.apache.lucene.codecs.lucene912.Lucene912Codec;
import org.apache.lucene.codecs.lucene99.Lucene99HnswScalarQuantizedVectorsFormat;
import org.apache.lucene.codecs.lucene99.Lucene99HnswVectorsFormat;
import org.apache.lucene.index.IndexWriter;
import org.apache.lucene.index.IndexWriterConfig;
import org.apache.lucene.index.VectorSimilarityFunction;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.mockito.ArgumentCaptor;

import org.apache.geode.CopyHelper;
import org.apache.geode.DataSerializer;
import org.apache.geode.cache.lucene.LuceneIndexFactory;
import org.apache.geode.cache.lucene.VectorIndexMetadata;
import org.apache.geode.internal.HeapDataOutputStream;
import org.apache.geode.internal.cache.BucketRegion;
import org.apache.geode.internal.cache.GemFireCacheImpl;
import org.apache.geode.internal.serialization.KnownVersion;
import org.apache.geode.test.junit.categories.LuceneTest;

@Category({LuceneTest.class})
public class VectorIndexProfileAndFactoryJUnitTest {

  @Test
  public void factoryFluentAddVectorFieldPassesMetadataToService() {
    LuceneServiceImpl service = mock(LuceneServiceImpl.class);
    LuceneIndexFactory factory = new LuceneIndexFactoryImpl(service);

    factory
        .addField("category")
        .addVectorField("embedding", 128, VectorSimilarityFunction.COSINE, true)
        .addVectorField("dense", 64, VectorSimilarityFunction.EUCLIDEAN, 32, 200, false)
        .create("vecIndex", "documents");

    @SuppressWarnings("unchecked")
    ArgumentCaptor<Map<String, VectorIndexMetadata>> vectorCaptor =
        ArgumentCaptor.forClass(Map.class);
    verify(service).createIndex(eq("vecIndex"), eq("documents"), any(), vectorCaptor.capture(),
        eq(null), eq(false));

    Map<String, VectorIndexMetadata> captured = vectorCaptor.getValue();
    assertThat(captured).containsKeys("embedding", "dense");
    assertThat(captured.get("embedding"))
        .isEqualTo(
            new VectorIndexMetadata("embedding", 128, VectorSimilarityFunction.COSINE, true));
    assertThat(captured.get("dense")).isEqualTo(
        new VectorIndexMetadata("dense", 64, VectorSimilarityFunction.EUCLIDEAN, 32, 200, false));
  }

  @Test
  public void factoryRejectsInvalidVectorFieldDimensionImmediately() {
    LuceneServiceImpl service = mock(LuceneServiceImpl.class);
    LuceneIndexFactory factory = new LuceneIndexFactoryImpl(service);

    assertThatThrownBy(
        () -> factory.addVectorField("embedding", 0, VectorSimilarityFunction.COSINE, false))
            .isInstanceOf(IllegalArgumentException.class)
            .hasMessageContaining("dimension");

    assertThatThrownBy(
        () -> factory.addVectorField("embedding", -10, VectorSimilarityFunction.DOT_PRODUCT, true))
            .isInstanceOf(IllegalArgumentException.class)
            .hasMessageContaining("dimension");
  }

  @Test
  public void serviceAllowsVectorOnlyIndexWithoutTextFields() throws Exception {
    LuceneServiceImpl service = new LuceneServiceImpl();
    GemFireCacheImpl cache = mock(GemFireCacheImpl.class);
    Field f = LuceneServiceImpl.class.getDeclaredField("cache");
    f.setAccessible(true);
    f.set(service, cache);

    service.createIndexFactory()
        .addVectorField("embedding", 128, VectorSimilarityFunction.COSINE, true)
        .create("vecOnlyIdx", "vectors");

    LuceneIndexCreationProfile profile = service.getDefinedIndex("vecOnlyIdx", "/vectors");
    assertThat(profile).isNotNull();
    assertThat(profile.getFieldNames()).isEmpty();
    assertThat(profile.getVectorFields()).containsKey("embedding");
    assertThat(profile.getVectorFields().get("embedding").getDimension()).isEqualTo(128);
    assertThat(profile.getVectorFields().get("embedding").isQuantizeSQ8()).isTrue();
  }

  @Test
  public void profileSerializationRoundTripPreservesVectorFields() throws Exception {
    Map<String, VectorIndexMetadata> vectors = new LinkedHashMap<>();
    vectors.put("embedding",
        new VectorIndexMetadata("embedding", 384, VectorSimilarityFunction.COSINE, 24, 150, true));
    vectors.put("aux",
        new VectorIndexMetadata("aux", 64, VectorSimilarityFunction.EUCLIDEAN, 16, 100, false));

    LuceneIndexCreationProfile profile = new LuceneIndexCreationProfile("idx", "/region",
        new String[] {"title"}, new StandardAnalyzer(), null, null, vectors);

    // CopyHelper deep copy
    LuceneIndexCreationProfile copy = CopyHelper.deepCopy(profile);
    assertThat(copy.getVectorFields()).isEqualTo(vectors);

    // Explicit Geode DataSerializer stream round-trip
    HeapDataOutputStream hdos = new HeapDataOutputStream(KnownVersion.CURRENT);
    DataSerializer.writeObject(profile, hdos);
    LuceneIndexCreationProfile restored =
        DataSerializer
            .readObject(new DataInputStream(new ByteArrayInputStream(hdos.toByteArray())));

    assertThat(restored.getIndexName()).isEqualTo("idx");
    assertThat(restored.getVectorFields()).isEqualTo(vectors);
  }

  @Test
  public void profileCompatibilityAcceptsIdenticalVectorDefinitions() {
    VectorIndexMetadata meta =
        new VectorIndexMetadata("embedding", 128, VectorSimilarityFunction.COSINE, 16, 100, true);
    LuceneIndexCreationProfile p1 = profileWithVector("idx", meta);
    LuceneIndexCreationProfile p2 = profileWithVector("idx", meta);

    assertThat(p1.checkCompatibility(SEPARATOR + "region", p2)).isNull();
  }

  @Test
  public void profileCompatibilityRejectsMismatchedDimensions() {
    LuceneIndexCreationProfile p1 = profileWithVector("idx",
        new VectorIndexMetadata("embedding", 128, VectorSimilarityFunction.COSINE, true));
    LuceneIndexCreationProfile p2 = profileWithVector("idx",
        new VectorIndexMetadata("embedding", 256, VectorSimilarityFunction.COSINE, true));

    String error = p1.checkCompatibility(SEPARATOR + "region", p2);
    assertThat(error)
        .isNotNull()
        .contains("dimension")
        .contains("128")
        .contains("256");
  }

  @Test
  public void profileCompatibilityRejectsMismatchedSimilarity() {
    LuceneIndexCreationProfile p1 = profileWithVector("idx",
        new VectorIndexMetadata("embedding", 128, VectorSimilarityFunction.COSINE, true));
    LuceneIndexCreationProfile p2 = profileWithVector("idx",
        new VectorIndexMetadata("embedding", 128, VectorSimilarityFunction.EUCLIDEAN, true));

    String error = p1.checkCompatibility(SEPARATOR + "region", p2);
    assertThat(error)
        .isNotNull()
        .contains("similarity")
        .contains("COSINE")
        .contains("EUCLIDEAN");
  }

  @Test
  public void profileCompatibilityRejectsMismatchedSQ8() {
    LuceneIndexCreationProfile p1 = profileWithVector("idx",
        new VectorIndexMetadata("embedding", 128, VectorSimilarityFunction.COSINE, true));
    LuceneIndexCreationProfile p2 = profileWithVector("idx",
        new VectorIndexMetadata("embedding", 128, VectorSimilarityFunction.COSINE, false));

    String error = p1.checkCompatibility(SEPARATOR + "region", p2);
    assertThat(error)
        .isNotNull()
        .contains("quantizeSQ8");
  }

  @Test
  public void profileCompatibilityRejectsMismatchedHnswParameters() {
    LuceneIndexCreationProfile p1 = profileWithVector("idx",
        new VectorIndexMetadata("embedding", 128, VectorSimilarityFunction.COSINE, 16, 100, false));
    LuceneIndexCreationProfile p2 = profileWithVector("idx",
        new VectorIndexMetadata("embedding", 128, VectorSimilarityFunction.COSINE, 32, 200, false));

    String error = p1.checkCompatibility(SEPARATOR + "region", p2);
    assertThat(error)
        .isNotNull()
        .contains("HNSW parameters");
  }

  @Test
  public void profileCompatibilityRejectsMissingVectorFieldOnOneMember() {
    LuceneIndexCreationProfile withVector = profileWithVector("idx",
        new VectorIndexMetadata("embedding", 128, VectorSimilarityFunction.COSINE, true));
    LuceneIndexCreationProfile withoutVector = new LuceneIndexCreationProfile("idx", "/region",
        new String[] {"title"}, new StandardAnalyzer(), null, null);

    String error = withVector.checkCompatibility(SEPARATOR + "region", withoutVector);
    assertThat(error)
        .isNotNull()
        .contains("vector fields");
  }

  @Test
  public void indexRepositoryFactoryConfiguresVectorCodecOnlyWhenVectorFieldsPresent()
      throws Exception {
    IndexRepositoryFactory factory = spy(new IndexRepositoryFactory());
    BucketRegion fileBucket = mock(BucketRegion.class);
    LuceneIndexForPartitionedRegion indexForPR = mock(LuceneIndexForPartitionedRegion.class);
    when(indexForPR.getAnalyzer()).thenReturn(new StandardAnalyzer());

    Map<String, VectorIndexMetadata> vectors = new LinkedHashMap<>();
    vectors.put("sq8Vec",
        new VectorIndexMetadata("sq8Vec", 64, VectorSimilarityFunction.COSINE, 16, 100, true));
    vectors.put("fp32Vec",
        new VectorIndexMetadata("fp32Vec", 64, VectorSimilarityFunction.EUCLIDEAN, 24, 120, false));
    when(indexForPR.getVectorFields()).thenReturn(vectors);

    IndexWriter mockWriter = mock(IndexWriter.class);
    ArgumentCaptor<IndexWriterConfig> configCaptor =
        ArgumentCaptor.forClass(IndexWriterConfig.class);
    doReturn(mockWriter).when(factory).getIndexWriter(any(), configCaptor.capture());

    factory.buildIndexWriter(0, fileBucket, indexForPR);

    Codec codec = configCaptor.getValue().getCodec();
    assertThat(codec).isInstanceOf(Lucene912Codec.class);
    Lucene912Codec lucene912Codec = (Lucene912Codec) codec;
    assertThat(lucene912Codec.getKnnVectorsFormatForField("sq8Vec"))
        .isInstanceOf(Lucene99HnswScalarQuantizedVectorsFormat.class);
    assertThat(lucene912Codec.getKnnVectorsFormatForField("fp32Vec"))
        .isInstanceOf(Lucene99HnswVectorsFormat.class);
  }

  private LuceneIndexCreationProfile profileWithVector(String indexName,
      VectorIndexMetadata metadata) {
    return new LuceneIndexCreationProfile(indexName, "/region", new String[] {"title"},
        new StandardAnalyzer(), null, null,
        Collections.singletonMap(metadata.getFieldName(), metadata));
  }
}
