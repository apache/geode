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
package org.apache.geode.cache.lucene.internal.repository.serializer;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

import org.apache.lucene.document.Document;
import org.apache.lucene.document.KnnFloatVectorField;
import org.apache.lucene.index.IndexableField;
import org.apache.lucene.index.VectorSimilarityFunction;
import org.junit.Test;
import org.junit.experimental.categories.Category;

import org.apache.geode.cache.lucene.LuceneIndex;
import org.apache.geode.cache.lucene.LuceneService;
import org.apache.geode.cache.lucene.VectorIndexMetadata;
import org.apache.geode.pdx.PdxInstance;
import org.apache.geode.test.junit.categories.LuceneTest;

@Category({LuceneTest.class})
public class VectorFieldMapperJUnitTest {

  private static class BaseDocumentPojo {
    @SuppressWarnings("unused")
    private final String title;
    @SuppressWarnings("unused")
    private final float[] embedding;

    BaseDocumentPojo(String title, float[] embedding) {
      this.title = title;
      this.embedding = embedding;
    }
  }

  private static class SubDocumentPojo extends BaseDocumentPojo {
    @SuppressWarnings("unused")
    private final List<Float> listEmbedding;
    @SuppressWarnings("unused")
    private final double[] doubleEmbedding;
    @SuppressWarnings("unused")
    private final Float[] boxedEmbedding;

    SubDocumentPojo(String title, float[] embedding, List<Float> listEmbedding,
        double[] doubleEmbedding, Float[] boxedEmbedding) {
      super(title, embedding);
      this.listEmbedding = listEmbedding;
      this.doubleEmbedding = doubleEmbedding;
      this.boxedEmbedding = boxedEmbedding;
    }
  }

  @Test
  public void extractsFloatArrayFromPdxInstanceWithoutCallingGetObject() {
    float[] vec = new float[] {0.5f, 0.5f, 0.5f, 0.5f};
    PdxInstance pdx = mock(PdxInstance.class);
    when(pdx.hasField("embedding")).thenReturn(true);
    when(pdx.getField("embedding")).thenReturn(vec);

    VectorIndexMetadata metadata =
        new VectorIndexMetadata("embedding", 4, VectorSimilarityFunction.COSINE, true);
    Document doc = new Document();

    boolean added = VectorFieldMapper.addVectorField(doc, metadata, pdx);

    assertThat(added).isTrue();
    verify(pdx, never()).getObject();
    IndexableField field = doc.getField("embedding");
    assertThat(field).isInstanceOf(KnnFloatVectorField.class);
    KnnFloatVectorField knnField = (KnnFloatVectorField) field;
    assertThat(knnField.vectorValue()).isSameAs(vec);
    assertThat(knnField.fieldType().vectorDimension()).isEqualTo(4);
    assertThat(knnField.fieldType().vectorSimilarityFunction())
        .isEqualTo(VectorSimilarityFunction.COSINE);
  }

  @Test
  public void extractsListOfFloatFromPdxInstanceWithoutCallingGetObject() {
    List<Float> vecList = Arrays.asList(1.0f, 2.0f, 3.0f);
    PdxInstance pdx = mock(PdxInstance.class);
    when(pdx.hasField("embedding")).thenReturn(true);
    when(pdx.getField("embedding")).thenReturn(vecList);

    VectorIndexMetadata metadata =
        new VectorIndexMetadata("embedding", 3, VectorSimilarityFunction.EUCLIDEAN, false);
    Document doc = new Document();

    boolean added = VectorFieldMapper.addVectorField(doc, metadata, pdx);

    assertThat(added).isTrue();
    verify(pdx, never()).getObject();
    KnnFloatVectorField knnField = (KnnFloatVectorField) doc.getField("embedding");
    assertThat(knnField.vectorValue()).containsExactly(1.0f, 2.0f, 3.0f);
  }

  @Test
  public void extractsVectorsFromPojoHierarchyAndMultipleContainerTypes() {
    SubDocumentPojo pojo = new SubDocumentPojo("hello",
        new float[] {1.0f, 0.0f},
        Arrays.asList(0.0f, 1.0f),
        new double[] {0.6, 0.8},
        new Float[] {0.8f, 0.6f});

    VectorIndexMetadata inheritedMeta =
        new VectorIndexMetadata("embedding", 2, VectorSimilarityFunction.COSINE, false);
    VectorIndexMetadata listMeta =
        new VectorIndexMetadata("listEmbedding", 2, VectorSimilarityFunction.DOT_PRODUCT, false);
    VectorIndexMetadata doubleMeta =
        new VectorIndexMetadata("doubleEmbedding", 2, VectorSimilarityFunction.EUCLIDEAN, true);
    VectorIndexMetadata boxedMeta =
        new VectorIndexMetadata("boxedEmbedding", 2, VectorSimilarityFunction.COSINE, true);

    Document doc = new Document();
    int added = VectorFieldMapper.addVectorFields(doc,
        Arrays.asList(inheritedMeta, listMeta, doubleMeta, boxedMeta), pojo);

    assertThat(added).isEqualTo(4);
    assertThat(((KnnFloatVectorField) doc.getField("embedding")).vectorValue())
        .containsExactly(1.0f, 0.0f);
    assertThat(((KnnFloatVectorField) doc.getField("listEmbedding")).vectorValue())
        .containsExactly(0.0f, 1.0f);
    assertThat(((KnnFloatVectorField) doc.getField("doubleEmbedding")).vectorValue())
        .containsExactly(0.6f, 0.8f);
    assertThat(((KnnFloatVectorField) doc.getField("boxedEmbedding")).vectorValue())
        .containsExactly(0.8f, 0.6f);
  }

  @Test
  public void extractsDirectRegionValueVector() {
    float[] vec = new float[] {0.25f, 0.5f, 0.75f};
    VectorIndexMetadata meta = new VectorIndexMetadata(LuceneService.REGION_VALUE_FIELD, 3,
        VectorSimilarityFunction.EUCLIDEAN, false);

    Document doc = new Document();
    assertThat(VectorFieldMapper.addVectorField(doc, meta, vec)).isTrue();
    assertThat(((KnnFloatVectorField) doc.getField(LuceneService.REGION_VALUE_FIELD)).vectorValue())
        .containsExactly(0.25f, 0.5f, 0.75f);
  }

  @Test
  public void skipsMissingOrNullVectorFieldsCleanly() {
    PdxInstance pdx = mock(PdxInstance.class);
    when(pdx.hasField("missing")).thenReturn(false);
    when(pdx.hasField("nullVec")).thenReturn(true);
    when(pdx.getField("nullVec")).thenReturn(null);

    Document doc = new Document();
    assertThat(VectorFieldMapper.addVectorField(doc,
        new VectorIndexMetadata("missing", 3, VectorSimilarityFunction.COSINE, false), pdx))
            .isFalse();
    assertThat(VectorFieldMapper.addVectorField(doc,
        new VectorIndexMetadata("nullVec", 3, VectorSimilarityFunction.COSINE, false), pdx))
            .isFalse();
    assertThat(doc.getFields()).isEmpty();
    verify(pdx, never()).getObject();
  }

  @Test
  public void rejectsDimensionMismatch() {
    PdxInstance pdx = mock(PdxInstance.class);
    when(pdx.hasField("embedding")).thenReturn(true);
    when(pdx.getField("embedding")).thenReturn(new float[] {1.0f, 2.0f});

    VectorIndexMetadata metadata =
        new VectorIndexMetadata("embedding", 4, VectorSimilarityFunction.EUCLIDEAN, false);

    assertThatThrownBy(() -> VectorFieldMapper.addVectorField(new Document(), metadata, pdx))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessageContaining("dimension mismatch")
        .hasMessageContaining("expected 4")
        .hasMessageContaining("got 2");
    verify(pdx, never()).getObject();
  }

  @Test
  public void rejectsNaNAndInfiniteValues() {
    VectorIndexMetadata metadata =
        new VectorIndexMetadata("embedding", 3, VectorSimilarityFunction.EUCLIDEAN, false);

    BaseDocumentPojo nanPojo =
        new BaseDocumentPojo("bad", new float[] {1.0f, Float.NaN, 2.0f});
    BaseDocumentPojo infPojo =
        new BaseDocumentPojo("bad", new float[] {1.0f, Float.POSITIVE_INFINITY, 2.0f});

    assertThatThrownBy(() -> VectorFieldMapper.addVectorField(new Document(), metadata, nanPojo))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessageContaining("non-finite");

    assertThatThrownBy(() -> VectorFieldMapper.addVectorField(new Document(), metadata, infPojo))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessageContaining("non-finite");
  }

  @Test
  public void rejectsZeroVectorForCosineButAllowsForEuclidean() {
    BaseDocumentPojo zeroPojo = new BaseDocumentPojo("zero", new float[] {0.0f, 0.0f, 0.0f});

    VectorIndexMetadata cosineMeta =
        new VectorIndexMetadata("embedding", 3, VectorSimilarityFunction.COSINE, false);
    assertThatThrownBy(
        () -> VectorFieldMapper.addVectorField(new Document(), cosineMeta, zeroPojo))
            .isInstanceOf(IllegalArgumentException.class)
            .hasMessageContaining("Zero-magnitude");

    VectorIndexMetadata euclideanMeta =
        new VectorIndexMetadata("embedding", 3, VectorSimilarityFunction.EUCLIDEAN, false);
    Document doc = new Document();
    assertThat(VectorFieldMapper.addVectorField(doc, euclideanMeta, zeroPojo)).isTrue();
    assertThat(((KnnFloatVectorField) doc.getField("embedding")).vectorValue())
        .containsExactly(0.0f, 0.0f, 0.0f);
  }

  @Test
  public void rejectsUnsupportedFieldTypeAndNullListElement() {
    VectorIndexMetadata metadata =
        new VectorIndexMetadata("embedding", 2, VectorSimilarityFunction.EUCLIDEAN, false);

    PdxInstance stringPdx = mock(PdxInstance.class);
    when(stringPdx.hasField("embedding")).thenReturn(true);
    when(stringPdx.getField("embedding")).thenReturn("not-a-vector");

    assertThatThrownBy(() -> VectorFieldMapper.addVectorField(new Document(), metadata, stringPdx))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessageContaining("Unsupported vector type");

    PdxInstance nullElemPdx = mock(PdxInstance.class);
    when(nullElemPdx.hasField("embedding")).thenReturn(true);
    when(nullElemPdx.getField("embedding")).thenReturn(Arrays.asList(1.0f, null));

    assertThatThrownBy(
        () -> VectorFieldMapper.addVectorField(new Document(), metadata, nullElemPdx))
            .isInstanceOf(IllegalArgumentException.class)
            .hasMessageContaining("non-null Number");
  }

  @Test
  public void heterogeneousSerializerHandlesMixedTextAndVectorFieldsForPojoAndPdx() {
    LuceneIndex index = mock(LuceneIndex.class);
    when(index.getFieldNames()).thenReturn(new String[] {"title"});
    Map<String, VectorIndexMetadata> vectorFields = new LinkedHashMap<>();
    vectorFields.put("embedding",
        new VectorIndexMetadata("embedding", 3, VectorSimilarityFunction.COSINE, true));
    when(index.getVectorFields()).thenReturn(vectorFields);

    HeterogeneousLuceneSerializer serializer = new HeterogeneousLuceneSerializer();

    // POJO path
    BaseDocumentPojo pojo = new BaseDocumentPojo("geode vector", new float[] {1.0f, 0.0f, 0.0f});
    Collection<Document> pojoDocs = serializer.toDocuments(index, pojo);
    assertThat(pojoDocs).hasSize(1);
    Document pojoDoc = pojoDocs.iterator().next();
    assertThat(pojoDoc.getField("title").stringValue()).isEqualTo("geode vector");
    assertThat(((KnnFloatVectorField) pojoDoc.getField("embedding")).vectorValue())
        .containsExactly(1.0f, 0.0f, 0.0f);

    // Zero-copy PdxInstance path
    PdxInstance pdx = mock(PdxInstance.class);
    when(pdx.hasField("title")).thenReturn(true);
    when(pdx.getField("title")).thenReturn("pdx vector");
    when(pdx.hasField("embedding")).thenReturn(true);
    when(pdx.getField("embedding")).thenReturn(Arrays.asList(0.0f, 1.0f, 0.0f));

    Collection<Document> pdxDocs = serializer.toDocuments(index, pdx);
    assertThat(pdxDocs).hasSize(1);
    Document pdxDoc = pdxDocs.iterator().next();
    assertThat(pdxDoc.getField("title").stringValue()).isEqualTo("pdx vector");
    assertThat(((KnnFloatVectorField) pdxDoc.getField("embedding")).vectorValue())
        .containsExactly(0.0f, 1.0f, 0.0f);
    verify(pdx, never()).getObject();
  }

  @Test
  public void heterogeneousSerializerHandlesVectorOnlyIndex() {
    LuceneIndex index = mock(LuceneIndex.class);
    when(index.getFieldNames()).thenReturn(new String[0]);
    when(index.getVectorFields()).thenReturn(Collections.singletonMap("embedding",
        new VectorIndexMetadata("embedding", 2, VectorSimilarityFunction.EUCLIDEAN, false)));

    HeterogeneousLuceneSerializer serializer = new HeterogeneousLuceneSerializer();
    BaseDocumentPojo pojo = new BaseDocumentPojo("ignored", new float[] {3.0f, 4.0f});
    Document doc = serializer.toDocuments(index, pojo).iterator().next();

    assertThat(doc.getField("title")).isNull();
    assertThat(((KnnFloatVectorField) doc.getField("embedding")).vectorValue())
        .containsExactly(3.0f, 4.0f);
  }
}
