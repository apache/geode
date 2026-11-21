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
package org.apache.geode.cache.lucene;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;

import org.apache.lucene.index.VectorSimilarityFunction;
import org.junit.Test;
import org.junit.experimental.categories.Category;

import org.apache.geode.test.junit.categories.LuceneTest;

@Category({LuceneTest.class})
public class VectorIndexMetadataJUnitTest {

  @Test
  public void defaultConstructorPopulatesDefaults() {
    VectorIndexMetadata meta =
        new VectorIndexMetadata("embedding", 768, VectorSimilarityFunction.COSINE, true);

    assertThat(meta.getFieldName()).isEqualTo("embedding");
    assertThat(meta.getDimension()).isEqualTo(768);
    assertThat(meta.getSimilarityFunction()).isEqualTo(VectorSimilarityFunction.COSINE);
    assertThat(meta.getMaxConnections()).isEqualTo(VectorIndexMetadata.DEFAULT_MAX_CONNECTIONS);
    assertThat(meta.getBeamWidth()).isEqualTo(VectorIndexMetadata.DEFAULT_BEAM_WIDTH);
    assertThat(meta.isQuantizeSQ8()).isTrue();
  }

  @Test
  public void explicitConstructorPopulatesAllFields() {
    VectorIndexMetadata meta = new VectorIndexMetadata("vec", 128,
        VectorSimilarityFunction.DOT_PRODUCT, 32, 200, false);

    assertThat(meta.getFieldName()).isEqualTo("vec");
    assertThat(meta.getDimension()).isEqualTo(128);
    assertThat(meta.getSimilarityFunction()).isEqualTo(VectorSimilarityFunction.DOT_PRODUCT);
    assertThat(meta.getMaxConnections()).isEqualTo(32);
    assertThat(meta.getBeamWidth()).isEqualTo(200);
    assertThat(meta.isQuantizeSQ8()).isFalse();
  }

  @Test
  public void rejectsInvalidFieldName() {
    assertThatThrownBy(
        () -> new VectorIndexMetadata(null, 128, VectorSimilarityFunction.COSINE, false))
            .isInstanceOf(IllegalArgumentException.class)
            .hasMessageContaining("field name");

    assertThatThrownBy(
        () -> new VectorIndexMetadata("   ", 128, VectorSimilarityFunction.COSINE, false))
            .isInstanceOf(IllegalArgumentException.class)
            .hasMessageContaining("field name");
  }

  @Test
  public void rejectsNonPositiveDimension() {
    assertThatThrownBy(
        () -> new VectorIndexMetadata("vec", 0, VectorSimilarityFunction.EUCLIDEAN, false))
            .isInstanceOf(IllegalArgumentException.class)
            .hasMessageContaining("dimension");

    assertThatThrownBy(
        () -> new VectorIndexMetadata("vec", -64, VectorSimilarityFunction.EUCLIDEAN, true))
            .isInstanceOf(IllegalArgumentException.class)
            .hasMessageContaining("dimension");
  }

  @Test
  public void rejectsNullSimilarity() {
    assertThatThrownBy(() -> new VectorIndexMetadata("vec", 64, null, false))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessageContaining("similarity");
  }

  @Test
  public void rejectsInvalidHnswParameters() {
    assertThatThrownBy(
        () -> new VectorIndexMetadata("vec", 64, VectorSimilarityFunction.COSINE, 0, 100, false))
            .isInstanceOf(IllegalArgumentException.class)
            .hasMessageContaining("maxConnections");

    assertThatThrownBy(
        () -> new VectorIndexMetadata("vec", 64, VectorSimilarityFunction.COSINE, -1, 100, false))
            .isInstanceOf(IllegalArgumentException.class)
            .hasMessageContaining("maxConnections");

    assertThatThrownBy(() -> new VectorIndexMetadata("vec", 64, VectorSimilarityFunction.COSINE,
        VectorIndexMetadata.MAX_CONNECTIONS_LIMIT + 1, 100, false))
            .isInstanceOf(IllegalArgumentException.class)
            .hasMessageContaining("maxConnections");

    assertThatThrownBy(
        () -> new VectorIndexMetadata("vec", 64, VectorSimilarityFunction.COSINE, 16, 0, false))
            .isInstanceOf(IllegalArgumentException.class)
            .hasMessageContaining("beamWidth");

    assertThatThrownBy(
        () -> new VectorIndexMetadata("vec", 64, VectorSimilarityFunction.COSINE, 16, -5, false))
            .isInstanceOf(IllegalArgumentException.class)
            .hasMessageContaining("beamWidth");

    assertThatThrownBy(() -> new VectorIndexMetadata("vec", 64, VectorSimilarityFunction.COSINE, 16,
        VectorIndexMetadata.MAX_BEAM_WIDTH_LIMIT + 1, false))
            .isInstanceOf(IllegalArgumentException.class)
            .hasMessageContaining("beamWidth");
  }

  @Test
  public void supportsJavaSerializationRoundTrip() throws Exception {
    VectorIndexMetadata original = new VectorIndexMetadata("embedding", 384,
        VectorSimilarityFunction.EUCLIDEAN, 24, 150, true);

    ByteArrayOutputStream baos = new ByteArrayOutputStream();
    try (ObjectOutputStream oos = new ObjectOutputStream(baos)) {
      oos.writeObject(original);
    }

    VectorIndexMetadata restored;
    try (ObjectInputStream ois =
        new ObjectInputStream(new ByteArrayInputStream(baos.toByteArray()))) {
      restored = (VectorIndexMetadata) ois.readObject();
    }

    assertThat(restored).isEqualTo(original);
    assertThat(restored.hashCode()).isEqualTo(original.hashCode());
    assertThat(restored.toString()).isEqualTo(original.toString());
  }

  @Test
  public void supportsDataStreamSerializationRoundTrip() throws Exception {
    VectorIndexMetadata original = new VectorIndexMetadata("embedding", 1536,
        VectorSimilarityFunction.COSINE, 32, 256, true);

    ByteArrayOutputStream baos = new ByteArrayOutputStream();
    try (DataOutputStream dos = new DataOutputStream(baos)) {
      original.writeTo(dos);
    }

    VectorIndexMetadata restored;
    try (DataInputStream dis = new DataInputStream(new ByteArrayInputStream(baos.toByteArray()))) {
      restored = VectorIndexMetadata.readFrom(dis);
    }

    assertThat(restored).isEqualTo(original);
  }
}
