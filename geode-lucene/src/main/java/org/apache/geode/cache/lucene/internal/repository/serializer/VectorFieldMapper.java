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

import java.io.IOException;
import java.lang.reflect.Field;
import java.util.Collection;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

import org.apache.lucene.codecs.Codec;
import org.apache.lucene.codecs.KnnVectorsFormat;
import org.apache.lucene.codecs.KnnVectorsReader;
import org.apache.lucene.codecs.KnnVectorsWriter;
import org.apache.lucene.codecs.lucene912.Lucene912Codec;
import org.apache.lucene.codecs.lucene99.Lucene99HnswScalarQuantizedVectorsFormat;
import org.apache.lucene.codecs.lucene99.Lucene99HnswVectorsFormat;
import org.apache.lucene.document.Document;
import org.apache.lucene.document.FieldType;
import org.apache.lucene.document.KnnFloatVectorField;
import org.apache.lucene.index.SegmentReadState;
import org.apache.lucene.index.SegmentWriteState;
import org.apache.lucene.index.VectorSimilarityFunction;

import org.apache.geode.cache.lucene.LuceneService;
import org.apache.geode.cache.lucene.VectorIndexMetadata;
import org.apache.geode.internal.util.concurrent.CopyOnWriteWeakHashMap;
import org.apache.geode.pdx.PdxInstance;

/**
 * Maps vector fields from POJOs and {@link PdxInstance} values to Lucene 9.12
 * {@link KnnFloatVectorField} instances and constructs per-field HNSW / SQ8 codecs.
 *
 * <p>
 * For {@link PdxInstance} values, vectors are extracted directly via
 * {@link PdxInstance#getField(String)} without invoking {@link PdxInstance#getObject()}, avoiding
 * full domain-object deserialization.
 */
public final class VectorFieldMapper {

  private static final Field MISSING_FIELD_SENTINEL;

  static {
    try {
      MISSING_FIELD_SENTINEL = VectorFieldMapper.class.getDeclaredField("MISSING_FIELD_SENTINEL");
    } catch (NoSuchFieldException e) {
      throw new ExceptionInInitializerError(e);
    }
  }

  /**
   * Weakly-keyed cache mapping POJO classes to their resolved declared fields by field name.
   */
  private static final Map<Class<?>, ConcurrentHashMap<String, Field>> FIELD_CACHE =
      new CopyOnWriteWeakHashMap<>();

  private VectorFieldMapper() {}

  /**
   * Extracts a float vector for the configured {@link VectorIndexMetadata} from a POJO or
   * {@link PdxInstance} and adds a {@link KnnFloatVectorField} to the Lucene {@link Document}.
   *
   * <p>
   * If the field is absent or its value is {@code null}, no field is added and {@code false} is
   * returned. If the value is present but invalid (unsupported type, dimension mismatch, NaN or
   * infinite elements, or zero-magnitude vector with {@link VectorSimilarityFunction#COSINE}), an
   * {@link IllegalArgumentException} is thrown.
   *
   * @param doc target Lucene document
   * @param metadata vector field descriptor
   * @param value region value (POJO, {@link PdxInstance}, or direct vector when using
   *        {@link LuceneService#REGION_VALUE_FIELD})
   * @return {@code true} if a {@link KnnFloatVectorField} was added, {@code false} if the field
   *         was absent or null
   */
  public static boolean addVectorField(Document doc, VectorIndexMetadata metadata, Object value) {
    if (doc == null) {
      throw new IllegalArgumentException("Lucene Document must not be null");
    }
    if (metadata == null) {
      throw new IllegalArgumentException("VectorIndexMetadata must not be null");
    }
    float[] vector = extractVector(value, metadata);
    if (vector == null) {
      return false;
    }
    doc.add(createKnnFloatVectorField(vector, metadata));
    return true;
  }

  /**
   * Extracts all configured vector fields from {@code value} and adds them to {@code doc}.
   *
   * @param doc target Lucene document
   * @param vectorFields collection of vector field metadata descriptors
   * @param value region value (POJO or {@link PdxInstance})
   * @return number of vector fields added to the document
   */
  public static int addVectorFields(Document doc, Collection<VectorIndexMetadata> vectorFields,
      Object value) {
    if (vectorFields == null || vectorFields.isEmpty()) {
      return 0;
    }
    int added = 0;
    for (VectorIndexMetadata metadata : vectorFields) {
      if (addVectorField(doc, metadata, value)) {
        added++;
      }
    }
    return added;
  }

  /**
   * Extracts and validates a {@code float[]} vector from a POJO or {@link PdxInstance} without
   * deserializing {@link PdxInstance} into a domain object.
   *
   * @param value source object (POJO, {@link PdxInstance}, or direct vector for
   *        {@link LuceneService#REGION_VALUE_FIELD})
   * @param metadata vector field descriptor
   * @return validated {@code float[]} vector, or {@code null} if the field is missing or null
   */
  public static float[] extractVector(Object value, VectorIndexMetadata metadata) {
    if (metadata == null) {
      throw new IllegalArgumentException("VectorIndexMetadata must not be null");
    }
    if (value == null) {
      return null;
    }
    Object rawValue = readRawFieldValue(value, metadata.getFieldName());
    if (rawValue == null) {
      return null;
    }
    float[] vector = toFloatArray(rawValue, metadata);
    validateVector(vector, metadata);
    return vector;
  }

  /**
   * Creates a Lucene 9.12 {@link KnnFloatVectorField} for a validated float vector.
   */
  public static KnnFloatVectorField createKnnFloatVectorField(float[] vector,
      VectorIndexMetadata metadata) {
    if (metadata == null) {
      throw new IllegalArgumentException("VectorIndexMetadata must not be null");
    }
    validateVector(vector, metadata);
    FieldType fieldType = KnnFloatVectorField.createFieldType(metadata.getDimension(),
        metadata.getSimilarityFunction());
    return new KnnFloatVectorField(metadata.getFieldName(), vector, fieldType);
  }

  /**
   * Creates a Lucene 9.12 {@link Codec} wrapping {@link Lucene912Codec} that applies per-field
   * HNSW parameters and optional SQ8 scalar quantization
   * ({@link Lucene99HnswScalarQuantizedVectorsFormat}) for all configured vector fields.
   *
   * @param vectorFields map of vector field names to metadata descriptors
   * @return configured {@link Codec}, or {@link Codec#getDefault()} if {@code vectorFields} is
   *         null or empty
   */
  public static Codec createCodec(Map<String, VectorIndexMetadata> vectorFields) {
    if (vectorFields == null || vectorFields.isEmpty()) {
      return Codec.getDefault();
    }
    Map<String, VectorIndexMetadata> snapshot =
        Collections.unmodifiableMap(new LinkedHashMap<>(vectorFields));
    return new VectorAwareLucene912Codec(snapshot);
  }

  /**
   * Creates the {@link KnnVectorsFormat} for a single vector field descriptor. Uses
   * {@link Lucene99HnswScalarQuantizedVectorsFormat} (backed by
   * {@code Lucene99ScalarQuantizedVectorsFormat}) when {@link VectorIndexMetadata#isQuantizeSQ8()}
   * is {@code true}, and {@link Lucene99HnswVectorsFormat} otherwise.
   */
  public static KnnVectorsFormat createKnnVectorsFormat(VectorIndexMetadata metadata) {
    if (metadata == null) {
      throw new IllegalArgumentException("VectorIndexMetadata must not be null");
    }
    KnnVectorsFormat baseFormat = metadata.isQuantizeSQ8()
        ? new Lucene99HnswScalarQuantizedVectorsFormat(metadata.getMaxConnections(),
            metadata.getBeamWidth())
        : new Lucene99HnswVectorsFormat(metadata.getMaxConnections(), metadata.getBeamWidth());
    if (metadata.getDimension() > baseFormat.getMaxDimensions(metadata.getFieldName())) {
      return new HighDimensionKnnVectorsFormat(baseFormat, metadata.getDimension());
    }
    return baseFormat;
  }

  private static Object readRawFieldValue(Object value, String fieldName) {
    if (value instanceof PdxInstance) {
      PdxInstance pdx = (PdxInstance) value;
      if (!pdx.hasField(fieldName)) {
        return null;
      }
      // Zero-copy / partial-read path: never invoke pdx.getObject().
      return pdx.getField(fieldName);
    }

    if (LuceneService.REGION_VALUE_FIELD.equals(fieldName) && isVectorContainer(value)) {
      return value;
    }

    Field field = resolveDeclaredField(value.getClass(), fieldName);
    if (field == null) {
      return null;
    }
    try {
      return field.get(value);
    } catch (IllegalAccessException e) {
      throw new IllegalArgumentException(
          "Unable to access vector field '" + fieldName + "' on " + value.getClass().getName(), e);
    }
  }

  private static boolean isVectorContainer(Object value) {
    return value instanceof float[]
        || value instanceof List<?>
        || value instanceof double[]
        || value instanceof Number[];
  }

  private static Field resolveDeclaredField(Class<?> clazz, String fieldName) {
    ConcurrentHashMap<String, Field> classFields = FIELD_CACHE.get(clazz);
    if (classFields == null) {
      classFields = new ConcurrentHashMap<>();
      FIELD_CACHE.put(clazz, classFields);
    }
    Field cached = classFields.get(fieldName);
    if (cached != null) {
      return cached == MISSING_FIELD_SENTINEL ? null : cached;
    }
    Field found = findDeclaredFieldInHierarchy(clazz, fieldName);
    classFields.put(fieldName, found == null ? MISSING_FIELD_SENTINEL : found);
    return found;
  }

  private static Field findDeclaredFieldInHierarchy(Class<?> clazz, String fieldName) {
    Class<?> current = clazz;
    while (current != null && current != Object.class) {
      try {
        Field field = current.getDeclaredField(fieldName);
        field.setAccessible(true);
        return field;
      } catch (NoSuchFieldException ignored) {
        current = current.getSuperclass();
      }
    }
    return null;
  }

  private static float[] toFloatArray(Object rawValue, VectorIndexMetadata metadata) {
    String fieldName = metadata.getFieldName();
    int expectedDim = metadata.getDimension();

    if (rawValue instanceof float[]) {
      float[] arr = (float[]) rawValue;
      checkDimension(fieldName, expectedDim, arr.length);
      return arr;
    }

    if (rawValue instanceof List<?>) {
      List<?> list = (List<?>) rawValue;
      checkDimension(fieldName, expectedDim, list.size());
      float[] result = new float[expectedDim];
      for (int i = 0; i < expectedDim; i++) {
        Object elem = list.get(i);
        if (!(elem instanceof Number)) {
          throw new IllegalArgumentException(
              "Vector field '" + fieldName + "' list element at index " + i
                  + " must be a non-null Number, got: "
                  + (elem == null ? "null" : elem.getClass().getName()));
        }
        result[i] = ((Number) elem).floatValue();
      }
      return result;
    }

    if (rawValue instanceof double[]) {
      double[] arr = (double[]) rawValue;
      checkDimension(fieldName, expectedDim, arr.length);
      float[] result = new float[expectedDim];
      for (int i = 0; i < expectedDim; i++) {
        result[i] = (float) arr[i];
      }
      return result;
    }

    if (rawValue instanceof Number[]) {
      Number[] arr = (Number[]) rawValue;
      checkDimension(fieldName, expectedDim, arr.length);
      float[] result = new float[expectedDim];
      for (int i = 0; i < expectedDim; i++) {
        Number elem = arr[i];
        if (elem == null) {
          throw new IllegalArgumentException(
              "Vector field '" + fieldName + "' array element at index " + i
                  + " must not be null");
        }
        result[i] = elem.floatValue();
      }
      return result;
    }

    throw new IllegalArgumentException(
        "Unsupported vector type for field '" + fieldName + "': " + rawValue.getClass().getName()
            + " (expected float[], List<Float>, double[], or Number[])");
  }

  private static void checkDimension(String fieldName, int expected, int actual) {
    if (actual != expected) {
      throw new IllegalArgumentException(
          "Vector dimension mismatch for field '" + fieldName + "': expected " + expected
              + ", got " + actual);
    }
  }

  /**
   * Validates vector dimensionality, finiteness (rejects NaN and Infinity), and non-zero norm for
   * {@link VectorSimilarityFunction#COSINE}.
   */
  public static void validateVector(float[] vector, VectorIndexMetadata metadata) {
    if (vector == null) {
      throw new IllegalArgumentException(
          "Vector for field '" + metadata.getFieldName() + "' must not be null");
    }
    checkDimension(metadata.getFieldName(), metadata.getDimension(), vector.length);

    double normSq = 0.0;
    for (int i = 0; i < vector.length; i++) {
      float v = vector[i];
      if (!Float.isFinite(v)) {
        throw new IllegalArgumentException(
            "Vector for field '" + metadata.getFieldName() + "' contains non-finite value at index "
                + i + ": " + v);
      }
      normSq += (double) v * (double) v;
    }
    if (metadata.getSimilarityFunction() == VectorSimilarityFunction.COSINE && normSq == 0.0) {
      throw new IllegalArgumentException(
          "Zero-magnitude vector is not allowed for COSINE similarity on field '"
              + metadata.getFieldName() + "'");
    }
  }

  /**
   * {@link Lucene912Codec} specialization selecting per-field HNSW or SQ8 scalar-quantized
   * {@link KnnVectorsFormat} instances based on {@link VectorIndexMetadata}.
   */
  private static final class VectorAwareLucene912Codec extends Lucene912Codec {
    private final Map<String, KnnVectorsFormat> formatsByField;

    VectorAwareLucene912Codec(Map<String, VectorIndexMetadata> vectorFields) {
      super();
      Map<String, KnnVectorsFormat> map = new LinkedHashMap<>();
      for (Map.Entry<String, VectorIndexMetadata> entry : vectorFields.entrySet()) {
        map.put(entry.getKey(), createKnnVectorsFormat(entry.getValue()));
      }
      this.formatsByField = Collections.unmodifiableMap(map);
    }

    @Override
    public KnnVectorsFormat getKnnVectorsFormatForField(String field) {
      KnnVectorsFormat format = formatsByField.get(field);
      if (format != null) {
        return format;
      }
      return super.getKnnVectorsFormatForField(field);
    }
  }

  /**
   * Thin delegating {@link KnnVectorsFormat} allowing dimensions above Lucene's 1024 default cap
   * (e.g., 1536 or 3072) while preserving the underlying standard SPI format name on disk.
   */
  private static final class HighDimensionKnnVectorsFormat extends KnnVectorsFormat {
    private final KnnVectorsFormat delegate;
    private final int maxDimensions;

    HighDimensionKnnVectorsFormat(KnnVectorsFormat delegate, int maxDimensions) {
      super(delegate.getName());
      this.delegate = delegate;
      this.maxDimensions = maxDimensions;
    }

    @Override
    public KnnVectorsWriter fieldsWriter(SegmentWriteState state) throws IOException {
      return delegate.fieldsWriter(state);
    }

    @Override
    public KnnVectorsReader fieldsReader(SegmentReadState state) throws IOException {
      return delegate.fieldsReader(state);
    }

    @Override
    public int getMaxDimensions(String fieldName) {
      return Math.max(delegate.getMaxDimensions(fieldName), maxDimensions);
    }
  }
}
