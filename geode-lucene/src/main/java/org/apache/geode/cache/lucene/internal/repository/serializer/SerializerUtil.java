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

package org.apache.geode.cache.lucene.internal.repository.serializer;

import java.io.ByteArrayOutputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.util.Collection;
import java.util.Collections;
import java.util.HashSet;
import java.util.Set;

import org.apache.lucene.document.Document;
import org.apache.lucene.document.DoublePoint;
import org.apache.lucene.document.Field.Store;
import org.apache.lucene.document.FloatPoint;
import org.apache.lucene.document.IntPoint;
import org.apache.lucene.document.LongPoint;
import org.apache.lucene.document.StringField;
import org.apache.lucene.document.TextField;
import org.apache.lucene.index.IndexableField;
import org.apache.lucene.index.Term;
import org.apache.lucene.util.BytesRef;

import org.apache.geode.DataSerializer;
import org.apache.geode.InternalGemFireError;
import org.apache.geode.internal.util.BlobHelper;

/**
 * Static utility functions for mapping objects to lucene documents
 */
public class SerializerUtil {
  private static final String KEY_FIELD = "_KEY";

  private static final Set<Class> SUPPORTED_PRIMITIVE_TYPES;

  static {
    HashSet<Class> primitiveTypes = new HashSet<>();
    primitiveTypes.add(String.class);
    primitiveTypes.add(long.class);
    primitiveTypes.add(int.class);
    primitiveTypes.add(float.class);
    primitiveTypes.add(double.class);
    primitiveTypes.add(Long.class);
    primitiveTypes.add(Integer.class);
    primitiveTypes.add(Float.class);
    primitiveTypes.add(Double.class);

    SUPPORTED_PRIMITIVE_TYPES = Collections.unmodifiableSet(primitiveTypes);
  }

  /**
   * A small buffer for converting keys to byte[] arrays.
   */
  private static final ThreadLocal<ByteArrayOutputStream> LOCAL_BUFFER =
      new ThreadLocal<ByteArrayOutputStream>() {
        @Override
        protected ByteArrayOutputStream initialValue() {
          return new ByteArrayOutputStream();
        }
      };

  private SerializerUtil() {}

  /**
   * Add a Apache Geode key to a document
   */
  public static void addKey(Object key, Document doc) {
    if (key instanceof String) {
      doc.add(new StringField(KEY_FIELD, (String) key, Store.YES));
    } else {
      doc.add(new StringField(KEY_FIELD, keyToBytes(key), Store.YES));
    }
  }

  /**
   * Add a field to the document.
   *
   * @return true if the field was successfully added
   */
  public static boolean addField(Document doc, String field, Object fieldValue) {
    Class<?> clazz = fieldValue.getClass();
    if (clazz == String.class) {
      doc.add(new TextField(field, (String) fieldValue, Store.NO));
    } else if (clazz == Long.class) {
      doc.add(new LongPoint(field, (Long) fieldValue));
    } else if (clazz == Integer.class) {
      doc.add(new IntPoint(field, (Integer) fieldValue));
    } else if (clazz == Float.class) {
      doc.add(new FloatPoint(field, (Float) fieldValue));
    } else if (clazz == Double.class) {
      doc.add(new DoublePoint(field, (Double) fieldValue));
    } else {
      return false;
    }

    return true;
  }

  /**
   * Return true if a field type can be written to a lucene document.
   */
  public static boolean isSupported(Class<?> type) {
    return SUPPORTED_PRIMITIVE_TYPES.contains(type);
  }

  public static Collection<Class> supportedPrimitiveTypes() {
    return SUPPORTED_PRIMITIVE_TYPES;
  }

  /**
   * Extract the Apache Geode key from a lucene document
   */
  public static Object getKey(Document doc) {
    IndexableField field = doc.getField(KEY_FIELD);
    if (field.stringValue() != null) {
      return field.stringValue();
    } else {
      return keyFromBytes(field.binaryValue());
    }
  }

  /**
   * Extract the Apache Geode key term from a lucene document
   */
  public static Term getKeyTerm(Document doc) {
    IndexableField field = doc.getField(KEY_FIELD);
    if (field.stringValue() != null) {
      return new Term(KEY_FIELD, field.stringValue());
    } else {
      return new Term(KEY_FIELD, field.binaryValue());
    }
  }

  /**
   * Convert a Apache Geode key into a key search term that can be used to update or delete the
   * document associated with this key.
   */
  public static Term toKeyTerm(Object key) {
    if (key instanceof String) {
      return new Term(KEY_FIELD, (String) key);
    } else {
      return new Term(KEY_FIELD, keyToBytes(key));
    }
  }

  private static Object keyFromBytes(BytesRef bytes) {
    try {
      return BlobHelper.deserializeBlob(bytes.bytes);
    } catch (ClassNotFoundException | IOException e) {
      throw new InternalGemFireError(e);
    }
  }

  /**
   * Convert a key to a byte array.
   */
  private static BytesRef keyToBytes(Object key) {
    ByteArrayOutputStream buffer = LOCAL_BUFFER.get();

    try {
      DataOutputStream out = new DataOutputStream(buffer);
      DataSerializer.writeObject(key, out);
      out.flush();
      BytesRef result = new BytesRef(buffer.toByteArray());
      buffer.reset();
      return result;
    } catch (IOException e) {
      throw new InternalGemFireError("Unable to serialize key", e);
    }
  }

}
