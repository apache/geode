/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 * 
 *   http://www.apache.org/licenses/LICENSE-2.0
 * 
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package com.gemstone.gemfire.cache.lucene.internal.repository.serializer;

import java.io.ByteArrayOutputStream;
import java.io.DataOutputStream;
import java.io.IOException;

import org.apache.lucene.document.Document;
import org.apache.lucene.document.DoubleField;
import org.apache.lucene.document.Field.Store;
import org.apache.lucene.document.FloatField;
import org.apache.lucene.document.IntField;
import org.apache.lucene.document.LongField;
import org.apache.lucene.document.StringField;
import org.apache.lucene.document.TextField;
import org.apache.lucene.index.IndexableField;
import org.apache.lucene.index.Term;
import org.apache.lucene.util.BytesRef;

import com.gemstone.gemfire.DataSerializer;
import com.gemstone.gemfire.InternalGemFireError;
import com.gemstone.gemfire.internal.util.BlobHelper;

/**
 * Static utility functions for mapping objects to lucene documents
 */
public class SerializerUtil {
  private static final String KEY_FIELD = "_KEY";
  
  /**
   * A small buffer for converting keys to byte[] arrays.
   */
  private static ThreadLocal<ByteArrayOutputStream> LOCAL_BUFFER = new ThreadLocal<ByteArrayOutputStream>() {
    @Override
    protected ByteArrayOutputStream initialValue() {
      return new ByteArrayOutputStream();
    }
  };

  private SerializerUtil() {
  }
  
  /**
   * Add a gemfire key to a document
   */
  public static void addKey(Object key, Document doc) {
    if(key instanceof String) {
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
    if(clazz == String.class) {
      doc.add(new TextField(field, (String)fieldValue, Store.NO));
    } else if (clazz == Long.class) {
      doc.add(new LongField(field, (Long) fieldValue, Store.NO));
    } else if (clazz == Integer.class) {
      doc.add(new IntField(field, (Integer) fieldValue, Store.NO));
    } else if (clazz == Float.class) {
      doc.add(new FloatField(field, (Float) fieldValue, Store.NO));
    }  else if (clazz == Double.class) {
        doc.add(new DoubleField(field, (Double) fieldValue, Store.NO));
    } else {
      return false;
    }
    
    return true;
  }

  /**
   * Return true if a field type can be written to a lucene document.
   */
  public static boolean isSupported(Class<?> type) {
    return type == String.class || type == long.class || type == int.class 
        || type == float.class || type == double.class
        || type == Long.class || type == Integer.class 
        || type == Float.class || type == Double.class; 
  }
  
  /**
   * Extract the gemfire key from a lucene document
   */
  public static Object getKey(Document doc) {
    IndexableField field = doc.getField(KEY_FIELD);
    if(field.stringValue() != null) {
      return field.stringValue();
    } else {
      return  keyFromBytes(field.binaryValue());
    }
  }
 
  /**
   * Extract the gemfire key term from a lucene document
   */
  public static Term getKeyTerm(Document doc) {
    IndexableField field = doc.getField(KEY_FIELD);
    if(field.stringValue() != null) {
      return new Term(KEY_FIELD, field.stringValue());
    } else {
      return new Term(KEY_FIELD, field.binaryValue());
    }
  }
  
  /**
   * Convert a gemfire key into a key search term that can be used to
   * update or delete the document associated with this key.
   */
  public static Term toKeyTerm(Object key) {
    if(key instanceof String) {
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
  private static BytesRef keyToBytes(Object key)  {
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