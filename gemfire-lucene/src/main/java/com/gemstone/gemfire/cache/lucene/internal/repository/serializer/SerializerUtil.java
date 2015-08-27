package com.gemstone.gemfire.cache.lucene.internal.repository.serializer;

import java.io.IOException;

import org.apache.lucene.document.BinaryDocValuesField;
import org.apache.lucene.document.Document;
import org.apache.lucene.document.DoubleField;
import org.apache.lucene.document.Field;
import org.apache.lucene.document.Field.Store;
import org.apache.lucene.document.FloatField;
import org.apache.lucene.document.IntField;
import org.apache.lucene.document.LongField;
import org.apache.lucene.document.StoredField;
import org.apache.lucene.document.TextField;
import org.apache.lucene.index.Term;
import org.apache.lucene.util.BytesRef;

import com.gemstone.gemfire.DataSerializer;
import com.gemstone.gemfire.InternalGemFireError;
import com.gemstone.gemfire.internal.HeapDataOutputStream;
import com.gemstone.gemfire.internal.Version;
import com.gemstone.gemfire.internal.util.BlobHelper;

/**
 * Static utility functions for mapping objects to lucene documents
 */
public class SerializerUtil {
  private static final String KEY_FIELD = "_STORED_KEY";
  private static final String KEY_SEARCH_FIELD = "_SEARCH_KEY";
  
  /**
   * A small buffer for converting keys to byte[] arrays.
   */
  private static ThreadLocal<HeapDataOutputStream> buffer = new ThreadLocal<HeapDataOutputStream>() {
    @Override
    protected HeapDataOutputStream initialValue() {
      return new HeapDataOutputStream(Version.CURRENT);
    }
  };

  private SerializerUtil() {
  }
  
  /**
   * Add a gemfire key to a document
   */
  public static void addKey(Object key, Document doc) {
    BytesRef keyBytes = keyToBytes(key);
    doc.add(new BinaryDocValuesField(KEY_SEARCH_FIELD, keyBytes));
    doc.add(new StoredField(KEY_FIELD, keyBytes));
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
    try {
      return BlobHelper.deserializeBlob(doc.getField(KEY_FIELD).binaryValue().bytes);
    } catch (ClassNotFoundException | IOException e) {
      throw new InternalGemFireError("Unable to deserialize key", e);
    }
  }
 
  /**
   * Extract the gemfire key term from a lucene document
   */
  public static Term getKeyTerm(Document doc) {
    return new Term(KEY_SEARCH_FIELD, doc.getField(KEY_FIELD).binaryValue());
  }
  
  /**
   * Convert a gemfire key into a key search term that can be used to
   * update or delete the document associated with this key.
   */
  public static Term toKeyTerm(Object key) {
    return new Term(KEY_SEARCH_FIELD, keyToBytes(key));
  }
  
  /**
   * Convert a key to a byte array.
   */
  private static BytesRef keyToBytes(Object key)  {
    buffer.get().reset();
    try {
      DataSerializer.writeObject(key, buffer.get());
    } catch (IOException e) {
      throw new InternalGemFireError("Unable to serialize key", e);
    }
    return new BytesRef(buffer.get().toByteArray());
  }

}