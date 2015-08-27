package com.gemstone.gemfire.cache.lucene.internal.repository.serializer;

import java.lang.reflect.Field;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashSet;
import java.util.Set;

import org.apache.lucene.document.Document;

/**
 * A lucene serializer that handles a single class and can
 * map an instance of that class to a document using reflection.
 */
class ReflectionLuceneSerializer implements LuceneSerializer {

  private Field[] fields;

  public ReflectionLuceneSerializer(Class<? extends Object> clazz,
      String[] indexedFields) {
    Set<String> fieldSet = new HashSet<String>();
    fieldSet.addAll(Arrays.asList(indexedFields));

    //Iterate through all declared fields and save them
    //in a list if they are an indexed field and have the correct
    //type.
    ArrayList<Field> foundFields = new ArrayList<Field>();
    while(clazz != Object.class) {
      for(Field field : clazz.getDeclaredFields()) {
        Class<?> type = field.getType();
        if(fieldSet.contains(field.getName()) 
            && SerializerUtil.isSupported(type)) {
          field.setAccessible(true);
          foundFields.add(field);
        }
      }
      
      clazz = clazz.getSuperclass();
    }
    
    this.fields = foundFields.toArray(new Field[foundFields.size()]);
  }

  @Override
  public void toDocument(Object value, Document doc) {
    for(Field field: fields) {
      try {
        Object fieldValue = field.get(value);
        SerializerUtil.addField(doc, field.getName(), fieldValue);
      } catch (IllegalArgumentException | IllegalAccessException e) {
        //TODO - what to do if we can't read a field?
      }
    }
  }
}