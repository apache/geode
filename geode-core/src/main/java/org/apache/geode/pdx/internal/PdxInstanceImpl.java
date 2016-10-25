/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.geode.pdx.internal;

import static org.apache.logging.log4j.message.MapMessage.MapFormat.JSON;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.text.SimpleDateFormat;
import java.util.Arrays;
import java.util.Collection;
import java.util.Iterator;
import java.util.List;
import java.util.SortedSet;
import java.util.TreeSet;

import com.fasterxml.jackson.databind.DeserializationFeature;
import com.fasterxml.jackson.databind.ObjectMapper;

import org.apache.commons.lang.StringUtils;
import org.apache.geode.InternalGemFireException;
import org.apache.geode.distributed.internal.DMStats;
import org.apache.geode.internal.ClassPathLoader;
import org.apache.geode.internal.DSCODE;
import org.apache.geode.internal.InternalDataSerializer;
import org.apache.geode.internal.Sendable;
import org.apache.geode.internal.cache.GemFireCacheImpl;
import org.apache.geode.internal.tcp.ByteBufferInputStream.ByteSource;
import org.apache.geode.internal.tcp.ByteBufferInputStream.ByteSourceFactory;
import org.apache.geode.pdx.JSONFormatter;
import org.apache.geode.pdx.PdxInstance;
import org.apache.geode.pdx.PdxSerializationException;
import org.apache.geode.pdx.WritablePdxInstance;

/**
 * Implementation code in this class must be careful to not directly call
 * super class state. Instead it must call {@link #getUnmodifiableReader()} and
 * access the super class state using it.
 * This class could be changed to not extend PdxReaderImpl but to instead have
 * an instance variable that is a PdxReaderImpl but that would cause this class
 * to use more memory.
 * 
 * We do not use this normal java io serialization
 * when serializing this class in GemFire because Sendable takes precedence over Serializable.
 * 
 *
 */
public class PdxInstanceImpl extends PdxReaderImpl implements PdxInstance, Sendable, ConvertableToBytes {

  private static final long serialVersionUID = -1669268527103938431L;

  private static final boolean USE_STATIC_MAPPER = Boolean.getBoolean("PdxInstance.use-static-mapper");

  static final ObjectMapper mapper = USE_STATIC_MAPPER? createObjectMapper() : null;

  private static ObjectMapper createObjectMapper() {
    ObjectMapper mapper = new ObjectMapper();
    mapper.setDateFormat(new SimpleDateFormat("MM/dd/yyyy"));
    mapper.configure(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES, false);
    mapper.configure(com.fasterxml.jackson.core.JsonParser.Feature.ALLOW_UNQUOTED_FIELD_NAMES, true);
    return mapper;
  }

  private transient volatile Object cachedObjectForm;

  /**
   * Computes the hash code once and stores it. This is added to address the
   * issue of identity value getting changed for each hash code call (as new objects
   * instances are created in each call for the same object). 
   * The value 0 means the hash code is not yet computed (this avoids using 
   * extra variable for this), if the computation returns 0, it will be set to 1. This 
   * doesn't break the equality rule, where hash code can be same for non-equal
   * objects. 
   */
  private static final int UNUSED_HASH_CODE = 0;
  private transient volatile int cachedHashCode = UNUSED_HASH_CODE;
  
  private static final ThreadLocal<Boolean> pdxGetObjectInProgress = new ThreadLocal<Boolean>();
  
  public PdxInstanceImpl(PdxType pdxType, DataInput in, int len) {
    super(pdxType, createDis(in, len));
  }
  
  protected PdxInstanceImpl(PdxReaderImpl original) {
    super(original);
  }
  
  private static PdxInputStream createDis(DataInput in, int len) {
    PdxInputStream dis;
    if (in instanceof PdxInputStream) {
      dis = new PdxInstanceInputStream((PdxInputStream) in, len);
      try {
        int bytesSkipped = in.skipBytes(len);
        int bytesRemaining = len - bytesSkipped;
        while (bytesRemaining > 0) {
          in.readByte();
          bytesRemaining--;
        }
      } catch (IOException ex) {
        throw new PdxSerializationException("Could not deserialize PDX", ex);
      }
    } else {
      byte[] bytes = new byte[len];
      try {
        in.readFully(bytes);
      } catch (IOException ex) {
        throw new PdxSerializationException("Could not deserialize PDX", ex);
      }
      dis = new PdxInstanceInputStream(bytes);
    }
    return dis;
  }
  
  public static boolean getPdxReadSerialized() {
    return pdxGetObjectInProgress.get() == null;
  }

  public static void setPdxReadSerialized(boolean readSerialized) {
    if (!readSerialized) {
      pdxGetObjectInProgress.set(true);
    } else {
      pdxGetObjectInProgress.remove();
    }
  }
  
  public Object getField(String fieldName) {
    return getUnmodifiableReader(fieldName).readField(fieldName);
  }
  
  private PdxWriterImpl convertToTypeWithNoDeletedFields(PdxReaderImpl ur) {
    PdxOutputStream os = new PdxOutputStream();
    PdxType pt = new PdxType(ur.getPdxType().getClassName(), !ur.getPdxType().getNoDomainClass());
    GemFireCacheImpl gfc = GemFireCacheImpl.getForPdx("PDX registry is unavailable because the Cache has been closed.");
    TypeRegistry tr = gfc.getPdxRegistry();
    PdxWriterImpl writer = new PdxWriterImpl(pt, tr, os);
    for (PdxField field: pt.getFields()) {
      if (!field.isDeleted()) {
        writer.writeRawField(field, ur.getRaw(field));
      }
    }
    writer.completeByteStreamGeneration();
    return writer;
  }
  
  // Sendable implementation
  public void sendTo(DataOutput out) throws IOException {
    PdxReaderImpl ur = getUnmodifiableReader();
    if (ur.getPdxType().getHasDeletedField()) {
      PdxWriterImpl writer = convertToTypeWithNoDeletedFields(ur);
      writer.sendTo(out);
    } else {
    out.write(DSCODE.PDX);
    out.writeInt(ur.basicSize());
    out.writeInt(ur.getPdxType().getTypeId());
    ur.basicSendTo(out);
    }
  }
  
  public byte[] toBytes() {
    PdxReaderImpl ur = getUnmodifiableReader();
    if (ur.getPdxType().getHasDeletedField()) {
      PdxWriterImpl writer = convertToTypeWithNoDeletedFields(ur);
      return writer.toByteArray();
    } else {
    byte[] result = new byte[PdxWriterImpl.HEADER_SIZE + ur.basicSize()];
    ByteBuffer bb = ByteBuffer.wrap(result);
    bb.put(DSCODE.PDX);
    bb.putInt(ur.basicSize());
    bb.putInt(ur.getPdxType().getTypeId());
    ur.basicSendTo(bb);
    return result;
    }
  }

  // this is for internal use of the query engine.
  public Object getCachedObject() {
    Object result = this.cachedObjectForm; 
    if (result == null) {
      result = getObject();
      this.cachedObjectForm = result;
    }
    return result;
  }
    
  private String extractTypeMetaData(){
    Object type = this.getField("@type");
    if (type != null) {
      if (type instanceof String) {
         return (String)type;
      }else {
        throw new PdxSerializationException("Could not deserialize as invalid className found");
      }
    }else {
     return null; 
    }  
  }

  @Override
  public Object getObject() {
    if (getPdxType().getNoDomainClass()) { 
      //In case of Developer Rest APIs, All PdxInstances converted from Json will have a className =__GEMFIRE_JSON.
      //Following code added to convert Json/PdxInstance into the Java object.
      if(this.getClassName().equals("__GEMFIRE_JSON")){
        
        //introspect the JSON, does the @type meta-data exist.
        String className = extractTypeMetaData();
        
        if(StringUtils.isNotBlank(className)) {
          try {
            String JSON = JSONFormatter.toJSON(this);
            ObjectMapper objMapper = USE_STATIC_MAPPER? mapper : createObjectMapper();
            Object classInstance = objMapper.readValue(JSON, ClassPathLoader.getLatest().forName(className));
            return classInstance;
          }catch(Exception e){
            throw new PdxSerializationException("Could not deserialize as java class type could not resolved", e);
          }
        }
      }
      return this;
    }
    boolean wouldReadSerialized = PdxInstanceImpl.getPdxReadSerialized();
    if (!wouldReadSerialized) {
      return getUnmodifiableReader().basicGetObject();
    } else {
      PdxInstanceImpl.setPdxReadSerialized(false);
      try {
        return getUnmodifiableReader().basicGetObject();
      } finally {
        PdxInstanceImpl.setPdxReadSerialized(true);
      }
    }
  }
  
  @Override
  public int hashCode() {
    if (this.cachedHashCode != UNUSED_HASH_CODE) {
      // Already computed.
      return this.cachedHashCode;
    }
    PdxReaderImpl ur = getUnmodifiableReader();
    
    // Compute hash code.
    Collection<PdxField> fields = ur.getPdxType().getSortedIdentityFields();
    int hashCode = 1;
    for(PdxField ft: fields) {
      switch (ft.getFieldType()) {
      case CHAR:
      case BOOLEAN:
      case BYTE:
      case SHORT:
      case INT:
      case LONG:
      case DATE:
      case FLOAT:
      case DOUBLE:
      case STRING:
      case BOOLEAN_ARRAY:
      case CHAR_ARRAY:
      case BYTE_ARRAY:
      case SHORT_ARRAY:
      case INT_ARRAY:
      case LONG_ARRAY:
      case FLOAT_ARRAY:
      case DOUBLE_ARRAY:
      case STRING_ARRAY:
      case ARRAY_OF_BYTE_ARRAYS: {
        ByteSource buffer = ur.getRaw(ft);
        if (!buffer.equals(ByteSourceFactory.create(ft.getFieldType().getDefaultBytes()))) {
          hashCode = hashCode *31  + buffer.hashCode();
        }
        break;
      }
      case OBJECT_ARRAY: {
        Object[] oArray = ur.readObjectArray(ft);
        if (oArray != null) {
          // default value of null does not modify hashCode.
          hashCode = hashCode *31  + Arrays.deepHashCode(oArray);
        }
        break;
      }
      case OBJECT: {
        Object objectValue = ur.readObject(ft);
        if (objectValue == null) {
          // default value of null does not modify hashCode.
        } else if (objectValue.getClass().isArray()) {
          Class<?> myComponentType = objectValue.getClass().getComponentType();
          if (myComponentType.isPrimitive()) {
            ByteSource buffer = getRaw(ft);
            hashCode = hashCode *31  + buffer.hashCode();
          } else {
            hashCode = hashCode *31  + Arrays.deepHashCode((Object[])objectValue);
          }
        } else {
          hashCode = hashCode *31  + objectValue.hashCode();
        }
        break;
      }
      default:
        throw new InternalGemFireException("Unhandled field type " + ft.getFieldType());
      }
    }
    int result = (hashCode == UNUSED_HASH_CODE)?(hashCode + 1):hashCode;
    this.cachedHashCode = result;
    return result;
  }
  
  @Override
  public boolean equals(Object obj) {
    if (obj == this) return true;
    
    if(obj == null) {
      //GemFireCacheImpl.getInstance().getLogger().info("DEBUG equals#0 o1=<" + this + "> o2=<" + obj + ">");
      return false;
    }
    if (!(obj instanceof PdxInstanceImpl)) {
      //      if (!result) {
      //        GemFireCacheImpl.getInstance().getLogger().info("DEBUG equals#1 o1=<" + this + "> o2=<" + obj + ">");
      //      }
      return false;
    }
    final PdxInstanceImpl other = (PdxInstanceImpl) obj;
    PdxReaderImpl ur2 = other.getUnmodifiableReader();
    PdxReaderImpl ur1 = getUnmodifiableReader();
    
    if(!ur1.getPdxType().getClassName().equals(ur2.getPdxType().getClassName())) {
      //GemFireCacheImpl.getInstance().getLogger().info("DEBUG equals#2 o1=<" + this + "> o2=<" + obj + ">");
      return false;
    }
    
    SortedSet<PdxField> myFields = ur1.getPdxType().getSortedIdentityFields();
    SortedSet<PdxField> otherFields = ur2.getPdxType().getSortedIdentityFields();
    if (!myFields.equals(otherFields)) {
      // It is not ok to modify myFields and otherFields in place so make copies
      myFields = new TreeSet<PdxField>(myFields);
      otherFields = new TreeSet<PdxField>(otherFields);
      addDefaultFields(myFields, otherFields);
      addDefaultFields(otherFields, myFields);
    }

    
      Iterator<PdxField> myFieldIterator = myFields.iterator();
      Iterator<PdxField> otherFieldIterator = otherFields.iterator();
      while(myFieldIterator.hasNext()) {
        PdxField myType = myFieldIterator.next();
        PdxField otherType = otherFieldIterator.next();

        switch (myType.getFieldType()) {
        case CHAR:
        case BOOLEAN:
        case BYTE:
        case SHORT:
        case INT:
        case LONG:
        case DATE:
        case FLOAT:
        case DOUBLE:
        case STRING:
        case BOOLEAN_ARRAY:
        case CHAR_ARRAY:
        case BYTE_ARRAY:
        case SHORT_ARRAY:
        case INT_ARRAY:
        case LONG_ARRAY:
        case FLOAT_ARRAY:
        case DOUBLE_ARRAY:
        case STRING_ARRAY:
        case ARRAY_OF_BYTE_ARRAYS: {
          ByteSource myBuffer = ur1.getRaw(myType);
          ByteSource otherBuffer = ur2.getRaw(otherType);
          if(!myBuffer.equals(otherBuffer)) {
            //GemFireCacheImpl.getInstance().getLogger().info("DEBUG equals#4 o1=<" + this + "> o2=<" + obj + ">");
            return false;
          }}
        break;

        case OBJECT_ARRAY: {
          Object[] myArray = ur1.readObjectArray(myType);
          Object[] otherArray = ur2.readObjectArray(otherType);
          if(!Arrays.deepEquals(myArray, otherArray)) {
            //GemFireCacheImpl.getInstance().getLogger().info("DEBUG equals#5 o1=<" + this + "> o2=<" + obj + ">");
            return false;
          }}
        break;

        case OBJECT: {
          Object myObject = ur1.readObject(myType);
          Object otherObject = ur2.readObject(otherType);
          if(myObject != otherObject) {
            if(myObject == null) {
              //GemFireCacheImpl.getInstance().getLogger().info("DEBUG equals#6 o1=<" + this + "> o2=<" + obj + ">");
              return false;
            }
            if(otherObject == null) {
              //GemFireCacheImpl.getInstance().getLogger().info("DEBUG equals#7 o1=<" + this + "> o2=<" + obj + ">");
              return false;
            }
            if (myObject.getClass().isArray()) { // for bug 42976
              Class<?> myComponentType = myObject.getClass().getComponentType();
              Class<?> otherComponentType = otherObject.getClass().getComponentType();
              if (!myComponentType.equals(otherComponentType)) {
                //GemFireCacheImpl.getInstance().getLogger().info("DEBUG equals#8 o1=<" + this + "> o2=<" + obj + ">");
                return false;
              }
              if (myComponentType.isPrimitive()) {
                ByteSource myBuffer = getRaw(myType);
                ByteSource otherBuffer = other.getRaw(otherType);
                if(!myBuffer.equals(otherBuffer)) {
                  //GemFireCacheImpl.getInstance().getLogger().info("DEBUG equals#9 o1=<" + this + "> o2=<" + obj + ">");
                  return false;
                }
              } else {
                if (!Arrays.deepEquals((Object[])myObject, (Object[])otherObject)) {
                  //GemFireCacheImpl.getInstance().getLogger().info("DEBUG equals#10 o1=<" + this + "> o2=<" + obj + ">");
                  return false;
                }
              }
            } else if (!myObject.equals(otherObject)) {
              //GemFireCacheImpl.getInstance().getLogger().info("DEBUG equals#11 fn=" + myType.getFieldName() + " myFieldClass=" + myObject.getClass() + " otherFieldCLass=" + otherObject.getClass() + " o1=<" + this + "> o2=<" + obj + ">" + "myObj=<" + myObject + "> otherObj=<" + otherObject + ">");
              return false;
            }
          }}
        break;

        default:
          throw new InternalGemFireException("Unhandled field type " + myType.getFieldType());
        }
      }
      return true;
  }
    

  /**
   * Any fields that are in otherFields but not in myFields
   * are added to myFields as defaults. When adding fields they
   * are inserted in the natural sort order.
   * Note: myFields may be modified by this call.
   */
  private static void addDefaultFields(SortedSet<PdxField> myFields, SortedSet<PdxField> otherFields) {
    for (PdxField f: otherFields) {
      if (!myFields.contains(f)) {
        myFields.add(new DefaultPdxField(f));
      }
    }
  }

  @Override
  public String toString() {
    StringBuilder result = new StringBuilder();
    PdxReaderImpl ur = getUnmodifiableReader();
    result.append("PDX[").append(ur.getPdxType().getTypeId()).append(",").append(ur.getPdxType().getClassName())
    //.append(",limit=").append(this.dis.size())
    .append("]{");
    boolean firstElement = true;
    for(PdxField fieldType : ur.getPdxType().getSortedIdentityFields()) {
      if(firstElement) {firstElement= false;} else {result.append(", ");};
      result.append(fieldType.getFieldName());
      //result.append(':').append(fieldType.getTypeIdString()); // DEBUG
      //result.append(':').append(getAbsolutePosition(fieldType)); // DEBUG
      result.append("=");
      try {
        // TODO check to see if getField returned an array and if it did use Arrays.deepToString
        result.append(ur.readField(fieldType.getFieldName()));
      } catch (RuntimeException e) {
        result.append(e);
      }
    }
    result.append("}");
    return result.toString();
  }

  public List<String> getFieldNames() {
    return getPdxType().getFieldNames();
  }
  
  @Override
  PdxUnreadData getReadUnreadFieldsCalled() {
    return null;
  }
  protected void clearCachedState() {
    this.cachedHashCode = UNUSED_HASH_CODE;
    this.cachedObjectForm = null;
  }

  public WritablePdxInstance createWriter() {
    if (isEnum()) {
      throw new IllegalStateException("PdxInstances that are an enum can not be modified.");
    }
    return new WritablePdxInstanceImpl(getUnmodifiableReader());
  }
  
  protected PdxReaderImpl getUnmodifiableReader() {
    return this;
  }
  protected PdxReaderImpl getUnmodifiableReader(String fieldName) {
    return this;
  }

  // All PdxReaderImpl methods that might change the ByteBuffer position
  // need to be synchronized so that they are done atomically.
  // This fixes bug 43178.
  
  // primitive read methods all use absolute read methods so they do not set position
  // so no sync is needed. Fixed width fields all use absolute read methods.

  @Override
  public synchronized String readString(String fieldName) {
    return super.readString(fieldName);
  }

  @Override
  public synchronized Object readObject(String fieldName) {
    return super.readObject(fieldName);
  }

  @Override
  public
  synchronized Object readObject(PdxField ft) {
    return super.readObject(ft);
  }

  @Override
  public synchronized char[] readCharArray(String fieldName) {
    return super.readCharArray(fieldName);
  }

  @Override
  public synchronized boolean[] readBooleanArray(String fieldName) {
    return super.readBooleanArray(fieldName);
  }

  @Override
  public synchronized byte[] readByteArray(String fieldName) {
    return super.readByteArray(fieldName);
  }

  @Override
  public synchronized short[] readShortArray(String fieldName) {
    return super.readShortArray(fieldName);
  }

  @Override
  public synchronized int[] readIntArray(String fieldName) {
    return super.readIntArray(fieldName);
  }

  @Override
  public synchronized long[] readLongArray(String fieldName) {
    return super.readLongArray(fieldName);
  }

  @Override
  public synchronized float[] readFloatArray(String fieldName) {
    return super.readFloatArray(fieldName);
  }

  @Override
  public synchronized double[] readDoubleArray(String fieldName) {
    return super.readDoubleArray(fieldName);
  }

  @Override
  public synchronized String[] readStringArray(String fieldName) {
    return super.readStringArray(fieldName);
  }

  @Override
  public synchronized Object[] readObjectArray(String fieldName) {
    return super.readObjectArray(fieldName);
  }

  @Override
  public synchronized Object[] readObjectArray(PdxField ft) {
    return super.readObjectArray(ft);
  }

  @Override
  public synchronized byte[][] readArrayOfByteArrays(String fieldName) {
    return super.readArrayOfByteArrays(fieldName);
  }

  @Override
  public synchronized Object readField(String fieldName) {
    return super.readField(fieldName);
  }

  @Override
  protected synchronized Object basicGetObject() {
    DMStats stats = InternalDataSerializer.getDMStats(null);
    long start = stats.startPdxInstanceDeserialization();
    try {
      return super.basicGetObject();
    } finally {
      stats.endPdxInstanceDeserialization(start);
    }
  }

  // override getRaw to fix bug 43569
  @Override
  protected synchronized ByteSource getRaw(PdxField ft) {
    return super.getRaw(ft);
  }

  @Override
  protected synchronized void basicSendTo(DataOutput out)  throws IOException {
    super.basicSendTo(out);
  }
  
  @Override
  protected synchronized void basicSendTo(ByteBuffer bb) {
    super.basicSendTo(bb);
  }

  public String getClassName() {
    return getPdxType().getClassName();
  }

  public boolean isEnum() {
    return false;
  }
  
  public Object getRawField(String fieldName){
    return getUnmodifiableReader(fieldName).readRawField(fieldName);
  }
  
  
 public Object getDefaultValueIfFieldExistsInAnyPdxVersions(String fieldName,
     String className) throws FieldNotFoundInPdxVersion {
   PdxType pdxType = GemFireCacheImpl
       .getForPdx(
           "PDX registry is unavailable because the Cache has been closed.")
       .getPdxRegistry().getPdxTypeForField(fieldName, className);
   if (pdxType == null) {
     throw new FieldNotFoundInPdxVersion("PdxType with field "
         + fieldName + " is not found for class " + className);
   }
   return pdxType.getPdxField(fieldName).getFieldType().getDefaultValue();
 }

}
