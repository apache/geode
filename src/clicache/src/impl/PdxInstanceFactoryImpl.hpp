/*=========================================================================
 * Copyright (c) 2002-2014 Pivotal Software, Inc. All Rights Reserved.
 * This product is protected by U.S. and international copyright
 * and intellectual property laws. Pivotal products are covered by
 * more patents listed at http://www.pivotal.io/patents.
 *=========================================================================
 */

#pragma once

#include "../IPdxInstanceFactory.hpp"
#include "../IPdxSerializable.hpp"
#include "../DataInput.hpp"
#include "PdxLocalWriter.hpp"

namespace GemStone
{
  namespace GemFire
  {
    namespace Cache
    {
			namespace Generic
			{     
        namespace Internal
        {
        ref class PdxInstanceFactoryImpl : IPdxInstanceFactory
				{
        private:
          bool                          m_created;
          PdxType^                      m_pdxType;
          Dictionary<String^, Object^>^ m_FieldVsValues;
        internal:
          PdxInstanceFactoryImpl(String^ className);
          void isFieldAdded(String^ fieldName);
         public:
                  /// <summary>
         /// Create a {@link PdxInstance}. The instance
         /// will contain any data written to this factory
         /// using the write methods.
         /// @return the created instance
         /// @throws IllegalStateException if called more than once
         /// </summary>
        virtual IPdxInstance^ Create();

         /// <summary>   
         /// Writes the named field with the given value to the serialized form.
         /// The fields type is <code>char</code>.
         /// <p>Java char is mapped to .NET System.Char.
         /// @param fieldName the name of the field to write
         /// @param value the value of the field to write
         /// @return this PdxInstanceFactory
         /// @throws PdxFieldAlreadyExistsException if the named field has already been written
         /// @throws PdxSerializationException if serialization of the field fails.
         /// </summary>
         virtual IPdxInstanceFactory^ WriteChar(String^ fieldName, Char value);
   
         /// <summary>
         /// Writes the named field with the given value to the serialized form.
         /// The fields type is <code>boolean</code>.
         /// <p>Java boolean is mapped to .NET System.Boolean.
         /// @param fieldName the name of the field to write
         /// @param value the value of the field to write
         /// @return this PdxInstanceFactory
         /// @throws PdxFieldAlreadyExistsException if the named field has already been written
         /// @throws PdxSerializationException if serialization of the field fails.
         /// </summary>
         virtual IPdxInstanceFactory^ WriteBoolean(String^ fieldName, Boolean value);
    
         /// </summary>
         /// Writes the named field with the given value to the serialized form.
         /// The fields type is <code>byte</code>.
         /// <p>Java byte is mapped to .NET System.SByte.
         /// @param fieldName the name of the field to write
         /// @param value the value of the field to write
         /// @return this PdxInstanceFactory
         /// @throws PdxFieldAlreadyExistsException if the named field has already been written
         /// @throws PdxSerializationException if serialization of the field fails.
         /// </summary> 
         virtual IPdxInstanceFactory^ WriteByte(String^ fieldName, SByte value);
  
         /// <summary>
         /// Writes the named field with the given value to the serialized form.
         /// The fields type is <code>short</code>.
         /// <p>Java short is mapped to .NET System.Int16.
         /// @param fieldName the name of the field to write
         /// @param value the value of the field to write
         /// @return this PdxInstanceFactory
         /// @throws PdxFieldAlreadyExistsException if the named field has already been written
         /// @throws PdxSerializationException if serialization of the field fails.
         /// </summary>
         virtual IPdxInstanceFactory^ WriteShort(String^ fieldName, Int16 value);
  
         /// <summary>
         /// Writes the named field with the given value to the serialized form.
         /// The fields type is <code>int</code>.
         /// <p>Java int is mapped to .NET System.Int32.
         /// @param fieldName the name of the field to write
         /// @param value the value of the field to write
         /// @return this PdxInstanceFactory
         /// @throws PdxFieldAlreadyExistsException if the named field has already been written
         /// @throws PdxSerializationException if serialization of the field fails.
         /// </summary>
         virtual IPdxInstanceFactory^ WriteInt(String^ fieldName, Int32 value);
  
         /// <summary>
         /// Writes the named field with the given value to the serialized form.
         /// The fields type is <code>long</code>.
         /// <p>Java long is mapped to .NET System.Int64.
         /// @param fieldName the name of the field to write
         /// @param value the value of the field to write
         /// @return this PdxInstanceFactory
         /// @throws PdxFieldAlreadyExistsException if the named field has already been written
         /// @throws PdxSerializationException if serialization of the field fails.
         /// </summary> 
         virtual IPdxInstanceFactory^ WriteLong(String^ fieldName, Int64 value);
  
         /// <summary>
         /// Writes the named field with the given value to the serialized form.
         /// The fields type is <code>float</code>.
         /// <p>Java float is mapped to .NET System.Float.
         /// @param fieldName the name of the field to write
         /// @param value the value of the field to write
         /// @return this PdxInstanceFactory
         /// @throws PdxFieldAlreadyExistsException if the named field has already been written
         /// @throws PdxSerializationException if serialization of the field fails.
         /// </summary>
         virtual IPdxInstanceFactory^ WriteFloat(String^ fieldName, float value);
  
         /// <summary>
         /// Writes the named field with the given value to the serialized form.
         /// The fields type is <code>double</code>.
         /// <p>Java double is mapped to .NET System.Double.
         /// @param fieldName the name of the field to write
         /// @param value the value of the field to write
         /// @return this PdxInstanceFactory
         /// @throws PdxFieldAlreadyExistsException if the named field has already been written
         /// @throws PdxSerializationException if serialization of the field fails.
         /// </summary>
         virtual IPdxInstanceFactory^ WriteDouble(String^ fieldName, double value);
  
         /// <summary>
         /// Writes the named field with the given value to the serialized form.
         /// The fields type is <code>Date</code>.
         /// <p>Java Date is mapped to .NET System.DateTime.
         /// @param fieldName the name of the field to write
         /// @param value the value of the field to write
         /// @return this PdxInstanceFactory
         /// @throws PdxFieldAlreadyExistsException if the named field has already been written
         /// @throws PdxSerializationException if serialization of the field fails.
         /// </summary>
         virtual IPdxInstanceFactory^ WriteDate(String^ fieldName, System::DateTime value);
  
         /// <summary>
         /// Writes the named field with the given value to the serialized form.
         /// The fields type is <code>String</code>.
         /// <p>Java String is mapped to .NET System.String.
         /// @param fieldName the name of the field to write
         /// @param value the value of the field to write
         /// @return this PdxInstanceFactory
         /// @throws PdxFieldAlreadyExistsException if the named field has already been written
         /// @throws PdxSerializationException if serialization of the field fails.
         /// </summary>
         virtual IPdxInstanceFactory^ WriteString(String^ fieldName, String^ value);
        
         /// <summary>
         /// Writes the named field with the given value to the serialized form.
         /// The fields type is <code>Object</code>.
         /// <p>
         /// It is best to use one of the other writeXXX methods if your field type
         /// will always be XXX. This method allows the field value to be anything
         /// that is an instance of Object. This gives you more flexibility but more
         /// space is used to store the serialized field.
         /// <p>
         /// Note that some Java objects serialized with this method may not be compatible with non-java languages.
         /// To ensure that only portable objects are serialized use {@link #writeObject(String, Object, boolean)}.
         /// 
         /// @param fieldName the name of the field to write
         /// @param value the value of the field to write
         /// @return this PdxInstanceFactory
         /// @throws PdxFieldAlreadyExistsException if the named field has already been written
         /// @throws PdxSerializationException if serialization of the field fails.
         /// </summary>
         virtual IPdxInstanceFactory^ WriteObject(String^ fieldName, Object^ value);  
  
         /// <summary>
         /// Writes the named field with the given value to the serialized form.
         /// The fields type is <code>boolean[]</code>.
         /// <p>Java boolean[] is mapped to .NET System.Boolean[].
         /// @param fieldName the name of the field to write
         /// @param value the value of the field to write
         /// @return this PdxInstanceFactory
         /// @throws PdxFieldAlreadyExistsException if the named field has already been written
         /// @throws PdxSerializationException if serialization of the field fails.
         /// </summary>
         virtual IPdxInstanceFactory^ WriteBooleanArray(String^ fieldName, array<Boolean>^ value);
  
         /// <summary>
         /// Writes the named field with the given value to the serialized form.
         /// The fields type is <code>char[]</code>.
         /// <p>Java char[] is mapped to .NET System.Char[].
         /// @param fieldName the name of the field to write
         /// @param value the value of the field to write
         /// @return this PdxInstanceFactory
         /// @throws PdxFieldAlreadyExistsException if the named field has already been written
         /// @throws PdxSerializationException if serialization of the field fails.
         /// </summary>
         virtual IPdxInstanceFactory^ WriteCharArray(String^ fieldName, array<Char>^ value);
        
         /// <summary>
         /// Writes the named field with the given value to the serialized form.
         /// The fields type is <code>byte[]</code>.
         /// <p>Java byte[] is mapped to .NET System.Byte[].
         /// @param fieldName the name of the field to write
         /// @param value the value of the field to write
         /// @return this PdxInstanceFactory
         /// @throws PdxFieldAlreadyExistsException if the named field has already been written
         /// @throws PdxSerializationException if serialization of the field fails.
         /// </summary>
         virtual IPdxInstanceFactory^ WriteByteArray(String^ fieldName, array<Byte>^ value);
        
         /// <summary>
         /// Writes the named field with the given value to the serialized form.
         /// The fields type is <code>short[]</code>.
         /// <p>Java short[] is mapped to .NET System.Int16[].
         /// @param fieldName the name of the field to write
         /// @param value the value of the field to write
         /// @return this PdxInstanceFactory
         /// @throws PdxFieldAlreadyExistsException if the named field has already been written
         /// @throws PdxSerializationException if serialization of the field fails.
         /// </summary>
         virtual IPdxInstanceFactory^ WriteShortArray(String^ fieldName, array<Int16>^ value);
        
         /// <summary>
         /// Writes the named field with the given value to the serialized form.
         /// The fields type is <code>int[]</code>.
         /// <p>Java int[] is mapped to .NET System.Int32[].
         /// @param fieldName the name of the field to write
         /// @param value the value of the field to write
         /// @return this PdxInstanceFactory
         /// @throws PdxFieldAlreadyExistsException if the named field has already been written
         /// @throws PdxSerializationException if serialization of the field fails.
         /// </summary>
         virtual IPdxInstanceFactory^ WriteIntArray(String^ fieldName, array<Int32>^ value);
        
         /// <summary>
         /// Writes the named field with the given value to the serialized form.
         /// The fields type is <code>long[]</code>.
         /// <p>Java long[] is mapped to .NET System.Int64[].
         /// @param fieldName the name of the field to write
         /// @param value the value of the field to write
         /// @return this PdxInstanceFactory
         /// @throws PdxFieldAlreadyExistsException if the named field has already been written
         /// @throws PdxSerializationException if serialization of the field fails.
         /// </summary>
         virtual  IPdxInstanceFactory^ WriteLongArray(String^ fieldName, array<Int64>^ value);
        
         /// <summary>
         /// Writes the named field with the given value to the serialized form.
         /// The fields type is <code>float[]</code>.
         /// <p>Java float[] is mapped to .NET System.Float[].
         /// @param fieldName the name of the field to write
         /// @param value the value of the field to write
         /// @return this PdxInstanceFactory
         /// @throws PdxFieldAlreadyExistsException if the named field has already been written
         /// @throws PdxSerializationException if serialization of the field fails.
         /// </summary>
         virtual IPdxInstanceFactory^ WriteFloatArray(String^ fieldName, array<float>^ value);
        
         /// <summary>
         /// Writes the named field with the given value to the serialized form.
         /// The fields type is <code>double[]</code>.
         /// <p>Java double[] is mapped to .NET System.Double[].
         /// @param fieldName the name of the field to write
         /// @param value the value of the field to write
         /// @return this PdxInstanceFactory
         /// @throws PdxFieldAlreadyExistsException if the named field has already been written
         /// @throws PdxSerializationException if serialization of the field fails.
         /// </summary>
         virtual IPdxInstanceFactory^ WriteDoubleArray(String^ fieldName, array<double>^ value);
        
         /// <summary>
         /// Writes the named field with the given value to the serialized form.
         /// The fields type is <code>String[]</code>.
         /// <p>Java String[] is mapped to .NET System.String[].
         /// @param fieldName the name of the field to write
         /// @param value the value of the field to write
         /// @return this PdxInstanceFactory
         /// @throws PdxFieldAlreadyExistsException if the named field has already been written
         /// @throws PdxSerializationException if serialization of the field fails.
         /// </summary>
         virtual IPdxInstanceFactory^ WriteStringArray(String^ fieldName, array<String^>^ value);
        
         /// <summary>
         /// Writes the named field with the given value to the serialized form.
         /// The fields type is <code>Object[]</code>.
         /// <p>Java Object[] is mapped to .NET System.Collections.Generic.List<Object>.
         /// For how each element of the array is a mapped to .NET see {@link #writeObject(String, Object, boolean) writeObject}.
         /// Note that this call may serialize elements that are not compatible with non-java languages.
         /// To ensure that only portable objects are serialized use {@link #writeObjectArray(String, Object[], boolean)}.
         /// @param fieldName the name of the field to write
         /// @param value the value of the field to write
         /// @return this PdxInstanceFactory
         /// @throws PdxFieldAlreadyExistsException if the named field has already been written
         /// @throws PdxSerializationException if serialization of the field fails.
         /// </summary>
         virtual IPdxInstanceFactory^ WriteObjectArray(String^ fieldName, System::Collections::Generic::List<Object^>^ value);              
         /// <summary>
         /// Writes the named field with the given value to the serialized form.
         /// The fields type is <code>byte[][]</code>.
         /// <p>Java byte[][] is mapped to .NET System.Byte[][].
         /// @param fieldName the name of the field to write
         /// @param value the value of the field to write
         /// @return this PdxInstanceFactory
         /// @throws PdxFieldAlreadyExistsException if the named field has already been written
         /// @throws PdxSerializationException if serialization of the field fails.
         /// </summary>
         virtual IPdxInstanceFactory^ WriteArrayOfByteArrays(String^ fieldName, array<array<Byte>^>^ value);
        
         /// <summary>
         /// Writes the named field with the given value and type to the serialized form.
         /// This method uses the <code>fieldType</code> to determine which writeXXX method it should call.
         /// If it can not find a specific match to a writeXXX method it will call {@link #writeObject(String, Object) writeObject}.
         /// This method may serialize objects that are not portable to non-java languages.
         /// To ensure that only objects that are portable to non-java languages are serialized use {@link #writeField(String, Object, Class, boolean)} instead.
         /// <p>The fieldTypes that map to a specific method are:
         /// <ul>
         /// <li>boolean.class: {@link #writeBoolean}
         /// <li>byte.class: {@link #writeByte}
         /// <li>char.class: {@link #writeChar}
         /// <li>short.class: {@link #writeShort}
         /// <li>int.class: {@link #writeInt}
         /// <li>long.class: {@link #writeLong}
         /// <li>float.class: {@link #writeFloat}
         /// <li>double.class: {@link #writeDouble}
         /// <li>String.class: {@link #writeString}
         /// <li>Date.class: {@link #writeDate}
         /// <li>boolean[].class: {@link #writeBooleanArray}
         /// <li>byte[].class: {@link #writeByteArray}
         /// <li>char[].class: {@link #writeCharArray}
         /// <li>short[].class: {@link #writeShortArray}
         /// <li>int[].class: {@link #writeIntArray}
         /// <li>long[].class: {@link #writeLongArray}
         /// <li>float[].class: {@link #writeFloatArray}
         /// <li>double[].class: {@link #writeDoubleArray}
         /// <li>String[].class: {@link #writeStringArray}
         /// <li>byte[][].class: {@link #writeArrayOfByteArrays}
         /// <li>any other array class: {@link #writeObjectArray}
         /// </ul>
         /// Note that the object form of primitives, for example Integer.class and Long.class, map to {@link #writeObject(String, Object) writeObject}.
         /// @param fieldName the name of the field to write
         /// @param fieldValue the value of the field to write; this parameter's class must extend the <code>fieldType</code>
         /// @param fieldType the type of the field to write
         /// @return this PdxInstanceFactory
         /// @throws PdxFieldAlreadyExistsException if the named field has already been written
         /// @throws PdxSerializationException if serialization of the field fails.
         /// <summary>
         virtual IPdxInstanceFactory^ WriteField(String^ fieldName, Object^ fieldValue, Type^ fieldType);
            
         /// <summary>
         /// Indicate that the named field should be included in hashCode and equals checks
         /// of this object on a server that is accessing {@link PdxInstance}
         /// or when a client executes a query on a server.
         /// 
         /// The fields that are marked as identity fields are used to generate the hashCode and
         /// equals methods of {@link PdxInstance}. Because of this, the identity fields should themselves
         /// either be primitives, or implement hashCode and equals.
         /// 
         /// If no fields are set as identity fields, then all fields will be used in hashCode and equals
         /// checks.
         /// 
         /// The identity fields should make marked after they are written using a write/// method.
         /// 
         /// @param fieldName the name of the field to mark as an identity field.
         /// @return this PdxInstanceFactory
         /// @throws PdxFieldDoesNotExistException if the named field has not already been written.
         /// </summary>
         virtual IPdxInstanceFactory^ MarkIdentityField(String^ fieldName);
   

				};
        }
			}
    }
  }
}
