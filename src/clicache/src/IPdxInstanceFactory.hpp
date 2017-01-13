/*=========================================================================
 ///Copyright (c) 2002-2014 Pivotal Software, Inc. All Rights Reserved.
 * This product is protected by U.S. and international copyright
 * and intellectual property laws. Pivotal products are covered by
 * more patents listed at http://www.pivotal.io/patents.
 *=========================================================================
 */

#pragma once
#include "IPdxInstance.hpp"
using namespace System;
using namespace System::Collections::Generic;
namespace GemStone
{
  namespace GemFire
  {
    namespace Cache
    {
		  namespace Generic
			{         
			
        /// <summary>
        /// PdxInstanceFactory gives you a way to create PdxInstances.
        /// Call the write methods to populate the field data and then call <see cref="Create"/>
        /// to produce an actual instance that contains the data.
        /// To create a factory call <see cref="IRegionService.CreatePdxInstanceFactory"/>.
        /// A factory can only create a single instance. To create multiple instances create
        /// multiple factories or use <see cref="IPdxInstance.CreateWriter" /> to create subsequent instances.
        /// 
        /// </summary>
        public interface class IPdxInstanceFactory 
        {
        public:
         /// <summary>
         /// Create a <see cref="IPdxInstance" /> . The instance
         /// will contain any data written to this factory
         /// using the write methods.
          /// </summary>
          /// <returns> the created instance</returns>
         /// <exception cref="IllegalStateException"/> if called more than once </exception>
         
        IPdxInstance^ Create();

         /// <summary>   
         /// Writes the named field with the given value to the serialized form.
         /// The fields type is <code>char</code>.
        /// <para>Java char is mapped to .NET System.Char.</para>
         /// </summary>
         /// <param name="fieldName"> the name of the field to write</param>
         /// <param name="value"> the value of the field to write</param>
         /// <returns> this PdxInstanceFactory</returns>
         /// <exception cref="IllegalStateException"/> if the named field has already been written</exception>
         
         IPdxInstanceFactory^ WriteChar(String^ fieldName, Char value);
   
         /// <summary>
         /// Writes the named field with the given value to the serialized form.
         /// The fields type is <code>boolean</code>.
         /// <para>Java boolean is mapped to .NET System.Boolean.</para>
         /// </summary>
         /// <param name="fieldName"> the name of the field to write</param>
         /// <param name="value"> the value of the field to write</param>
         /// <returns> this PdxInstanceFactory</returns>
         /// <exception cref="IllegalStateException"/> if the named field has already been written</exception>
         
         IPdxInstanceFactory^ WriteBoolean(String^ fieldName, Boolean value);
    
         /// <summary>
         /// Writes the named field with the given value to the serialized form.
         /// The fields type is <code>sbyte</code>.
         /// <para>Java byte is mapped to .NET System.SByte.</para>
         /// </summary>
         /// <param name="fieldName"> the name of the field to write</param>
         /// <param name="value"> the value of the field to write</param>
         /// <returns> this PdxInstanceFactory</returns>
         /// <exception cref="IllegalStateException"/> if the named field has already been written</exception>
          
         IPdxInstanceFactory^ WriteByte(String^ fieldName, SByte value);
  
         /// <summary>
         /// Writes the named field with the given value to the serialized form.
         /// The fields type is <code>short</code>.
         /// <para>Java short is mapped to .NET System.Int16.</para>
         /// </summary>
         /// <param name="fieldName"> the name of the field to write</param>
         /// <param name="value"> the value of the field to write</param>
         /// <returns> this PdxInstanceFactory</returns>
         /// <exception cref="IllegalStateException"/> if the named field has already been written</exception>
         IPdxInstanceFactory^ WriteShort(String^ fieldName, Int16 value);
  
         /// <summary>
         /// Writes the named field with the given value to the serialized form.
         /// The fields type is <code>int</code>.
         /// <para>Java int is mapped to .NET System.Int32.</para>
         /// </summary>
         /// <param name="fieldName"> the name of the field to write</param>
         /// <param name="value"> the value of the field to write</param>
         /// <returns> this PdxInstanceFactory</returns>
         /// <exception cref="IllegalStateException"/> if the named field has already been written</exception>
         IPdxInstanceFactory^ WriteInt(String^ fieldName, Int32 value);
  
         /// <summary>
         /// Writes the named field with the given value to the serialized form.
         /// The fields type is <code>long</code>.
         /// <para>Java long is mapped to .NET System.Int64.</para>
         /// <param name="fieldName"> the name of the field to write</param>
         /// <param name="value"> the value of the field to write</param>
         /// <returns> this PdxInstanceFactory</returns>
         /// <exception cref="IllegalStateException"/> if the named field has already been written</exception>
         /// </summary> 
         IPdxInstanceFactory^ WriteLong(String^ fieldName, Int64 value);
  
         /// <summary>
         /// Writes the named field with the given value to the serialized form.
         /// The fields type is <code>float</code>.
         /// <para>Java float is mapped to .NET System.Single(float).</para>
         /// </summary>
         /// <param name="fieldName"> the name of the field to write</param>
         /// <param name="value"> the value of the field to write</param>
         /// <returns> this PdxInstanceFactory</returns>
         /// <exception cref="IllegalStateException"/> if the named field has already been written</exception>
         
         IPdxInstanceFactory^ WriteFloat(String^ fieldName, float value);
  
         /// <summary>
         /// Writes the named field with the given value to the serialized form.
         /// The fields type is <code>double</code>.
         /// <para>Java double is mapped to .NET System.Double.</para>
         /// </summary>
         /// <param name="fieldName"> the name of the field to write</param>
         /// <param name="value"> the value of the field to write</param>
         /// <returns> this PdxInstanceFactory</returns>
         /// <exception cref="IllegalStateException"/> if the named field has already been written</exception>
         IPdxInstanceFactory^ WriteDouble(String^ fieldName, double value);
  
         /// <summary>
         /// Writes the named field with the given value to the serialized form.
         /// The fields type is <code>Date</code>.
         /// <para>Java Date is mapped to .NET System.DateTime.</para>
         /// </summary>
         /// <param name="fieldName"> the name of the field to write</param>
         /// <param name="value"> the value of the field to write</param>
         /// <returns> this PdxInstanceFactory</returns>
         /// <exception cref="IllegalStateException"/> if the named field has already been written</exception>
         
         IPdxInstanceFactory^ WriteDate(String^ fieldName, System::DateTime value);
  
         /// <summary>
         /// Writes the named field with the given value to the serialized form.
         /// The fields type is <code>String</code>.
         /// <para>Java String is mapped to .NET System.String.</para>
         /// </summary>
         /// <param name="fieldName"> the name of the field to write</param>
         /// <param name="value"> the value of the field to write</param>
         /// <returns> this PdxInstanceFactory</returns>
         /// <exception cref="IllegalStateException"/> if the named field has already been written</exception>
         IPdxInstanceFactory^ WriteString(String^ fieldName, String^ value);
        
         /// <summary>
         /// Writes the named field with the given value to the serialized form.
         /// The fields type is <code>Object</code>.
         /// 
         /// It is best to use one of the other writeXXX methods if your field type
         /// will always be XXX. This method allows the field value to be anything
         /// that is an instance of Object. This gives you more flexibility but more
         /// space is used to store the serialized field.
         /// </summary>
         /// 
         /// <param name="fieldName"> the name of the field to write</param>
         /// <param name="value"> the value of the field to write</param>
         /// <returns> this PdxInstanceFactory</returns>
         /// <exception cref="IllegalStateException"/> if the named field has already been written</exception>
         
         IPdxInstanceFactory^ WriteObject(String^ fieldName, Object^ value);
  
         /// <summary>
         /// Writes the named field with the given value to the serialized form.
         /// The fields type is <code>boolean[]</code>.
         /// <para>Java boolean[] is mapped to .NET System.Boolean[].</para>
         /// </summary>
         /// <param name="fieldName"> the name of the field to write</param>
         /// <param name="value"> the value of the field to write</param>
         /// <returns> this PdxInstanceFactory</returns>
         /// <exception cref="IllegalStateException"/> if the named field has already been written</exception>
         
         IPdxInstanceFactory^ WriteBooleanArray(String^ fieldName, array<Boolean>^ value);
  
         /// <summary>
         /// Writes the named field with the given value to the serialized form.
         /// The fields type is <code>char[]</code>.
         /// <para>Java char[] is mapped to .NET System.Char[].</para>
         /// </summary>
         /// <param name="fieldName"> the name of the field to write</param>
         /// <param name="value"> the value of the field to write</param>
         /// <returns> this PdxInstanceFactory</returns>
         /// <exception cref="IllegalStateException"/> if the named field has already been written</exception>
         IPdxInstanceFactory^ WriteCharArray(String^ fieldName, array<Char>^ value);
        
         /// <summary>
         /// Writes the named field with the given value to the serialized form.
         /// The fields type is <code>byte[]</code>.
         /// <para>Java byte[] is mapped to .NET System.Byte[].</para>
         /// </summary>
         /// <param name="fieldName"> the name of the field to write</param>
         /// <param name="value"> the value of the field to write</param>
         /// <returns> this PdxInstanceFactory</returns>
         /// <exception cref="IllegalStateException"/> if the named field has already been written</exception>
         IPdxInstanceFactory^ WriteByteArray(String^ fieldName, array<Byte>^ value);
        
         /// <summary>
         /// Writes the named field with the given value to the serialized form.
         /// The fields type is <code>short[]</code>.
         /// <para>Java short[] is mapped to .NET System.Int16[].</para>
         /// </summary>
         /// <param name="fieldName"> the name of the field to write</param>
         /// <param name="value"> the value of the field to write</param>
         /// <returns> this PdxInstanceFactory</returns>
         /// <exception cref="IllegalStateException"/> if the named field has already been written</exception>
         IPdxInstanceFactory^ WriteShortArray(String^ fieldName, array<Int16>^ value);
        
         /// <summary>
         /// Writes the named field with the given value to the serialized form.
         /// The fields type is <code>int[]</code>.
         /// <para>Java int[] is mapped to .NET System.Int32[].</para>
         /// </summary>
         /// <param name="fieldName"> the name of the field to write</param>
         /// <param name="value"> the value of the field to write</param>
         /// <returns> this PdxInstanceFactory</returns>
         /// <exception cref="IllegalStateException"/> if the named field has already been written</exception>
         IPdxInstanceFactory^ WriteIntArray(String^ fieldName, array<Int32>^ value);
        
         /// <summary>
         /// Writes the named field with the given value to the serialized form.
         /// The fields type is <code>long[]</code>.
         /// <para>Java long[] is mapped to .NET System.Int64[].</para>
         /// </summary>
         /// <param name="fieldName"> the name of the field to write</param>
         /// <param name="value"> the value of the field to write</param>
         /// <returns> this PdxInstanceFactory</returns>
         /// <exception cref="IllegalStateException"/> if the named field has already been written</exception>
         IPdxInstanceFactory^ WriteLongArray(String^ fieldName, array<Int64>^ value);
        
         /// <summary>
         /// Writes the named field with the given value to the serialized form.
         /// The fields type is <code>float[]</code>.
         /// <para>Java float[] is mapped to .NET System.Single[] or float[].</para>
         /// </summary>
         /// <param name="fieldName"> the name of the field to write</param>
         /// <param name="value"> the value of the field to write</param>
         /// <returns> this PdxInstanceFactory</returns>
         /// <exception cref="IllegalStateException"/> if the named field has already been written</exception>
         /// </summary>
         IPdxInstanceFactory^ WriteFloatArray(String^ fieldName, array<float>^ value);
        
         /// <summary>
         /// Writes the named field with the given value to the serialized form.
         /// The fields type is <code>double[]</code>.
         /// <para>Java double[] is mapped to .NET System.Double[].</para>
         /// </summary>
         /// <param name="fieldName"> the name of the field to write</param>
         /// <param name="value"> the value of the field to write</param>
         /// <returns> this PdxInstanceFactory</returns>
         /// <exception cref="IllegalStateException"/> if the named field has already been written</exception>
         IPdxInstanceFactory^ WriteDoubleArray(String^ fieldName, array<double>^ value);
        
         /// <summary>
         /// Writes the named field with the given value to the serialized form.
         /// The fields type is <code>String[]</code>.
         /// <para>Java String[] is mapped to .NET System.String[].</para>
         /// </summary>
         /// <param name="fieldName"> the name of the field to write</param>
         /// <param name="value"> the value of the field to write</param>
         /// <returns> this PdxInstanceFactory</returns>
         /// <exception cref="IllegalStateException"/> if the named field has already been written</exception>
         IPdxInstanceFactory^ WriteStringArray(String^ fieldName, array<String^>^ value);
        
         /// <summary>
         /// Writes the named field with the given value to the serialized form.
         /// The fields type is <code>Object[]</code>.
         /// Java Object[] is mapped to .NET System.Collections.Generic.List<Object>.
         /// </summary>
         /// <param name="fieldName"> the name of the field to write</param>
         /// <param name="value"> the value of the field to write</param>
         /// <returns> this PdxInstanceFactory</returns>
         /// <exception cref="IllegalStateException"/> if the named field has already been written</exception>
         
         IPdxInstanceFactory^ WriteObjectArray(String^ fieldName, System::Collections::Generic::List<Object^>^ value);
  
         /// <summary>
         /// Writes the named field with the given value to the serialized form.
         /// The fields type is <code>byte[][]</code>.
         /// <para>Java byte[][] is mapped to .NET System.Byte[][].</para>
         /// </summary>
         /// <param name="fieldName"> the name of the field to write</param>
         /// <param name="value"> the value of the field to write</param>
         /// <returns> this PdxInstanceFactory</returns>
         /// <exception cref="IllegalStateException"/> if the named field has already been written</exception>
         IPdxInstanceFactory^ WriteArrayOfByteArrays(String^ fieldName, array<array<Byte>^>^ value);
        
         /// <summary>
         /// Writes the named field with the given value and type to the serialized form.
         /// This method uses the <code>fieldType</code> to determine which writeXXX method it should call.
         /// If it can not find a specific match to a writeXXX method it will call <see cref="WriteObject" />.
         /// This method may serialize objects that are not portable to non-java languages.
         /// <para>The fieldTypes maps to a specific method.</para>
         /// <param name="fieldName"> the name of the field to write</param>
         /// <summary>
         /// @param fieldValue the value of the field to write; this parameter's class must extend the <code>fieldType</code>
         /// @param fieldType the type of the field to write
         /// <returns> this PdxInstanceFactory</returns>
         /// <exception cref="IllegalStateException"/> if the named field has already been written</exception>
         
         IPdxInstanceFactory^ WriteField(String^ fieldName, Object^ fieldValue, Type^ fieldType);        

         /// <summary>
         /// Indicate that the named field should be included in hashCode and equals checks
         /// of this object on a server that is accessing <see cref="IPdxInstance" />
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
         /// </summary>
         /// <param name="fieldName"> the name of the field to mark as an identity field.</param>
         /// <returns> this PdxInstanceFactory</returns>
         /// <exception cref="IllegalStateException"/> if the named field has not already been written.</exception>
         
         IPdxInstanceFactory^ MarkIdentityField(String^ fieldName);

        };

			}
    }
  }
}
