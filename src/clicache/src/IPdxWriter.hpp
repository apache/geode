/*=========================================================================
 * Copyright (c) 2002-2014 Pivotal Software, Inc. All Rights Reserved.
 * This product is protected by U.S. and international copyright
 * and intellectual property laws. Pivotal products are covered by
 * more patents listed at http://www.pivotal.io/patents.
 *=========================================================================
 */

#pragma once

#include "gf_defs.hpp"
//#include "IRegion.hpp"
#include "IPdxUnreadFields.hpp"
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
				/// A IPdxWriter will be passed to IPdxSerializable.toData
				/// when it is serializing the domain class. The domain class needs to serialize member 
				/// fields using this interface. This interface is implemented 
				/// by Native Client.
				/// </summary>
				public interface class IPdxWriter
				{
				public:
	        
					/// <summary>
					/// Write a byte to the <c>IPdxWriter</c>.
					/// </summary>
					/// <param name="fieldName">The name of the field associated with the value.</param>
					/// <param name="value">The byte to write.</param>
					IPdxWriter^ WriteByte( String^ fieldName, SByte value );
	        
					/// <summary>
					/// Write a boolean value to the <c>IPdxWriter</c>.
					/// </summary>
					/// <param name="fieldName">The name of the field associated with the value.</param>
					/// <param name="value">The boolean value to write.</param>
					IPdxWriter^ WriteBoolean( String^ fieldName, Boolean value );

					/// <summary>
					/// Write a char value to the <c>IPdxWriter</c>.
					/// </summary>
					/// <param name="fieldName">The name of the field associated with the value.</param>
					/// <param name="value">The char value to write.</param>
					IPdxWriter^ WriteChar( String^ fieldName, Char value );
	                                             
					/// <summary>
					/// Write a 16-bit integer to the <c>IPdxWriter</c>.
					/// </summary>
					/// <param name="fieldName">The name of the field associated with the value.</param>
					/// <param name="value">The 16-bit integer to write.</param>
					IPdxWriter^ WriteShort( String^ fieldName, Int16 value );

					/// <summary>
					/// Write a 32-bit integer to the <c>IPdxWriter</c>.
					/// </summary>
					/// <param name="fieldName">The name of the field associated with the value.</param>
					/// <param name="value">The 32-bit integer to write.</param>
					IPdxWriter^ WriteInt( String^ fieldName, Int32 value );

					/// <summary>
					/// Write a 64-bit integer to the <c>IPdxWriter</c>.
					/// </summary>
					/// <param name="fieldName">The name of the field associated with the value.</param>
					/// <param name="value">The 64-bit integer to write.</param>
					IPdxWriter^ WriteLong( String^ fieldName, Int64 value );

					/// <summary>
					/// Write a float to the <c>IPdxWriter</c>.
					/// </summary>
					/// <param name="fieldName">The name of the field associated with the value.</param>
					/// <param name="value">The float value to write.</param>
					IPdxWriter^ WriteFloat( String^ fieldName, float value );

					/// <summary>
					/// Write a double precision real number to the <c>IPdxWriter</c>.
					/// </summary>
					/// <param name="fieldName">The name of the field associated with the value.</param>
					/// <param name="value">
					/// The double precision real number to write.
					/// </param>
					IPdxWriter^ WriteDouble( String^ fieldName, double value );

					/// <summary>
					/// Write a string using java-modified UTF-8 encoding to
					/// <c>IPdxWriter</c>.
					/// </summary>
					/// <param name="fieldName">The name of the field associated with the value.</param>
					/// <param name="value">The UTF encoded string to write.</param>
					IPdxWriter^ WriteString( String^ fieldName, String^ value );
	        
					/// <summary>
					/// Write an <c>Object</c> object to the <c>IPdxWriter</c>.
					/// </summary>
					/// <param name="fieldName">The name of the field associated with the value.</param>
					/// <param name="obj">The object to write.</param>
					IPdxWriter^ WriteObject( String^ fieldName, Object^ obj );

					//TODO:
					//IPdxWriter^ WriteMap( String^ fieldName, System::Collections::IDictionary^ map );
	        
					/// <summary>
					/// Write an collection to the <c>IPdxWriter</c>.
					/// </summary>
					/// <param name="fieldName">The name of the field associated with the value.</param>
					/// <param name="date">The date to write.</param>
					IPdxWriter^ WriteDate( String^ fieldName, System::DateTime date);
	        
					//TODO:
					//IPdxWriter^ writeFile(String fieldName, File file) ;

					/// <summary>
					/// Write an collection to the <c>IPdxWriter</c>.
					/// </summary>
					/// <param name="fieldName">The name of the field associated with the value.</param>
					/// <param name="boolArray">The boolArray to write.</param>
					IPdxWriter^ WriteBooleanArray( String^ fieldName, array<bool>^ boolArray);

					/// <summary>
					/// Write an collection to the <c>IPdxWriter</c>.
					/// </summary>
					/// <param name="fieldName">The name of the field associated with the value.</param>
					/// <param name="charArray">The charArray to write.</param>
					IPdxWriter^ WriteCharArray(String^ fieldName, array<Char>^ charArray) ;

					/// <summary>
					/// Write an collection to the <c>IPdxWriter</c>.
					/// </summary>
					/// <param name="fieldName">The name of the field associated with the value.</param>
					/// <param name="byteArray">The byteArray to write.</param>
					IPdxWriter^ WriteByteArray(String^ fieldName, array<Byte>^ byteArray) ;
	        
					/// <summary>
					/// Write an collection to the <c>IPdxWriter</c>.
					/// </summary>
					/// <param name="fieldName">The name of the field associated with the value.</param>
					/// <param name="shortArray">The shortArray to write.</param>
					IPdxWriter^ WriteShortArray(String^ fieldName, array<System::Int16>^ shortArray);

					/// <summary>
					/// Write an collection to the <c>IPdxWriter</c>.
					/// </summary>
					/// <param name="fieldName">The name of the field associated with the value.</param>
					/// <param name="intArray">The intArray to write.</param>
					IPdxWriter^ WriteIntArray(String^ fieldName, array<System::Int32>^ intArray);

					/// <summary>
					/// Write an collection to the <c>IPdxWriter</c>.
					/// </summary>
					/// <param name="fieldName">The name of the field associated with the value.</param>
					/// <param name="longArray">The longArray to write.</param>
					IPdxWriter^ WriteLongArray(String^ fieldName, array<Int64>^ longArray);

					/// <summary>
					/// Write an collection to the <c>IPdxWriter</c>.
					/// </summary>
					/// <param name="fieldName">The name of the field associated with the value.</param>
					/// <param name="floatArray">The floatArray to write.</param>
					IPdxWriter^ WriteFloatArray(String^ fieldName, array<float>^ floatArray);

					/// <summary>
					/// Write an collection to the <c>IPdxWriter</c>.
					/// </summary>
					/// <param name="fieldName">The name of the field associated with the value.</param>
					/// <param name="doubleArray">The doubleArray to write.</param>
					IPdxWriter^ WriteDoubleArray(String^ fieldName, array<double>^ doubleArray);

					/// <summary>
					/// Write an collection to the <c>IPdxWriter</c>.
					/// </summary>
					/// <param name="fieldName">The name of the field associated with the value.</param>
					/// <param name="stringArray">The stringArray to write.</param>
					IPdxWriter^ WriteStringArray(String^ fieldName, array<String^>^ stringArray);

					/// <summary>
					/// Write an collection to the <c>IPdxWriter</c>.
					/// </summary>
					/// <param name="fieldName">The name of the field associated with the value.</param>
					/// <param name="objectArray">The objectArray to write.</param>
					IPdxWriter^ WriteObjectArray(String^ fieldName, List<Object^>^ objectArray);

					/// <summary>
					/// Write an collection to the <c>IPdxWriter</c>.
					/// </summary>
					/// <param name="fieldName">The name of the field associated with the value.</param>
					/// <param name="byteArrays">The byteArrays to write.</param>
					IPdxWriter^ WriteArrayOfByteArrays(String^ fieldName, array<array<Byte>^>^ byteArrays);
	        
					//TODO:
					//IPdxWriter^ WriteEnum(String^ fieldName, Enum e) ;
					//IPdxWriter^ WriteInetAddress(String^ fieldName, InetAddress address);

					/// <summary>
					/// Indicate that the given field name should be included in hashCode and equals checks
					/// of this object on a server that is using {@link CacheFactory#setPdxReadSerialized(boolean)}
					/// or when a client executes a query on a server.
					/// 
					/// The fields that are marked as identity fields are used to generate the hashCode and
					/// equals methods of {@link PdxInstance}. Because of this, the identity fields should themselves
					/// either be primatives, or implement hashCode and equals.
					/// 
					/// If no fields are set as identity fields, then all fields will be used in hashCode and equals
					/// checks.
					/// 
					/// The identity fields should make marked after they are written using a write* method.
					/// </summary>
					/// <param name="fieldName"> the name of the field that should be used in the as part of the identity.</param>
					/// <returns>this PdxWriter</returns>

					IPdxWriter^ MarkIdentityField(String^ fieldName);

					/// <summary>
					/// To append unread data with updated data.
					/// 
					/// </summary>
					/// <returns>this PdxWriter</returns>
					IPdxWriter^ WriteUnreadFields(IPdxUnreadFields^ unread);

          /// <summary>
					/// Writes the named field with the given value and type to the serialized form.
          /// This method uses the <code>fieldType</code> to determine which WriteXXX method it should call.
          /// If it can not find a specific match to a writeXXX method it will call <see cref="WriteObject(String^, Object^)">.
					/// 
					/// </summary>
					/// <returns>this PdxWriter</returns>
          IPdxWriter^ WriteField(String^ fieldName, Object^ fieldValue, Type^ type);
				};
			}
    }
  }
}
