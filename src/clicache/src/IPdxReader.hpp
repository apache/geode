/*=========================================================================
 * Copyright (c) 2002-2014 Pivotal Software, Inc. All Rights Reserved.
 * This product is protected by U.S. and international copyright
 * and intellectual property laws. Pivotal products are covered by
 * more patents listed at http://www.pivotal.io/patents.
 *=========================================================================
 */

#pragma once

#include "gf_defs.hpp"
#include "IRegion.hpp"
#include "IPdxUnreadFields.hpp"
namespace GemStone
{
  namespace GemFire
  {
    namespace Cache
    {
			namespace Generic
			{
				/// <summary>
				/// A IPdxReader will be passed to IPdxSerializable.fromData or 
				/// during deserialization of a PDX. The domain class needs to deserialize field members 
				/// using this interface. This interface is implemented by Native Client.
				/// Each readXXX call will return the field's value. If the serialized 
				/// PDX does not contain the named field then a default value will 
				/// be returned. Standard Java defaults are used. For Objects this is 
				/// null and for primitives it is 0 or 0.0.
				/// </summary>
				public interface class IPdxReader
				{
				public:

					/// <summary>
					/// Read a signed byte from the stream.
					/// </summary>
					/// <param name="fieldName">The name of a member field whose value to read.</param>
					SByte ReadByte( String^ fieldName );
	        
					/// <summary>
					/// Read a boolean value from the stream.
					/// </summary>
					/// <param name="fieldName">The name of a member field whose value to read.</param>
					Boolean ReadBoolean( String^ fieldName );

					/// <summary>
					/// Read a char value from the stream.
					/// </summary>
					/// <param name="fieldName">The name of a member field whose value to read.</param>
					Char ReadChar( String^ fieldName );
	                                
					/// <summary>
					/// Read a 16-bit integer from the stream.
					/// </summary>
					/// <param name="fieldName">The name of a member field whose value to read.</param>
					short ReadShort( String^ fieldName );

					/// <summary>
					/// Read a 32-bit integer from the stream.
					/// </summary>
					/// <param name="fieldName">The name of a member field whose value to read.</param>
					Int32 ReadInt( String^ fieldName );

					/// <summary>
					/// Read a 64-bit integer from the stream.
					/// </summary>
					/// <param name="fieldName">The name of a member field whose value to read.</param>
					Int64 ReadLong( String^ fieldName );

					/// <summary>
					/// Read a floating point number from the stream.
					/// </summary>
					/// <param name="fieldName">The name of a member field whose value to read.</param>
					float ReadFloat( String^ fieldName );

					/// <summary>
					/// Read a double precision number from the stream.
					/// </summary>
					/// <param name="fieldName">The name of a member field whose value to read.</param>
					double ReadDouble( String^ fieldName );

					/// <summary>
					/// Read a string after java-modified UTF-8 decoding from the stream.
					/// </summary>
					/// <param name="fieldName">The name of a member field whose value to read.</param>
					String^ ReadString( String^ fieldName );

					/// <summary>
					/// Read a serializable object from the data. Null objects are handled.
					/// </summary>
					/// <param name="fieldName">The name of a member field whose value to read.</param>
					Object^ ReadObject( String^ fieldName );
	        
					//TODO:
					//void WriteMap( String^ fieldName, System::Collections::IDictionary^ map );

					/// <summary>
					/// Read a Date from the data. 
					/// </summary>
					/// <param name="fieldName">The name of a member field whose value to read.</param>
					System::DateTime ReadDate( String^ fieldName) ;
					//void writeFile(String fieldName, File file) ;

					/// <summary>
					/// Read a boolean array from the data. 
					/// </summary>
					/// <param name="fieldName">The name of a member field whose value to read.</param>
					array<Boolean>^ ReadBooleanArray( String^ fieldName );

					/// <summary>
					/// Read a char array from the data.
					/// </summary>
					/// <param name="fieldName">The name of a member field whose value to read.</param>
					array<Char>^ ReadCharArray(String^ fieldName );

					/// <summary>
					/// Read a signed byte array from the data.
					/// </summary>
					/// <param name="fieldName">The name of a member field whose value to read.</param>
					array<Byte>^ ReadByteArray(String^ fieldName);
	        
					/// <summary>
					/// Read a short from the data.
					/// </summary>
					/// <param name="fieldName">The name of a member field whose value to read.</param>
					array<short>^ ReadShortArray(String^ fieldName);
	        
					/// <summary>
					/// Read a int array from the data.
					/// </summary>
					/// <param name="fieldName">The name of a member field whose value to read.</param>
					array<System::Int32>^ ReadIntArray(String^ fieldName);
	        
					/// <summary>
					/// Read a long array from the data.
					/// </summary>
					/// <param name="fieldName">The name of a member field whose value to read.</param>
					array<Int64>^ ReadLongArray(String^ fieldName);
	        
					/// <summary>
					/// Read a float from the data.
					/// </summary>
					/// <param name="fieldName">The name of a member field whose value to read.</param>
					array<float>^ ReadFloatArray(String^ fieldName);

					/// <summary>
					/// Read a double array from the data.
					/// </summary>
					/// <param name="fieldName">The name of a member field whose value to read.</param>
					array<double>^ ReadDoubleArray(String^ fieldName);

					/// <summary>
					/// Read a string array from the data.
					/// </summary>
					/// <param name="fieldName">The name of a member field whose value to read.</param>
					array<String^>^ ReadStringArray(String^ fieldName);

					/// <summary>
					/// Read a object array from the data.
					/// </summary>
					/// <param name="fieldName">The name of a member field whose value to read.</param>
					List<Object^>^ ReadObjectArray(String^ fieldName);

					/// <summary>
					/// Read a two-dimenesional signed byte array from the data.
					/// </summary>
					/// <param name="fieldName">The name of a member field whose value to read.</param>
					array<array<Byte>^>^ ReadArrayOfByteArrays(String^ fieldName );

					//TODO:
					//void WriteEnum(String^ fieldName, Enum e) ;
					//void WriteInetAddress(String^ fieldName, InetAddress address);
	        
					/// <summary>
					/// Whether field is available or not.
					/// </summary>
					/// <param name="fieldName">The name of a member field.</param>
					bool HasField(String^ fieldName);
	  
					/// <summary>
					/// Whether field is used as identity field or not.
					/// </summary>
					/// <param name="fieldName">The name of a member field.</param>
					bool IsIdentityField(String^ fieldName);

					/// <summary>
					/// To preserve unread data, which get added in new version of type.
					/// </summary>
					/// <return>Unread data.</return>
					IPdxUnreadFields^ ReadUnreadFields();

          /// <summary>
          /// Reads the named field  of Type "type" and returns its value.
          /// </summary>
          /// <param name="fieldName">The name of a member field.</param>
          /// <param name="type">The type of a member field, which value needs to read.</param>
          Object^ ReadField(String^ fieldName, Type^ type);
				};
			}
    }
  }
}
