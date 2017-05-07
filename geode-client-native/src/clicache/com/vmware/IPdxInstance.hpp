/*=========================================================================
 ///Copyright (c) 2002-2014 Pivotal Software, Inc. All Rights Reserved.
 * This product is protected by U.S. and international copyright
 * and intellectual property laws. Pivotal products are covered by
 * more patents listed at http://www.pivotal.io/patents.
 *=========================================================================
 */

#pragma once
#include "IWritablePdxInstance.hpp"
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
         ///PdxInstance provides run time access to the fields of a PDX without 
         ///deserializing the PDX. Preventing deserialization saves time
         ///and memory and does not require the domain class.
         ///This interface is implemented by NativeClient. The PdxInstance implementation 
         ///is a light weight wrapper that simply refers to the raw bytes of the PDX 
         ///that are kept in the cache.
         ///Applications can choose to access PdxInstances instead of .NET objects by 
         ///configuring the Cache to prefer PDX instances during deserialization. 
         ///This can be done in <c>cache.xml</c> by setting the attribute <c>read-serialized</c> 
         ///to true on the <c>pdx</c> element. Or it can be done programmatically using
         ///the <see cref="CacheFactory.SetPdxReadSerialized" /> 
         ///method. Once this preference is configured, then any time deserialization of a 
         ///PDX is done it will deserialize into a PdxInstance.
         ///PdxInstance are immutable. If you want to change one call
        ///<see cref="IPdxInstance.CreateWriter"/>.
         ///
         /// </summary>
        public interface class IPdxInstance : IDisposable
        {
          public:
           /// <summary> 
           ///Return the full name of the class that this pdx instance represents.          
           /// </summary>
            ///<returns> the name of the class that this pdx instance represents.</returns>
           String^ GetClassName();
           /// <summary> 
           ///Deserializes and returns the domain object that this instance represents.           
           /// </summary>
            ///<returns> the deserialized domain object.</returns>
          Object^ GetObject();

           /// <summary>
           ///Checks if the named field exists and returns the result.
           ///This can be useful when writing code that handles more than one version of
           ///a PDX class.
           /// </summary>
          ///<param> fieldName the name of the field to check</param>
          ///<returns> <code>true</code> if the named field exists; otherwise <code>false</code></returns>
           
          bool HasField(String^ fieldName);
          
            /// <summary>
            ///Return an list of the field names on this PdxInstance.            
            /// </summary>
          ///<returns> an list of the field names on this PdxInstance</returns>
          IList<String^>^ GetFieldNames();
          
          /// <summary>
          ///Checks if the named field was <see cref="IPdxWriter.MarkIdentityField" /> marked as an identity field.
          ///Note that if no fields have been marked then all the fields are used as identity fields even though
          ///this method will return <code>false</code> since none of them have been marked.
          /// </summary>
          ///<param name="fieldName">  the name of the field to check</param>
          ///<returns> <code>true</code> if the named field exists and was marked as an identify field; otherwise <code>false</code></returns> 
           
          bool IsIdentityField(String^ fieldName);

           /// <summary>
           ///Reads the named field and returns its value. If the field does
           ///not exist <code>null</code> is returned.
           ///A <code>null</code> result indicates that the field does not exist
           ///or that it exists and its value is currently <code>null</code>.
          ///The <see cref="HasField" /> method can be used to figure out
           ///which if these two cases is true.
           ///If an Object[] is deserialized by this call then that array's component
           ///type will be <code>Object</code> instead of the original class that
           ///the array had when it was serialized. This is done so that PdxInstance objects
           ///can be added to the array.
           /// </summary>
           /// 
           ///<param name="fieldName">  name of the field to read</param>
           ///         
           ///
           ///<returns> If this instance has the named field then the field's value is returned,
           ///otherwise <code>null</code> is returned.</returns> 
           
            Object^ GetField(String^ fieldName);

           /// <summary>
           ///Returns true if the given object is equals to this instance.
           ///If <code>other</code> is not a PdxInstance then it is not equal to this instance.
            ///NOTE: Even if <code>other</code> is the result of calling <see cref="GetObject" /> it will not
           ///be equal to this instance.
           ///Otherwise equality of two PdxInstances is determined as follows:
           /// <list type="bullet">
           /// <item>
           ///<description>The domain class name must be equal for both PdxInstances</description>
            /// </item>
           /// <item>
           ///<description>Each identity field must be equal.</description>
            /// </item>
           /// </list>
           ///If one of the instances does not have a field that the other one does then equals will assume it
           ///has the field with a default value.
            ///If a PdxInstance has marked identity fields using <see cref="IPdxWriter.MarkIdentityField" /> 
           ///then only the marked identity fields are its identity fields.
           ///Otherwise all its fields are identity fields.
           ///An identity field is equal if all the following are true:
           /// <list type="bullet">
            /// <item>
           ///<description>The field name is equal.</description>
            /// </item>
            /// <item>
           ///<description>The field type is equal.</description>
            /// </item>
            /// <item>
           ///<description>The field value is equal.</description>
            /// </item>
           /// </list>
           ///If a field's type is <code>OBJECT</code> then its value must be deserialized to determine if it is equals. If the deserialized object is an array then all the array element is used to determine equality. Otherwise <see cref="Object.Equals" /> is used.
           ///If a field's type is <code>OBJECT[]</code> then its value must be deserialized and all the array element is used to determine equality.
           ///For all other field types then the value does not need to be deserialized. Instead the serialized raw bytes are compared and used to determine equality.
           ///Note that any fields that have objects that do not override <see cref="Object.Equals" /> will cause equals to return false when you might have expected it to return true.
            /// </summary>
            ///<param name="other"> the other instance to compare to this.</param>
            ///<returns> <code>true</code> if this instance is equal to <code>other</code>.</returns>
           
           bool Equals(Object^ other);

           /// <summary>
           ///Generates a hashCode based on the identity fields of
           ///this PdxInstance. 
           ///If a PdxInstance has marked identity fields using <see cref="IPdxWriter.MarkIdentityField" />
           ///then only the marked identity fields are its identity fields.
           ///Otherwise all its fields are identity fields.
           ///
           ///If an identity field is of type <code>OBJECT</code> then it is deserialized. If the deserialized object is an array then all the array element is used. Otherwise <see cref="Object.GetHashCode" /> is used.
           ///If an identity field is of type <code>OBJECT[]</code> this it is deserialized and all the array element is used.
           ///Otherwise the field is not deserialized and the raw bytes of its value are used to compute the hash code.
           /// </summary>
           int GetHashCode();

           /// <summary>
           ///Prints out all of the identity fields of this PdxInstance.
           ///If a PdxInstance has marked identity fields using <see cref="IPdxWriter.MarkIdentityField" />
           ///then only the marked identity fields are its identity fields.
           ///Otherwise all its fields are identity fields.
           /// </summary>
           String^ ToString();
          
           /// <summary>
           ///Creates and returns a <see cref="IWritablePdxInstance"/> whose initial
           ///values are those of this PdxInstance.
           ///This call returns a copy of the current field values so modifications
           ///made to the returned value will not modify this PdxInstance.
           /// </summary>
           ///
           ///<returns> a <see cref="IWritablePdxInstance"/></returns>
           
           IWritablePdxInstance^ CreateWriter();

        };

			}
    }
  }
}
