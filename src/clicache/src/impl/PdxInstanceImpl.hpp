/*=========================================================================
 * Copyright (c) 2002-2014 Pivotal Software, Inc. All Rights Reserved.
 * This product is protected by U.S. and international copyright
 * and intellectual property laws. Pivotal products are covered by
 * more patents listed at http://www.pivotal.io/patents.
 *=========================================================================
 */

#pragma once

#include "../IPdxInstance.hpp"
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
        ref class PdxType;
				/// <summary>
				/// Serialize the data in gemfire Portable Data eXchange(Pdx) Format.
				/// This format provides class versioning(forward and backward compability of types) in cache.
				/// This provides ability to query .NET domian objects.
				/// </summary>
        ref class PdxInstanceImpl : IWritablePdxInstance, IPdxInstance, IPdxSerializable
				{
        private:
         static array<SByte>^ Boolean_DefaultBytes = gcnew array<SByte>{0};
         static array<SByte>^ Byte_DefaultBytes = gcnew array<SByte>{0};
         static array<SByte>^ Char_DefaultBytes = gcnew array<SByte>{0, 0};
         static array<SByte>^ Short_DefaultBytes = gcnew array<SByte>{0, 0};
         static array<SByte>^ Int_DefaultBytes = gcnew array<SByte>{0, 0, 0, 0};
         static array<SByte>^ Long_DefaultBytes = gcnew array<SByte>{0, 0, 0, 0, 0, 0, 0, 0};
         static array<SByte>^ Float_DefaultBytes = gcnew array<SByte>{0, 0, 0, 0};
         static array<SByte>^ Double_DefaultBytes = gcnew array<SByte>{0, 0, 0, 0, 0, 0, 0, 0};
         static array<SByte>^ Date_DefaultBytes = gcnew array<SByte>{-1, -1, -1, -1, -1, -1, -1, -1};
         static array<SByte>^ String_DefaultBytes = gcnew array<SByte>{gemfire::GemfireTypeIds::CacheableNullString};
         static array<SByte>^ Object_DefaultBytes = gcnew array<SByte>{gemfire::GemfireTypeIds::NullObj};
         static array<SByte>^ NULL_ARRAY_DefaultBytes = gcnew array<SByte>{-1};

         static PdxFieldType^ Default_PdxFieldType = gcnew PdxFieldType("default", "default", -1,
																										-1/*field index*/, 
																										false, 1, -1/*var len field idx*/);

         bool hasDefaultBytes(PdxFieldType^ pField, DataInput^ dataInput, int start, int end);
         bool compareDefaulBytes(DataInput^ dataInput, int start, int end, array<SByte>^ defaultBytes);

         void cleanup();

       
          //DataInput^ m_dataInput;
          uint8_t* m_buffer;
          int m_bufferLength;
          int m_typeId;
          bool m_own;
          PdxType^ m_pdxType;
         internal:
          Dictionary<String^, Object^>^ m_updatedFields;                  

          Object^ readField(DataInput^ dataInput, String^ fieldName, int typeId);

          bool checkType(Type^ fieldType, int typeId);

          void writeField(IPdxWriter^ writer, String^ fieldName, int typeId, Object^ value);

          int getOffset(DataInput^ dataInput, PdxType^ pt, int sequenceId);

          int getSerializedLength(DataInput^ dataInput, PdxType^ pt);

          void writeUnmodifieldField(DataInput^ dataInput, int startPos, int endPos, PdxLocalWriter^ localWriter);

          int getNextFieldPosition(DataInput^ dataInput, int fieldId, PdxType^ pt);

          IList<PdxFieldType^>^ getIdentityPdxFields(PdxType^ pt);

          bool isPrimitiveArray(Object^ object);

          int getRawHashCode(PdxType^ pt, PdxFieldType^ pField, DataInput^ dataInput);

          static int deepArrayHashCode(Object^ obj);

          generic <class T>where T:System::Collections::ICollection, System::Collections::IList, System::Collections::IEnumerable
          static int primitiveArrayHashCode(T objArray) ;
          
          static int enumerableHashCode(System::Collections::IEnumerable^ enumObj);

          static int enumerateDictionary(System::Collections::IDictionary^ iDict);

          void setOffsetForObject(DataInput^ dataInput, PdxType^ pt, int sequenceId);

          static int comparePdxField(PdxFieldType^ a, PdxFieldType^ b);

          void equatePdxFields(IList<PdxFieldType^>^ my, IList<PdxFieldType^>^ other);

          //bool compareRawBytes(PdxInstanceImpl^ other, PdxType^ myPT,  PdxFieldType^ myF, PdxType^ otherPT,  PdxFieldType^ otherF);
          bool compareRawBytes(PdxInstanceImpl^ other, PdxType^ myPT,  PdxFieldType^ myF,DataInput^ myDataInput, PdxType^ otherPT,  PdxFieldType^ otherF, DataInput^ otherDataInput);

          static bool enumerateDictionaryForEqual(System::Collections::IDictionary^ iDict, System::Collections::IDictionary^ otherIDict);

          static bool enumerableEquals(System::Collections::IEnumerable^ enumObj, System::Collections::IEnumerable^ enumOtherObj);

          static bool deepArrayEquals(Object^ obj, Object^ otherObj);

          void updatePdxStream(uint8_t* newPdxStream, int len);

        public:
          PdxInstanceImpl (uint8_t* buffer, int length, int typeId, bool own)
          {
            //m_dataInput = dataInput;
            m_buffer = buffer;
            m_bufferLength = length;
            m_typeId = typeId;
            m_updatedFields = nullptr;
            m_own = own;
            m_pdxType = nullptr;
          }

          //for pdxInstance factory
          PdxInstanceImpl(Dictionary<String^, Object^>^ fieldVsValue, PdxType^ pdxType);          

          ~PdxInstanceImpl( ) 
          {
            cleanup();
          }
          !PdxInstanceImpl()
          {
            cleanup();
          }
          
          PdxType^ getPdxType();

          void setPdxId(Int32 typeId);

          virtual String^ GetClassName();

           /// <summary> 
           ///Deserializes and returns the domain object that this instance represents.
           ///
           ///@return the deserialized domain object.
           ///@throws PdxSerializationException if the instance could not be deserialized
           /// </summary>
          virtual Object^ GetObject();

           /// <summary>
           ///Checks if the named field exists and returns the result.
           ///This can be useful when writing code that handles more than one version of
           ///a PDX class.
           ///@param fieldName the name of the field to check
           ///@return <code>true</code> if the named field exists; otherwise <code>false</code>
           /// </summary>
          virtual bool HasField(String^ fieldName);
          
            /// <summary>
            ///Return an unmodifiable list of the field names on this PdxInstance.
            ///@return an unmodifiable list of the field names on this PdxInstance
            /// </summary>
          virtual IList<String^>^ GetFieldNames();
          
           /// <summary>
           ///Checks if the named field was {@link PdxWriter#markIdentityField(String) marked} as an identity field.
           ///Note that if no fields have been marked then all the fields are used as identity fields even though
           ///this method will return <code>false</code> since none of them have been <em>marked</em>.
           ///@param fieldName the name of the field to check
           ///@return <code>true</code> if the named field exists and was marked as an identify field; otherwise <code>false</code>
           /// </summary>
          virtual bool IsIdentityField(String^ fieldName);

           /// <summary>
           ///Reads the named field and returns its value. If the field does
           ///not exist <code>null</code> is returned.
           ///A <code>null</code> result indicates that the field does not exist
           ///or that it exists and its value is currently <code>null</code>.
           ///The {@link #hasField(String) hasField} method can be used to figure out
           ///which if these two cases is true.
           ///If an Object[] is deserialized by this call then that array's component
           ///type will be <code>Object.class</code> instead of the original class that
           ///the array had when it was serialized. This is done so that PdxInstance objects
           ///can be added to the array.
           /// 
           ///@param fieldName
           ///         name of the field to read
           ///
           ///@return If this instance has the named field then the field's value is returned,
           ///otherwise <code>null</code> is returned.
           ///@throws PdxSerializationException if the field could not be deserialized
           /// </summary>
           virtual  Object^ GetField(String^ fieldName);

           /// <summary>
           ///Returns true if the given object is equals to this instance.
           ///If <code>other</code> is not a PdxInstance then it is not equal to this instance.
           ///NOTE: Even if <code>other</code> is the result of calling {@link #getObject()} it will not
           ///be equal to this instance.
           ///Otherwise equality of two PdxInstances is determined as follows:
           ///<ol>
           ///<li>The domain class name must be equal for both PdxInstances</li>
           ///<li>Each identity field must be equal.</li>
           ///</ol>
           ///If one of the instances does not have a field that the other one does then equals will assume it
           ///has the field with a default value.
           ///If a PdxInstance has marked identity fields using {@link PdxWriter#markIdentityField(String) markIdentityField}
           ///then only the marked identity fields are its identity fields.
           ///Otherwise all its fields are identity fields.
           ///An identity field is equal if all the following are true:
           ///<ol>
           ///<li>The field name is equal.</li>
           ///<li>The field type is equal.</li>
           ///<li>The field value is equal.</li>
           ///</ol>
           ///If a field's type is <code>OBJECT</code> then its value must be deserialized to determine if it is equals. If the deserialized object is an array then {@link java.util.Arrays#deepEquals(Object[], Object[]) deepEquals} is used to determine equality. Otherwise {@link Object#equals(Object) equals} is used.
           ///If a field's type is <code>OBJECT[]</code> then its value must be deserialized and {@link java.util.Arrays#deepEquals(Object[], Object[]) deepEquals} is used to determine equality.
           ///For all other field types then the value does not need to be deserialized. Instead the serialized raw bytes are compared and used to determine equality.
           ///Note that any fields that have objects that do not override {@link Object#equals(Object) equals} will cause equals to return false when you might have expected it to return true.
           ///The only exceptions to this are those that call {@link java.util.Arrays#deepEquals(Object[], Object[]) deepEquals} as noted above. You should either override equals and hashCode in these cases
           ///or mark other fields as your identity fields.  
           ///@param other the other instance to compare to this.
           ///@return <code>true</code> if this instance is equal to <code>other</code>.
           /// </summary>
           virtual bool Equals(Object^ other) override;

           /// <summary>
           ///Generates a hashCode based on the identity fields of
           ///this PdxInstance. 
           ///If a PdxInstance has marked identity fields using {@link PdxWriter#markIdentityField(String) markIdentityField}
           ///then only the marked identity fields are its identity fields.
           ///Otherwise all its fields are identity fields.
           ///
           ///If an identity field is of type <code>OBJECT</code> then it is deserialized. If the deserialized object is an array then {@link java.util.Arrays#deepHashCode(Object[]) deepHashCode} is used. Otherwise {@link Object#hashCode() hashCode} is used.
           ///If an identity field is of type <code>OBJECT[]</code> this it is deserialized and {@link java.util.Arrays#deepHashCode(Object[]) deepHashCode} is used.
           ///Otherwise the field is not deserialized and the raw bytes of its value are used to compute the hash code.
           /// </summary>
           virtual int GetHashCode()  override;

           /// <summary>
           ///Prints out all of the identity fields of this PdxInstance.
           ///If a PdxInstance has marked identity fields using {@link PdxWriter#markIdentityField(String) markIdentityField}
           ///then only the marked identity fields are its identity fields.
           ///Otherwise all its fields are identity fields.
           /// </summary>
           virtual String^ ToString()  override;
          
           /// <summary>
           ///Creates and returns a {@link WritablePdxInstance} whose initial
           ///values are those of this PdxInstance.
           ///This call returns a copy of the current field values so modifications
           ///made to the returned value will not modify this PdxInstance.
           ///
           ///@return a {@link WritablePdxInstance}
           /// </summary>
           virtual IWritablePdxInstance^ CreateWriter();

            /// <summary>
          ///Set the existing named field to the given value.
          ///The setField method has copy-on-write semantics.
          /// So for the modifications to be stored in the cache the WritablePdxInstance 
          ///must be put into a region after setField has been called one or more times.
          ///
          ///@param fieldName
          ///         name of the field whose value will be set
          ///@param value
          ///         value that will be assigned to the field
          ///@throws PdxFieldDoesNotExistException if the named field does not exist
          ///@throws PdxFieldTypeMismatchException if the type of the value is not compatible with the field
          /// </summary>
          virtual void SetField(String^ fieldName, Object^ value);

          virtual void ToData( IPdxWriter^ writer );
          
          virtual void FromData( IPdxReader^ reader );          

				};
        }
			}
    }
  }
}
