/*=========================================================================
 * Copyright (c) 2002-2014 Pivotal Software, Inc. All Rights Reserved.
 * This product is protected by U.S. and international copyright
 * and intellectual property laws. Pivotal products are covered by
 * more patents listed at http://www.pivotal.io/patents.
 *=========================================================================
 */

#pragma once
#include "PdxInstanceFactoryImpl.hpp"
#include "PdxInstanceImpl.hpp"
#include "DotNetTypes.hpp"
using namespace System::Text;

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
          PdxInstanceFactoryImpl::PdxInstanceFactoryImpl(String^ className)
          {
            if(className == nullptr)
             throw gcnew IllegalStateException(
                "Classname should not be null.");
            m_pdxType = gcnew PdxType(className, false);
            m_FieldVsValues = gcnew Dictionary<String^, Object^>();
            m_created = false;
          }

          IPdxInstance^ PdxInstanceFactoryImpl::Create()
          {
            if(m_created)
            {
              throw gcnew IllegalStateException(
                "The IPdxInstanceFactory.Create() method can only be called once.");
            }
            //need to get typeid;
            IPdxInstance^ pi = gcnew PdxInstanceImpl(m_FieldVsValues, m_pdxType);
            m_created = true;
            return pi;
          }

          IPdxInstanceFactory^ PdxInstanceFactoryImpl::WriteChar(String^ fieldName, Char value)
          {
            isFieldAdded(fieldName);
            m_pdxType->AddFixedLengthTypeField(fieldName, "char", PdxTypes::CHAR, GemFireClassIds::CHAR_SIZE);
            m_FieldVsValues->Add(fieldName, value);
            return this;
          }
   
          IPdxInstanceFactory^ PdxInstanceFactoryImpl::WriteBoolean(String^ fieldName, Boolean value)
          {
            isFieldAdded(fieldName);
            m_pdxType->AddFixedLengthTypeField(fieldName, "boolean", PdxTypes::BOOLEAN, GemFireClassIds::BOOLEAN_SIZE);
            m_FieldVsValues->Add(fieldName, value);
            return this;
          }
    
          IPdxInstanceFactory^ PdxInstanceFactoryImpl::WriteByte(String^ fieldName, SByte value)
          {
            isFieldAdded(fieldName);
            m_pdxType->AddFixedLengthTypeField(fieldName, "byte", PdxTypes::BYTE, GemFireClassIds::BYTE_SIZE);
            m_FieldVsValues->Add(fieldName, value);
            return this;
          }
  
          IPdxInstanceFactory^ PdxInstanceFactoryImpl::WriteShort(String^ fieldName, Int16 value)
          {
            isFieldAdded(fieldName);
            m_pdxType->AddFixedLengthTypeField(fieldName, "short", PdxTypes::SHORT, GemFireClassIds::SHORT_SIZE);
            m_FieldVsValues->Add(fieldName, value);
            return this;
          }
  
          IPdxInstanceFactory^ PdxInstanceFactoryImpl::WriteInt(String^ fieldName, Int32 value)
          {
            isFieldAdded(fieldName);
            m_pdxType->AddFixedLengthTypeField(fieldName, "int", PdxTypes::INT, GemFireClassIds::INTEGER_SIZE);
            m_FieldVsValues->Add(fieldName, value);
            return this;
          }
  
          IPdxInstanceFactory^ PdxInstanceFactoryImpl::WriteLong(String^ fieldName, Int64 value)
          {
            isFieldAdded(fieldName);
            m_pdxType->AddFixedLengthTypeField(fieldName, "long", PdxTypes::LONG, GemFireClassIds::LONG_SIZE);
            m_FieldVsValues->Add(fieldName, value);
            return this;
          }
  
          IPdxInstanceFactory^ PdxInstanceFactoryImpl::WriteFloat(String^ fieldName, float value)
          {
            isFieldAdded(fieldName);
            m_pdxType->AddFixedLengthTypeField(fieldName, "float", PdxTypes::FLOAT, GemFireClassIds::FLOAT_SIZE);
            m_FieldVsValues->Add(fieldName, value);
            return this;
          }
  
          IPdxInstanceFactory^ PdxInstanceFactoryImpl::WriteDouble(String^ fieldName, double value)
          {
            isFieldAdded(fieldName);
            m_pdxType->AddFixedLengthTypeField(fieldName, "double", PdxTypes::DOUBLE, GemFireClassIds::DOUBLE_SIZE);
            m_FieldVsValues->Add(fieldName, value);
            return this;
          }
  
          IPdxInstanceFactory^ PdxInstanceFactoryImpl::WriteDate(String^ fieldName, System::DateTime value)
          {
            isFieldAdded(fieldName);
            m_pdxType->AddFixedLengthTypeField(fieldName, "Date", PdxTypes::DATE, GemFireClassIds::DATE_SIZE);
            m_FieldVsValues->Add(fieldName, value);
            return this;
          }
  
          IPdxInstanceFactory^ PdxInstanceFactoryImpl::WriteString(String^ fieldName, String^ value)
          {
            isFieldAdded(fieldName);
            m_pdxType->AddVariableLengthTypeField(fieldName, "String", PdxTypes::STRING);
            m_FieldVsValues->Add(fieldName, value);
            return this;
          }
        
          IPdxInstanceFactory^ PdxInstanceFactoryImpl::WriteObject(String^ fieldName, Object^ value)
          {
            isFieldAdded(fieldName);
            m_pdxType->AddVariableLengthTypeField(fieldName, /*obj->GetType()->FullName*/"Object", PdxTypes::OBJECT);
            m_FieldVsValues->Add(fieldName, value);
            return this;
          }
  
          IPdxInstanceFactory^ PdxInstanceFactoryImpl::WriteBooleanArray(String^ fieldName, array<Boolean>^ value)
          {
            isFieldAdded(fieldName);
            m_pdxType->AddVariableLengthTypeField(fieldName, "bool[]", PdxTypes::BOOLEAN_ARRAY);
            m_FieldVsValues->Add(fieldName, value);
            return this;
          }
  
          IPdxInstanceFactory^ PdxInstanceFactoryImpl::WriteCharArray(String^ fieldName, array<Char>^ value)
          {
            isFieldAdded(fieldName);
            m_pdxType->AddVariableLengthTypeField(fieldName, "char[]", PdxTypes::CHAR_ARRAY);
            m_FieldVsValues->Add(fieldName, value);
            return this;
          }
        
          IPdxInstanceFactory^ PdxInstanceFactoryImpl::WriteByteArray(String^ fieldName, array<Byte>^ value)
          {
            isFieldAdded(fieldName);
            m_pdxType->AddVariableLengthTypeField(fieldName, "byte[]", PdxTypes::BYTE_ARRAY);
            m_FieldVsValues->Add(fieldName, value);
            return this;
          }
        
          IPdxInstanceFactory^ PdxInstanceFactoryImpl::WriteShortArray(String^ fieldName, array<Int16>^ value)
          {
            isFieldAdded(fieldName);
            m_pdxType->AddVariableLengthTypeField(fieldName, "short[]", PdxTypes::SHORT_ARRAY);
            m_FieldVsValues->Add(fieldName, value);
            return this;
          }
        
          IPdxInstanceFactory^ PdxInstanceFactoryImpl::WriteIntArray(String^ fieldName, array<Int32>^ value)
          {
            isFieldAdded(fieldName);
            m_pdxType->AddVariableLengthTypeField(fieldName, "int[]", PdxTypes::INT_ARRAY);
            m_FieldVsValues->Add(fieldName, value);
            return this;
          }
        
          IPdxInstanceFactory^ PdxInstanceFactoryImpl::WriteLongArray(String^ fieldName, array<Int64>^ value)
          {
            isFieldAdded(fieldName);
            m_pdxType->AddVariableLengthTypeField(fieldName, "long[]", PdxTypes::LONG_ARRAY);
            m_FieldVsValues->Add(fieldName, value);
            return this;
          }
        
          IPdxInstanceFactory^ PdxInstanceFactoryImpl::WriteFloatArray(String^ fieldName, array<float>^ value)
          {
            isFieldAdded(fieldName);
            m_pdxType->AddVariableLengthTypeField(fieldName, "float[]", PdxTypes::FLOAT_ARRAY);
            m_FieldVsValues->Add(fieldName, value);
            return this;
          }
        
          IPdxInstanceFactory^ PdxInstanceFactoryImpl::WriteDoubleArray(String^ fieldName, array<double>^ value)
          {
            isFieldAdded(fieldName);
            m_pdxType->AddVariableLengthTypeField(fieldName, "double[]", PdxTypes::DOUBLE_ARRAY);
            m_FieldVsValues->Add(fieldName, value);
            return this;
          }
        
          IPdxInstanceFactory^ PdxInstanceFactoryImpl::WriteStringArray(String^ fieldName, array<String^>^ value)
          {
            isFieldAdded(fieldName);
            m_pdxType->AddVariableLengthTypeField(fieldName, "String[]", PdxTypes::STRING_ARRAY);
            m_FieldVsValues->Add(fieldName, value);
            return this;
          }
        
          IPdxInstanceFactory^ PdxInstanceFactoryImpl::WriteObjectArray(String^ fieldName, System::Collections::Generic::List<Object^>^ value)
          {
            isFieldAdded(fieldName);
            m_pdxType->AddVariableLengthTypeField(fieldName, "Object[]", PdxTypes::OBJECT_ARRAY);
            m_FieldVsValues->Add(fieldName, value);
            return this;
          }
              
          IPdxInstanceFactory^ PdxInstanceFactoryImpl::WriteArrayOfByteArrays(String^ fieldName, array<array<Byte>^>^ value)
          {
            isFieldAdded(fieldName);
            m_pdxType->AddVariableLengthTypeField(fieldName, "byte[][]", PdxTypes::ARRAY_OF_BYTE_ARRAYS);
            m_FieldVsValues->Add(fieldName, value);
            return this;
          }
        
          IPdxInstanceFactory^ PdxInstanceFactoryImpl::WriteField(String^ fieldName, Object^ fieldValue, Type^ type)
          {
            isFieldAdded(fieldName);
            if(type->Equals(DotNetTypes::IntType))
            {
              return this->WriteInt(fieldName, (int)fieldValue);
            }
            else if(type->Equals(DotNetTypes::StringType))
            {
              return this->WriteString(fieldName, (String^)fieldValue);
            }
            else if(type->Equals(DotNetTypes::BooleanType))
            {
              return this->WriteBoolean(fieldName, (bool)fieldValue);
            }
            else if(type->Equals(DotNetTypes::FloatType))
            {
              return this->WriteFloat(fieldName, (float)fieldValue);
            }
            else if(type->Equals(DotNetTypes::DoubleType))
            {
              return this->WriteDouble(fieldName, (double)fieldValue);
            }
            else if(type->Equals(DotNetTypes::CharType))
            {
              return this->WriteChar(fieldName, (Char)fieldValue);
            }
            else if(type->Equals(DotNetTypes::SByteType))
            {
              return this->WriteByte(fieldName, (SByte)fieldValue);
            }
            else if(type->Equals(DotNetTypes::ShortType))
            {
              return this->WriteShort(fieldName, (short)fieldValue);
            }
            else if(type->Equals(DotNetTypes::LongType))
            {
              return this->WriteLong(fieldName, (Int64)fieldValue);
            }
            else if(type->Equals(DotNetTypes::ByteArrayType))
            {
              return this->WriteByteArray(fieldName, (array<Byte>^)fieldValue);
            }
            else if(type->Equals(DotNetTypes::DoubleArrayType))
            {
              return this->WriteDoubleArray(fieldName, (array<double>^)fieldValue);
            }
            else if(type->Equals(DotNetTypes::FloatArrayType))
            {
              return this->WriteFloatArray(fieldName, (array<float>^)fieldValue);
            }
            else if(type->Equals(DotNetTypes::ShortArrayType))
            {
              return this->WriteShortArray(fieldName, (array<Int16>^)fieldValue);
            }
            else if(type->Equals(DotNetTypes::IntArrayType))
            {
              return this->WriteIntArray(fieldName, (array<int32>^)fieldValue);
            }
            else if(type->Equals(DotNetTypes::LongArrayType))
            {
              return this->WriteLongArray(fieldName, (array<Int64>^)fieldValue);
            }
            else if(type->Equals(DotNetTypes::BoolArrayType))
            {
              return this->WriteBooleanArray(fieldName, (array<bool>^)fieldValue);
            }
            else if(type->Equals(DotNetTypes::CharArrayType))
            {
              return this->WriteCharArray(fieldName, (array<Char>^)fieldValue);
            }
            else if(type->Equals(DotNetTypes::StringArrayType))
            {
              return this->WriteStringArray(fieldName, (array<String^>^)fieldValue);
            }
            else if(type->Equals(DotNetTypes::DateType))
            {
              return this->WriteDate(fieldName, (DateTime)fieldValue);
            }
            else if(type->Equals(DotNetTypes::ByteArrayOfArrayType))
            {
              return this->WriteArrayOfByteArrays(fieldName, (array<array<Byte>^>^)fieldValue);
            }
            else if(type->Equals(DotNetTypes::ObjectArrayType))
            {
              return this->WriteObjectArray(fieldName, safe_cast<System::Collections::Generic::List<Object^>^>(fieldValue));
            }
            else
            {
              return this->WriteObject(fieldName, fieldValue);
              //throw gcnew IllegalStateException("WriteField unable to serialize  " 
								//																	+ fieldName + " of " + type); 
            }
           // return this;
          }
                
          IPdxInstanceFactory^ PdxInstanceFactoryImpl::MarkIdentityField(String^ fieldName)
          {
            PdxFieldType^ pft = m_pdxType->GetPdxField(fieldName);

            if(pft == nullptr)
            {
              throw gcnew IllegalStateException(
                "Field must be added before calling MarkIdentityField ");
            }

            pft->IdentityField = true;
            return this;
          }

          void PdxInstanceFactoryImpl::isFieldAdded(String^ fieldName)
          {
            if(fieldName == nullptr || fieldName->Length == 0 || m_FieldVsValues->ContainsKey(fieldName))
            {
              throw gcnew IllegalStateException(
                "Field: " + fieldName + " either already added into PdxInstanceFactory or it is null");
            }
          }
        }       
			}
    }
  }
}
