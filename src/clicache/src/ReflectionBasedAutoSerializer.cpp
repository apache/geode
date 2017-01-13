/*=========================================================================
 * Copyright (c) 2010-2014 Pivotal Software, Inc. All Rights Reserved.
 * This product is protected by U.S. and international copyright
 * and intellectual property laws. Pivotal products are covered by
 * one or more patents listed at http://www.pivotal.io/patents.
 *=========================================================================
 */
#include "ReflectionBasedAutoSerializer.hpp"
#include "PdxIdentityFieldAttribute.hpp"
#include "Serializable.hpp"
#pragma warning(disable:4091)
#include <msclr/lock.h>
#include "ExceptionTypes.hpp"
#include "impl/DotNetTypes.hpp"
namespace GemStone
{
  namespace GemFire
  {
    namespace Cache
    {
      namespace Generic
      {
         ref class FieldWrapper
        {
          private:
             //static readonly Module Module = typeof(Program).Module;
            static array<Type^>^ oneObj = gcnew array<Type^>(1) { Type::GetType("System.Object") };
            static array<Type^>^ twoObj = gcnew array<Type^>(2) { Type::GetType("System.Object"), Type::GetType("System.Object") };
            delegate void MySetter(Object^ t1, Object^ t2);
            delegate Object^ MyGetter(Object^ t1);

            static Type^ setterDelegateType = Type::GetType("GemStone.GemFire.Cache.Generic.FieldWrapper+MySetter");
            static Type^ getterDelegateType = Type::GetType("GemStone.GemFire.Cache.Generic.FieldWrapper+MyGetter");

            FieldInfo^ m_fi;
            String^    m_fieldName;
            bool       m_isIdentityField;
            FieldType  m_fieldType;
            int        m_pdxType;

            MyGetter^ m_getter;
            MySetter^ m_setter;

            static MySetter^ createFieldSetter(FieldInfo^ fieldInfo)
            {
              DynamicMethod^ dynam = gcnew DynamicMethod("", Internal::DotNetTypes::VoidType , twoObj, fieldInfo->DeclaringType, true);
              ILGenerator^ il = dynam->GetILGenerator();

              if (!fieldInfo->IsStatic)
                pushInstance(il, fieldInfo->DeclaringType);

              il->Emit(OpCodes::Ldarg_1);
              unboxIfNeeded(il, fieldInfo->FieldType);
              il->Emit(OpCodes::Stfld, fieldInfo);
              il->Emit(OpCodes::Ret);

              return (MySetter^)dynam->CreateDelegate( setterDelegateType );
            }

            static MyGetter^ createFieldGetter(FieldInfo^ fieldInfo)
            {
              DynamicMethod^ dynam = gcnew DynamicMethod( "", Internal::DotNetTypes::ObjectType, oneObj, fieldInfo->DeclaringType, true);
              ILGenerator^ il = dynam->GetILGenerator();

              if (!fieldInfo->IsStatic)
                pushInstance(il, fieldInfo->DeclaringType);

              il->Emit(OpCodes::Ldfld, fieldInfo);
              boxIfNeeded(il, fieldInfo->FieldType);
              il->Emit(OpCodes::Ret);

              return (MyGetter^)dynam->CreateDelegate(getterDelegateType);
            }

            static void boxIfNeeded(ILGenerator^ il, Type^ type)
            {
              if (type->IsValueType)
                il->Emit(OpCodes::Box, type);
            }

            static void pushInstance( ILGenerator^ il, Type^ type)
            {
              il->Emit(OpCodes::Ldarg_0);
              if (type->IsValueType)
                il->Emit(OpCodes::Unbox, type);
            }

            static void unboxIfNeeded( ILGenerator^ il, Type^ type)
            {
              if (type->IsValueType)
                il->Emit(OpCodes::Unbox_Any, type);
            }
          public:
            FieldWrapper(FieldInfo^ fi, String^ fieldName, bool isIdentityField, FieldType  fieldtype)
            {
              m_fi = fi;
              m_fieldName = fieldName;
              m_isIdentityField = isIdentityField;
              m_fieldType = fieldtype;

              m_setter = createFieldSetter(fi);
              m_getter = createFieldGetter(fi);
            }

            property bool isIdentityField
            {
              bool get(){return m_isIdentityField;}
            }

            property Type^ FType
            {
               Type^ get() {return m_fi->FieldType;}
            }

            property String^ FieldName
            {
             String^ get(){return m_fieldName;}
            }

            property FieldInfo^ FI
            {
              FieldInfo^  get(){return m_fi;}
            }
            
            void SetFieldValue(Object^ parent, Object^ val)
            {
              m_setter(parent, val);
            }

            Object^ GetFieldValue(Object^ parent)
            {
              return m_getter(parent);
            }

            void SerializeField(IPdxWriter^ w, Object^ value)
            {
              switch(m_fieldType)
              {
                case FieldType::BOOLEAN:
                  w->WriteBoolean(m_fieldName, (bool)value);
                break;
			          case FieldType::BYTE:
                  w->WriteByte(m_fieldName, (SByte)value);
                break;
			          case FieldType::CHAR:
                  w->WriteChar(m_fieldName, (Char)value);
                break;
			          case FieldType::SHORT:
                  w->WriteShort(m_fieldName, (short)value);
                break;
			          case FieldType::INT:
                  w->WriteInt(m_fieldName, (int)value);
                break;
			          case FieldType::LONG:
                  w->WriteLong(m_fieldName, (Int64)value);
                break;
			          case FieldType::FLOAT:
                  w->WriteFloat(m_fieldName, (float)value);
                break;
			          case FieldType::DOUBLE:
                  w->WriteDouble(m_fieldName, (double)value);
                break;
			          case FieldType::DATE:
                  w->WriteDate(m_fieldName, (DateTime)value);
                break;
			          case FieldType::STRING:
                  w->WriteString(m_fieldName, (String^)value);
                break;
			          case FieldType::OBJECT:
                  w->WriteObject(m_fieldName, value);
                break;
			          case FieldType::BOOLEAN_ARRAY:
                  w->WriteBooleanArray(m_fieldName, (array<bool>^)value);
                break;
			          case FieldType::CHAR_ARRAY:
                  w->WriteCharArray(m_fieldName, (array<Char>^)value);
                break;
			          case FieldType::BYTE_ARRAY:
                  w->WriteByteArray(m_fieldName, (array<Byte>^)value);
                break;
			          case FieldType::SHORT_ARRAY:
                  w->WriteShortArray(m_fieldName, (array<Int16>^)value);
                break;
			          case FieldType::INT_ARRAY:
                  w->WriteIntArray(m_fieldName, (array<Int32>^)value);
                break;
			          case FieldType::LONG_ARRAY:
                  w->WriteLongArray(m_fieldName, (array<int64>^)value);
                break;
			          case FieldType::FLOAT_ARRAY:
                  w->WriteFloatArray(m_fieldName, (array<float>^)value);
                break;
			          case FieldType::DOUBLE_ARRAY:
                  w->WriteDoubleArray(m_fieldName, (array<double>^)value);
                break;
			          case FieldType::STRING_ARRAY:
                  w->WriteStringArray(m_fieldName, (array<String^>^)value);
                break;
			          case FieldType::OBJECT_ARRAY:
                  w->WriteObjectArray(m_fieldName, safe_cast<System::Collections::Generic::List<Object^>^>(value));
                break;
			          case FieldType::ARRAY_OF_BYTE_ARRAYS:
                  w->WriteArrayOfByteArrays(m_fieldName, (array<array<Byte>^>^)value);
                break;
			          default:
                  throw gcnew IllegalStateException("Not found FieldType: " + m_fieldType.ToString());
              }
            }

            Object^ DeserializeField(IPdxReader^ r)
            {
             switch(m_fieldType)
              {
                case FieldType::BOOLEAN:
                  return r->ReadBoolean(m_fieldName);
                break;
			          case FieldType::BYTE:
                  return r->ReadByte(m_fieldName);
                break;
			          case FieldType::CHAR:
                  return r->ReadChar(m_fieldName);
                break;
			          case FieldType::SHORT:
                  return r->ReadShort(m_fieldName);
                break;
			          case FieldType::INT:
                  return r->ReadInt(m_fieldName);
                break;
			          case FieldType::LONG:
                  return r->ReadLong(m_fieldName);
                break;
			          case FieldType::FLOAT:
                  return r->ReadFloat(m_fieldName);
                break;
			          case FieldType::DOUBLE:
                  return r->ReadDouble(m_fieldName);
                break;
			          case FieldType::DATE:
                  return r->ReadDate(m_fieldName);
                break;
			          case FieldType::STRING:
                  return r->ReadString(m_fieldName);
                break;
			          case FieldType::OBJECT:
                  return r->ReadObject(m_fieldName);
                break;
			          case FieldType::BOOLEAN_ARRAY:
                  return r->ReadBooleanArray(m_fieldName);
                break;
			          case FieldType::CHAR_ARRAY:
                  return r->ReadCharArray(m_fieldName);
                break;
			          case FieldType::BYTE_ARRAY:
                  return r->ReadByteArray(m_fieldName);
                break;
			          case FieldType::SHORT_ARRAY:
                  return r->ReadShortArray(m_fieldName);
                break;
			          case FieldType::INT_ARRAY:
                  return r->ReadIntArray(m_fieldName);
                break;
			          case FieldType::LONG_ARRAY:
                  return r->ReadLongArray(m_fieldName);
                break;
			          case FieldType::FLOAT_ARRAY:
                  return r->ReadFloatArray(m_fieldName);
                break;
			          case FieldType::DOUBLE_ARRAY:
                  return r->ReadDoubleArray(m_fieldName);
                break;
			          case FieldType::STRING_ARRAY:
                  return r->ReadStringArray(m_fieldName);
                break;
			          case FieldType::OBJECT_ARRAY:
                  return r->ReadObjectArray(m_fieldName);
                break;
			          case FieldType::ARRAY_OF_BYTE_ARRAYS:
                  return r->ReadArrayOfByteArrays(m_fieldName);
                break;
			          default:
                  throw gcnew IllegalStateException("Not found FieldType: " + m_fieldType.ToString());
              }
              return nullptr;
            }



        };

        ReflectionBasedAutoSerializer::ReflectionBasedAutoSerializer()
        {
          PdxIdentityFieldAttribute^ pif = gcnew PdxIdentityFieldAttribute();
          PdxIdentityFieldAttributeType = pif->GetType();
          classNameVsFieldInfoWrapper = gcnew Dictionary<String^, List<FieldWrapper^>^>();
        }

		    bool ReflectionBasedAutoSerializer::ToData( Object^ o,IPdxWriter^ writer )
        {
          serializeFields(o, writer);
          return true;
        }

        Object^ ReflectionBasedAutoSerializer::FromData(String^ o, IPdxReader^ reader )
        {
          return deserializeFields(o, reader);
        }

        void ReflectionBasedAutoSerializer::serializeFields(Object^ o,IPdxWriter^ writer )
        {
          Type^ ty = o->GetType();
         // Log::Debug("ReflectionBasedAutoSerializer::serializeFields classname {0}: objectType {1}", o->GetType()->FullName,o->GetType());
          for each(FieldWrapper^ fi in GetFields(o->GetType()))
          {
           // Log::Debug("ReflectionBasedAutoSerializer::serializeFields fieldName: {0}, fieldType: {1}", fi->FieldName, fi->FType);
           // writer->WriteField(fi->Name, fi->GetValue(o), fi->FieldType);           
           // SerializeField(o, fi, writer);
            //Object^ originalValue = fi->FI->GetValue(o);
            Object^ originalValue = fi->GetFieldValue(o);
            //hook which can overide by app
            originalValue = WriteTransform(fi->FI, fi->FType, originalValue);

            fi->SerializeField(writer, originalValue);

            if(fi->isIdentityField)
            {
             // Log::Debug("ReflectionBasedAutoSerializer::serializeFields fieldName: {0} is identity field.", fi->FieldName);
              writer->MarkIdentityField(fi->FieldName);
            }
          }

        //  serializeBaseClassFields(o, writer, ty->BaseType);
        }

		  
        /*void ReflectionBasedAutoSerializer::SerializeField(Object^ o, FieldInfo^ fi, IPdxWriter^ writer)
        {
          writer->WriteField(fi->Name, fi->GetValue(o), fi->FieldType);
        }

        Object^ ReflectionBasedAutoSerializer::DeserializeField(Object^ o, FieldInfo^ fi, IPdxReader^ reader)
        {
           return reader->ReadField(fi->Name,  fi->FieldType);  
        }*/

        Object^ ReflectionBasedAutoSerializer::deserializeFields(String^ className, IPdxReader^ reader)
        {
          Object^ o = CreateObject(className);
          //Log::Debug("ReflectionBasedAutoSerializer::deserializeFields classname {0}: objectType {1}", className,o->GetType());
          for each(FieldWrapper^ fi in GetFields(o->GetType()))
          {
            //Log::Debug("1ReflectionBasedAutoSerializer::deserializeFields fieldName: {0}, fieldType: {1}", fi->FieldName, fi->FType);
            Object^ serializeValue = fi->DeserializeField(reader);
            serializeValue = ReadTransform( fi->FI, fi->FType, serializeValue);
            //fi->FI->SetValue(o, serializeValue);            
            fi->SetFieldValue(o, serializeValue);
          }

          return o;
          //deserializeBaseClassFields(o, reader, ty->BaseType);
        }
        
        Object^ ReflectionBasedAutoSerializer::CreateObject(String^ className)
        {
          return Serializable::CreateObject(className);
        }

        bool ReflectionBasedAutoSerializer::IsPdxIdentityField(FieldInfo^ fi)
        {
          array<Object^>^ cAttr=  fi->GetCustomAttributes(PdxIdentityFieldAttributeType, true);
          if(cAttr != nullptr && cAttr->Length > 0)
          {
            PdxIdentityFieldAttribute^ pifa = (PdxIdentityFieldAttribute^)(cAttr[0]);
            return true;
          }
          return false;
        }

        List<FieldWrapper^>^ ReflectionBasedAutoSerializer::GetFields(Type^ domaimType)
        {
          List<FieldWrapper^>^ retVal = nullptr;

          String^ className = domaimType->FullName;
          System::Collections::Generic::Dictionary<String^, List<FieldWrapper^>^>^ tmp = classNameVsFieldInfoWrapper;
          tmp->TryGetValue(className, retVal);
          if(retVal != nullptr)
            return retVal;
          msclr::lock lockInstance(classNameVsFieldInfoWrapper);
          {
            tmp = classNameVsFieldInfoWrapper;
            tmp->TryGetValue(className, retVal);
            if(retVal != nullptr)
              return retVal;
             
            List<FieldWrapper^>^ collectFields = gcnew List<FieldWrapper^>();
             while(domaimType != nullptr)
             {
                for each(FieldInfo^ fi in domaimType->GetFields(BindingFlags::Public| BindingFlags::NonPublic | BindingFlags::Instance
                  |BindingFlags::DeclaredOnly
                  ))
                {
                  if(!fi->IsNotSerialized && !fi->IsStatic && !fi->IsLiteral && !fi->IsInitOnly)
                  {
                    //to ignore the fild
                    if(IsFieldIncluded(fi, domaimType))
                    {                      
                      //This are all hooks which app can implement

                      String^ fieldName = GetFieldName(fi, domaimType);
                      bool isIdentityField = IsIdentityField(fi, domaimType);
                      FieldType ft = GetFieldType(fi, domaimType);
  
                      FieldWrapper^ fw = gcnew FieldWrapper(fi, fieldName, isIdentityField, ft);

                      collectFields->Add(fw);
                    }
                  }
                }
                domaimType = domaimType->BaseType;
             }
             tmp = gcnew System::Collections::Generic::Dictionary<String^, List<FieldWrapper^>^>(classNameVsFieldInfoWrapper); 
             tmp->Add(className, collectFields);
             classNameVsFieldInfoWrapper = tmp;

             return collectFields;
          }
        }

        
        String^ ReflectionBasedAutoSerializer::GetFieldName(FieldInfo^ fi, Type^ type)
        {
          return fi->Name;
        }

        bool ReflectionBasedAutoSerializer::IsIdentityField(FieldInfo^ fi, Type^ type)
        {
          return IsPdxIdentityField(fi);
        }

        FieldType ReflectionBasedAutoSerializer::GetFieldType(FieldInfo^ fi, Type^ type)
        {
          return getPdxFieldType(fi->FieldType);
        }

        bool ReflectionBasedAutoSerializer::IsFieldIncluded(FieldInfo^ fi, Type^ type)
        {
          return true;
        }

        Object^ ReflectionBasedAutoSerializer::WriteTransform(FieldInfo^ fi, Type^ type, Object^ originalValue)
        {
          return originalValue;
        }

        Object^ ReflectionBasedAutoSerializer::ReadTransform(FieldInfo^ fi, Type^ type, Object^ serializeValue)
        {
          return serializeValue;
        }

        FieldType ReflectionBasedAutoSerializer::getPdxFieldType( Type^ type)
        {
          if(type->Equals(Internal::DotNetTypes::IntType))
          {
            return FieldType::INT;
          }
          else if(type->Equals(Internal::DotNetTypes::StringType))
          {
            return FieldType::STRING;
          }
          else if(type->Equals(Internal::DotNetTypes::BooleanType))
          {
            return FieldType::BOOLEAN;
          }
          else if(type->Equals(Internal::DotNetTypes::FloatType))
          {
            return FieldType::FLOAT;
          }
          else if(type->Equals(Internal::DotNetTypes::DoubleType))
          {
            return FieldType::DOUBLE;
          }
          else if(type->Equals(Internal::DotNetTypes::CharType))
          {
            return FieldType::CHAR;
          }
          else if(type->Equals(Internal::DotNetTypes::SByteType))
          {
            return FieldType::BYTE;
          }
          else if(type->Equals(Internal::DotNetTypes::ShortType))
          {
            return FieldType::SHORT;
          }
          else if(type->Equals(Internal::DotNetTypes::LongType))
          {
            return FieldType::LONG;
          }
          else if(type->Equals(Internal::DotNetTypes::ByteArrayType))
          {
            return FieldType::BYTE_ARRAY;
          }
          else if(type->Equals(Internal::DotNetTypes::DoubleArrayType))
          {
            return FieldType::DOUBLE_ARRAY;
          }
          else if(type->Equals(Internal::DotNetTypes::FloatArrayType))
          {
            return FieldType::FLOAT_ARRAY;
          }
          else if(type->Equals(Internal::DotNetTypes::ShortArrayType))
          {
            return FieldType::SHORT_ARRAY;
          }
          else if(type->Equals(Internal::DotNetTypes::IntArrayType))
          {
            return FieldType::INT_ARRAY;
          }
          else if(type->Equals(Internal::DotNetTypes::LongArrayType))
          {
            return FieldType::LONG_ARRAY;
          }
          else if(type->Equals(Internal::DotNetTypes::BoolArrayType))
          {
            return FieldType::BOOLEAN_ARRAY;
          }
          else if(type->Equals(Internal::DotNetTypes::CharArrayType))
          {
            return FieldType::CHAR_ARRAY;
          }
          else if(type->Equals(Internal::DotNetTypes::StringArrayType))
          {
            return FieldType::STRING_ARRAY;
          }
          else if(type->Equals(Internal::DotNetTypes::DateType))
          {
            return FieldType::DATE;
          }
          else if(type->Equals(Internal::DotNetTypes::ByteArrayOfArrayType))
          {
            return FieldType::ARRAY_OF_BYTE_ARRAYS;
          }
          /*else if(type->Equals(Internal::DotNetTypes::ObjectArrayType))
          {
            //Giving more preference to arraylist instead of Object[] in java side
            //return this->WriteObjectArray(fieldName, safe_cast<System::Collections::Generic::List<Object^>^>(fieldValue));
            return FieldType::OBJECT_ARRAY;
          }*/
          else
          {
            return FieldType::OBJECT;
            //throw gcnew IllegalStateException("WriteField unable to serialize  " 
							//																	+ fieldName + " of " + type); 
          }
        }

       
      } // end namespace Generic
    }
  }
}