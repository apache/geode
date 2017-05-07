/*=========================================================================
 * Copyright (c) 2002-2014 Pivotal Software, Inc. All Rights Reserved.
 * This product is protected by U.S. and international copyright
 * and intellectual property laws. Pivotal products are covered by
 * more patents listed at http://www.pivotal.io/patents.
 *========================================================================
 */

/*
 * @brief ExampleObject class for testing the put functionality for object. 
 */

using System;
namespace GemStone.GemFire.Cache.Examples
{
  public class ExampleObject
    : IGFSerializable
  {
    #region Private members
    private double double_field;
    private float  float_field;
    private long  long_field;
    private int  int_field;
    private short  short_field;
    private string  string_field;
      private System.Collections.ArrayList string_vector = new System.Collections.ArrayList();
    #endregion

    #region Public accessors
    public int Int_Field
    {
      get
      {
        return int_field;
      }
      set
      {
        int_field = value;
      }
    }
    public short Short_Field
    {
      get
      {
        return short_field;
      }
      set
      {
        short_field = value;
      }
    }
    public long Long_Field
    {
      get
      {
        return long_field;
      }
      set
      {
        long_field = value;
      }
    }
    public float Float_Field
    {
      get
      {
        return float_field;
      }
      set
      {
        float_field = value;
      }
    }
    public double Double_Field
    {
      get
      {
        return double_field;
      }
      set
      {
        double_field = value;
      }
    }
    public string String_Field
    {
      get
      {
        return string_field;
      }
      set
      {
        string_field = value;
      }
    }
    public System.Collections.ArrayList String_Vector
    {
      get
      {
        return string_vector;
      }
      set
      {
        string_vector = value;
      }
    }
    public override string ToString()
    {
        string buffer = "ExampleObject: "+int_field+"(int),"+string_field+"(str),";
        buffer += "[";
        for (int idx = 0; idx < string_vector.Count; idx++)
        {
            buffer += string_vector[idx];
        }
        buffer += "(string_vector)]";
        return buffer;
    }
    #endregion

    #region Constructors
    public ExampleObject()
    {
      double_field = (double)0.0;
      float_field = (float)0.0;
      long_field = 0;
      int_field = 0;
      short_field = 0;
      string_field = "";
      string_vector.Clear();
    }

    public ExampleObject(int id) {
      int_field = id;
      short_field = (Int16)id;
      long_field = (Int64)id;
      float_field = (float)id;
      double_field = (double)id;
      string_field = ""+id;
      string_vector.Clear();
      for (int i=0; i<3; i++) {
        string_vector.Add(string_field);
      }
    }
    public ExampleObject(string sValue) {
      int_field = Int32.Parse(sValue);
      long_field = Int64.Parse(sValue);
      short_field = Int16.Parse(sValue);
      double_field = (double)int_field;
      float_field = (float)int_field;
      string_field = sValue;
      string_vector.Clear();
      for (int i=0; i<3; i++) {
        string_vector.Add(sValue);
      }
    }
    #endregion

    #region IGFSerializable Members
    public IGFSerializable FromData(DataInput input)
    {
      double_field = input.ReadInt64();
      float_field = input.ReadFloat();
      long_field = input.ReadInt64();
      int_field = input.ReadInt32();
      short_field = input.ReadInt16();
      string_field = input.ReadUTF();
      int itemCount = input.ReadInt32();
      string_vector.Clear();
      for (int idx = 0; idx<itemCount; itemCount++) {
          string_vector.Add(input.ReadUTF());
      }
      return this;
    }

    public void ToData(DataOutput output)
    {
      output.WriteDouble( double_field );
      output.WriteFloat( float_field );
      output.WriteInt64( long_field );
      output.WriteInt32( int_field );
      output.WriteInt16( short_field );
      output.WriteUTF( string_field );
      int itemCount = string_vector.Count;
      output.WriteInt32( itemCount );
      for( int idx = 0; idx < itemCount; idx++ ) {
          string s = (string)string_vector[idx];
        output.WriteUTF( s );
      }
    }

    public UInt32 ObjectSize
    {
      get
      {
        UInt32 objectSize = 0;
        objectSize += (UInt32)sizeof(double);
        objectSize += (UInt32)sizeof(float);
        objectSize += (UInt32)sizeof(Int64);
        objectSize += (UInt32)sizeof(Int32);
        objectSize += (UInt32)sizeof(Int16);
        objectSize += (UInt32)(string_field == null ? 0 : sizeof(char) * string_field.Length);
        objectSize += (UInt32)sizeof(Int32);
        for (int idx = 0; idx<string_vector.Count; idx++) {
            string s = (string)string_vector[idx];
            objectSize += (UInt32)(string_vector[idx] == null ? 0 : sizeof(char) * s.Length);
        }
        return objectSize;
      }
    }

    public UInt32 ClassId
    {
      get
      {
        return 0x2e;
      }
    }
    #endregion

    public static IGFSerializable CreateDeserializable()
    {
      return new ExampleObject();
    }
  }

};



