/*=========================================================================
 * Copyright (c) 2002-2014 Pivotal Software, Inc. All Rights Reserved.
 * This product is protected by U.S. and international copyright
 * and intellectual property laws. Pivotal products are covered by
 * more patents listed at http://www.pivotal.io/patents.
 *========================================================================
 */

using System;
using System.Text.RegularExpressions;

namespace GemStone.GemFire.Cache.Examples
{
  /// <summary>
  /// A complex number -- no operations defined yet.
  /// </summary>
  public class ComplexNumber : GemStone.GemFire.Cache.Generic.ICacheableKey
  {
    #region Public Accessors

    /// <summary>
    /// The real portion of the complex number.
    /// </summary>
    public double Real
    {
      get
      {
        return m_real;
      }
      set
      {
        m_real = value;
      }
    }

    /// <summary>
    /// The imaginary portion of the complex number.
    /// </summary>
    public double Imaginary
    {
      get
      {
        return m_imaginary;
      }
      set
      {
        m_imaginary = value;
      }
    }

    #endregion

    #region Private members

    private double m_real;
    private double m_imaginary;

    #endregion

    #region Constructor and factory function

    /// <summary>
    /// Constructor.
    /// </summary>
    /// <param name="real">Real part</param>
    /// <param name="imaginary">Imaginary part</param>
    public ComplexNumber(double real, double imaginary)
    {
      m_real = real;
      m_imaginary = imaginary;
    }

    /// <summary>
    /// Factory function to be registered using
    /// <see cref="Serializable.registerType" />.
    /// </summary>
    public static GemStone.GemFire.Cache.Generic.IGFSerializable Create()
    {
      return new ComplexNumber(0.0, 0.0);
    }

    #endregion

    #region Serialization -- IGFSerializable members

    /// <summary>
    /// Read the complex number from the <see cref="DataInput" /> stream.
    /// </summary>
    /// <param name="input">The <c>DataInput</c> stream.</param>
    /// <returns>A reference to <c>this</c> object.</returns>
    public GemStone.GemFire.Cache.Generic.IGFSerializable FromData(GemStone.GemFire.Cache.Generic.DataInput input)
    {
      m_real = input.ReadDouble();
      m_imaginary = input.ReadDouble();
      return this;
    }

    /// <summary>
    /// Write the complex number to the <see cref="DataOutput" /> stream.
    /// </summary>
    /// <param name="output">The <c>DataOutput</c> stream.</param>
    public void ToData(GemStone.GemFire.Cache.Generic.DataOutput output)
    {
      output.WriteDouble(m_real);
      output.WriteDouble(m_imaginary);
    }

    /// <summary>
    /// return the size of this object in bytes
    /// </summary>
    public UInt32 ObjectSize
    {
      get
      {
        return (UInt32) (sizeof(double) + sizeof(double));
      }
    }

    /// <summary>
    /// Get the <see cref="IGFSerializable.ClassId" /> of this class.
    /// </summary>
    /// <returns>The classId.</returns>
    public UInt32 ClassId
    {
      get
      {
        return 0x04;
      }
    }

    #endregion

    #region ICacheableKey Members

    public bool Equals(GemStone.GemFire.Cache.Generic.ICacheableKey other)
    {
      ComplexNumber cOther = other as ComplexNumber;
      if (cOther != null)
      {
        return (m_real == cOther.m_real) && (m_imaginary == cOther.m_imaginary);
      }
      return false;
    }

    /// <summary>
    /// Gets the hashcode by XORing the hashcodes of real and imaginary parts
    /// </summary>
    public override int GetHashCode()
    {
      return m_real.GetHashCode() ^ m_imaginary.GetHashCode();
    }

    #endregion

    #region Overridden System.Object members

    /// <summary>
    /// Check whether this <c>ComplexNumber</c> is equal to the given object.
    /// </summary>
    /// <param name="obj">The object to use compare to.</param>
    /// <returns>True if the object is equal, false otherwise.</returns>
    public override bool Equals(object obj)
    {
      if (obj is ComplexNumber)
      {
        ComplexNumber other = obj as ComplexNumber;
        return (m_real == other.m_real) && (m_imaginary == other.m_imaginary);
      }
      return false;
    }

    /// <summary>
    /// Get a string representation of the form 'a+bi' for this complex number.
    /// </summary>
    /// <returns>The string representation of this object.</returns>
    public override string ToString()
    {
      return m_real.ToString() + '+' + m_imaginary.ToString() + 'i';
    }

    #endregion

    #region Read string representation and create new ComplexNumber

    /// <summary>
    /// A convenience Parse method to get a ComplexNumber from a given string.
    /// The format is expected to be of the form a+bi, such as "7+3.14i".
    /// </summary>
    /// <param name="s">The string to parse.</param>
    /// <returns>The ComplexNumber that this string represents; null if format not correct.</returns>
    public static ComplexNumber Parse(string s)
    {
      const string dbPat = @"([0-9]+)|([0-9]*\.[0-9]+)";
      Match mt = Regex.Match(s, @"^\s*(?<REAL>" + dbPat + @")\s*\+\s*(?<IM>" + dbPat + @")i\s*$");
      if (mt != null && mt.Length > 0)
      {
        double real = double.Parse(mt.Groups["REAL"].Value);
        double imaginary = double.Parse(mt.Groups["IM"].Value);
        return new ComplexNumber(real, imaginary);
      }
      return null;
    }

    #endregion
  }
}
