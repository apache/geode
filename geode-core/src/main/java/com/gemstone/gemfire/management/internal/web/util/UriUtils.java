/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.gemstone.gemfire.management.internal.web.util;

import java.io.UnsupportedEncodingException;
import java.net.URLDecoder;
import java.net.URLEncoder;
import java.util.Map;

import com.gemstone.gemfire.internal.lang.StringUtils;

/**
 * The UriUtils is a utility class for processing URIs and URLs.
 * <p/>
 * @see java.net.URLDecoder
 * @see java.net.URLEncoder
 * @since GemFire 8.0
 */
@SuppressWarnings("unused")
public abstract class UriUtils {

  public static final String DEFAULT_ENCODING = StringUtils.UTF_8;

  /**
   * Decodes the encoded String value using the default encoding, UTF-8.  It is assumed the String value was encoded
   * with the URLEncoder using the UTF-8 encoding.  This method handles UnsupportedEncodingException by just returning
   * the encodedValue.  Since it is possible for a String value to have been encoded multiple times, the String value
   * is decoded until the value stops changing (in other words, until the value is completely decoded).
   * <p/>
   * @param encodedValue the encoded String value encoded to decode.
   * @return the decoded value of the String.  If UTF-8 is unsupported, then the encodedValue is returned.
   * @see #decode(String, String)
   * @see java.net.URLDecoder
   */
  public static String decode(final String encodedValue) {
    return decode(encodedValue, DEFAULT_ENCODING);
  }

  /**
   * Decodes the encoded String value using the specified encoding (such as UTF-8).  It is assumed the String value
   * was encoded with the URLEncoder using the specified encoding.  This method handles UnsupportedEncodingException
   * by just returning the encodedValue.  Since it is possible for a String value to have been encoded multiple times,
   * the String value is decoded until the value stops changing (in other words, until the value is completely decoded).
   * <p/>
   * @param encodedValue the encoded String value to decode.
   * @param encoding a String value specifying the encoding.
   * @return the decoded value of the String.  If the encoding is unsupported, then the encodedValue is returned.
   * @see java.net.URLDecoder
   */
  public static String decode(String encodedValue, final String encoding) {
    try {
      if (encodedValue != null) {
        String previousEncodedValue;

        do {
          previousEncodedValue = encodedValue;
          encodedValue = URLDecoder.decode(encodedValue, encoding);
        }
        while (!encodedValue.equals(previousEncodedValue));
      }

      return encodedValue;
    }
    catch (UnsupportedEncodingException ignore) {
      return encodedValue;
    }
  }

  /**
   * Decodes the array of String values using the default encoding, UTF-8.  Each String value in the array is decoded
   * individually and placed back in the array at the exact same index.  Each String value is decoded until the value
   * stops changing (in other words, until the value is completely decoded).
   * <p/>
   * @param encodedValues the array of encoded String values to decode.
   * @return the same String array with each String value decoded using the default encoding of UTF-8.
   * @see #decode(String[], String)
   * @see java.net.URLDecoder
   */
  public static String[] decode(final String[] encodedValues) {
    return decode(encodedValues, DEFAULT_ENCODING);
  }

  /**
   * Decodes the array of String values using the specified encoding (e.g. UTF-8).  Each String value in the array
   * is decoded individually and placed back in the array at the exact same index.  Each String value is decoded until
   * the value stops changing (in other words, until the value is completely decoded).
   * <p/>
   * @param encodedValues the array of encoded String values to decode.
   * @return the same String array with each String value decoded using the specified encoding (e.g. UTF-8).
   * @see #decode(String, String)
   * @see java.net.URLDecoder
   */
  public static String[] decode(final String[] encodedValues, final String encoding) {
    if (encodedValues != null) {
      for (int index = 0; index < encodedValues.length; index++) {
        encodedValues[index] = decode(encodedValues[index], encoding);
      }
    }

    return encodedValues;
  }

  /**
   * Decodes the encoded String values in the Map using the default encoding, UTF-8.  The Map is structurally similar
   * to a key/value data store but with the contents from an HTML form.  Keys with String values are searched
   * and decoded, which then get repaired with the same Key.  The String value is decoded until the value stops changing
   * (in other words, until the value is completely decoded).
   * <p/>
   * @param form a mapping of key/value pairs containing possible encoded String values.
   * @return the original Map (form) with keys re-paired with decoded String values.
   * @see #decode(java.util.Map, String)
   * @see java.net.URLDecoder
   */
  public static Map<String, Object> decode(final Map<String, Object> form) {
    return decode(form, DEFAULT_ENCODING);
  }

  /**
   * Decodes the encoded String values in the Map using the specified encoding (e.g. UTF-8).  The Map is structurally
   * similar to a key/value data store but with the contents from an HTML form.  Keys with String values are searched
   * and decoded, which then get repaired with the same Key.  The String value is decoded until the value stops changing
   * (in other words, until the value is completely decoded).
   * <p/>
   * @param form a mapping of key/value pairs containing possible encoded String values.
   * @return the original Map (form) with keys re-paired with decoded String values.
   * @see #decode(String, String)
   * @see java.net.URLDecoder
   */
  public static Map<String, Object> decode(final Map<String, Object> form, final String encoding) {
    if (form != null) {
      for (final String key : form.keySet()) {
        final Object value = form.get(key);
        if (value instanceof String) {
          form.put(key, decode(value.toString(), encoding));
        }
      }
    }

    return form;
  }

  /**
   * Encode the specified String value using the default encoding, UTF-8.
   * <p/>
   * @param value the String value to encode.
   * @return an encoded value of the String using the default encoding, UTF-8.  If UTF-8 is unsupported,
   * then value is returned.
   * @see #encode(String, String)
   * @see java.net.URLEncoder
   */
  public static String encode(final String value) {
    return encode(value, DEFAULT_ENCODING);
  }

  /**
   * Encode the String value using the specified encoding.
   * <p/>
   * @param value the String value to encode.
   * @param encoding a String value indicating the encoding.
   * @return an encoded value of the String using the specified encoding.  If the encoding is unsupported,
   * then value is returned.
   * @see java.net.URLEncoder
   */
  public static String encode(final String value, final String encoding) {
    try {
      return (value != null ? URLEncoder.encode(value, encoding) : value);
    }
    catch (UnsupportedEncodingException ignore) {
      return value;
    }
  }

  /**
   * Encodes the array of String values using the default encoding, UTF-8.  Each String value in the array is encoded
   * individually and placed back in the array at the exact same index.
   * <p/>
   * @param values the array of encoded String values to encode.
   * @return the same String array with each String value encoded using the default encoding of UTF-8.
   * @see #encode(String[], String)
   * @see java.net.URLEncoder
   */
  public static String[] encode(final String[] values) {
    return encode(values, DEFAULT_ENCODING);
  }

  /**
   * Encodes the array of String values using the specified encoding (e.g. UTF-8).  Each String value in the array
   * is encoded individually and placed back in the array at the exact same index.
   * <p/>
   * @param values the array of encoded String values to encode.
   * @return the same String array with each String value encoded using the default encoding of UTF-8.
   * @see #encode(String, String)
   * @see java.net.URLEncoder
   */
  public static String[] encode(final String[] values, final String encoding) {
    if (values != null) {
      for (int index = 0; index < values.length; index++) {
        values[index] = encode(values[index], encoding);
      }
    }

    return values;
  }

  /**
   * Encodes the String values in the Map using the default encoding, UTF-8.  The Map is structurally similar
   * to a key/value data store but with the contents from an HTML form.  Keys with String values are searched
   * and encoded, which then get repaired with the same Key.
   * <p/>
   * @param form a mapping of key/value pairs containing String values to encode.
   * @return the original Map (form) with keys re-paired with encoded String values.
   * @see #encode(java.util.Map, String)
   * @see java.net.URLEncoder
   */
  public static Map<String, Object> encode(final Map<String, Object> form) {
    return encode(form, DEFAULT_ENCODING);
  }

  /**
   * Encodes the String values in the Map using the specified encoding (e.g. UTF-8).  The Map is structurally similar
   * to a key/value data store but with the contents from an HTML form.  Keys with String values are searched
   * and encoded, which then get repaired with the same Key.
   * <p/>
   * @param form a mapping of key/value pairs containing String values to encode.
   * @return the original Map (form) with keys re-paired with encoded String values.
   * @see #encode(String, String)
   * @see java.net.URLEncoder
   */
  public static Map<String, Object> encode(final Map<String, Object> form, final String encoding) {
    if (form != null) {
      for (final String key : form.keySet()) {
        final Object value = form.get(key);
        if (value instanceof String) {
          form.put(key, encode(value.toString(), encoding));
        }
      }
    }

    return form;
  }

}
