/*=========================================================================
 * Copyright (c) 2010-2014 Pivotal Software, Inc. All Rights Reserved.
 * This product is protected by U.S. and international copyright
 * and intellectual property laws. Pivotal products are covered by
 * one or more patents listed at http://www.pivotal.io/patents.
 *=========================================================================
 */
package com.gemstone.gemfire.management.internal.web;

import java.io.UnsupportedEncodingException;
import java.net.URI;
import java.net.URISyntaxException;
import java.net.URLDecoder;
import java.net.URLEncoder;
import java.util.HashMap;
import java.util.Map;

import com.gemstone.gemfire.internal.lang.StringUtils;
import com.gemstone.gemfire.management.internal.web.domain.Link;

/**
 * The AbstractWebDomainTests class is abstract base class containing functionality common to a test suite classes
 * in the com.gemstone.gemfire.management.internal.web.domain package.
 * <p/>
 * @author John Blum
 * @see java.net.URI
 * @see java.net.URLDecoder
 * @see java.net.URLEncoder
 * @see com.gemstone.gemfire.management.internal.web.domain.Link
 * @since 8.0
 */
@SuppressWarnings("unused")
public abstract class AbstractWebTestCase {

  protected <E> E[] createArray(final E... array) {
    return array;
  }

  protected <K, V> Map<K, V> createMap(final K[] keys, final V[] values) {
    assert keys != null : "The Keys for the Map cannot be null!";
    assert values != null : "The Values for the Map cannot be null!";
    assert keys.length == values.length;

    final Map<K, V> map = new HashMap<K, V>(keys.length);
    int index = 0;

    for (final K key : keys) {
      map.put(key, values[index++]);
    }

    return map;
  }

  protected String decode(final String encodedValue) throws UnsupportedEncodingException {
    return URLDecoder.decode(encodedValue, StringUtils.UTF_8);
  }

  protected String encode(final String value) throws UnsupportedEncodingException {
    return URLEncoder.encode(value, StringUtils.UTF_8);
  }

  protected String toString(final Link... links) throws UnsupportedEncodingException {
    final StringBuilder buffer = new StringBuilder("[");
    int count = 0;

    for (final Link link : links) {
      buffer.append(count++ > 0 ? ", " : StringUtils.EMPTY_STRING).append(toString(link));

    }

    buffer.append("]");

    return buffer.toString();
  }

  protected String toString(final Link link) throws UnsupportedEncodingException {
    return link.toHttpRequestLine();
  }

  protected String toString(final URI uri) throws UnsupportedEncodingException {
    return decode(uri.toString());
  }

  protected URI toUri(final String uriString) throws UnsupportedEncodingException, URISyntaxException {
    return new URI(encode(uriString));
  }

}
