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
package com.gemstone.gemfire.management.internal.web.domain;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeMap;
import java.util.TreeSet;
import javax.xml.bind.annotation.XmlElement;
import javax.xml.bind.annotation.XmlRootElement;

import com.gemstone.gemfire.internal.lang.StringUtils;

/**
 * The LinkIndex class is abstraction for modeling an index of Links.
 * <p/>
 * @see javax.xml.bind.annotation.XmlRootElement
 * @see com.gemstone.gemfire.management.internal.web.domain.Link
 * @since GemFire 8.0
 */
@SuppressWarnings("unused")
@XmlRootElement(name = "link-index")
public class LinkIndex implements Iterable<Link> {

  @XmlElement(name = "link")
  private final Set<Link> links = new TreeSet<Link>();

  public LinkIndex add(final Link link) {
    assert link != null : "The Link to add to the index cannot be null!";
    links.add(link);
    return this;
  }

  public LinkIndex addAll(final Link... links) {
    assert links != null : "The array of Links to add to this index cannot be null!";
    return addAll(Arrays.asList(links));
  }

  public LinkIndex addAll(final Iterable<Link> links) {
    assert links != null : "The Iterable collection of Links to add to this index cannot be null!";
    for (final Link link : links) {
      add(link);
    }
    return this;
  }

  public Link find(final String relation) {
    Link linkFound = null;

    for (Link link : this) {
      if (link.getRelation().equalsIgnoreCase(relation)) {
        linkFound = link;
        break;
      }
    }

    return linkFound;
  }

  public Link[] findAll(final String relation) {
    final List<Link> links = new ArrayList<Link>();

    for (final Link link : this) {
      if (link.getRelation().equalsIgnoreCase(relation)) {
        links.add(link);
      }
    }

    return links.toArray(new Link[links.size()]);
  }

  public boolean isEmpty() {
    return links.isEmpty();
  }

  @Override
  public Iterator<Link> iterator() {
    return Collections.unmodifiableSet(links).iterator();
  }

  public int size() {
    return links.size();
  }

  public List<Link> toList() {
    return new ArrayList<Link>(links);
  }

  public Map<String, List<Link>> toMap() {
    final Map<String, List<Link>> links = new TreeMap<String, List<Link>>();

    for (final Link link : this) {
      List<Link> linksByRelation = links.get(link.getRelation());

      if (linksByRelation == null) {
        linksByRelation = new ArrayList<Link>(size());
        links.put(link.getRelation(), linksByRelation);
      }

      linksByRelation.add(link);
    }

    return links;
  }

  @Override
  public String toString() {
    final StringBuilder buffer = new StringBuilder("[");
    int count = 0;

    for (final Link link : this) {
      buffer.append(count++ > 0 ? ", " : StringUtils.EMPTY_STRING).append(link);
    }

    buffer.append("]");

    return buffer.toString();
  }

}
