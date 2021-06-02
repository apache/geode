/*
 * Licensed to the Apache Software Foundation (ASF) under one or more contributor license
 * agreements. See the NOTICE file distributed with this work for additional information regarding
 * copyright ownership. The ASF licenses this file to You under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance with the License. You may obtain a
 * copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software distributed under the License
 * is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express
 * or implied. See the License for the specific language governing permissions and limitations under
 * the License.
 */

package org.apache.geode.redis.internal.executor.sortedset;

import java.util.List;

import com.github.davidmoten.geo.LatLong;

import org.apache.geode.cache.Region;
import org.apache.geode.redis.internal.ByteArrayWrapper;
import org.apache.geode.redis.internal.Coder;
import org.apache.geode.redis.internal.GeoCoder;
import org.apache.geode.redis.internal.MemberNotFoundException;
import org.apache.geode.redis.internal.RedisCommandParserException;

public class GeoRadiusParameters {
  final double lon;
  final double lat;
  final double radius;
  final String unit;
  final String member;

  final boolean withDist;
  final boolean withCoord;
  final boolean withHash;
  final Integer count;
  final SortOrder order;

  final Double distScale;
  final String centerHashPrecise;

  public enum CommandType {
    GEORADIUS, GEORADIUSBYMEMBER
  }

  public enum SortOrder {
    ASC, DESC, UNSORTED
  }

  public GeoRadiusParameters(Region<ByteArrayWrapper, ByteArrayWrapper> keyRegion,
      List<byte[]> commandElems, CommandType cmdType) throws IllegalArgumentException,
      RedisCommandParserException, MemberNotFoundException {
    byte[] radArray;

    switch (cmdType) {
      case GEORADIUS:
        byte[] lonArray = commandElems.get(2);
        byte[] latArray = commandElems.get(3);
        radArray = commandElems.get(4);
        unit = new String(commandElems.get(5));
        centerHashPrecise = GeoCoder.geohash(lonArray, latArray);
        lon = Coder.bytesToDouble(lonArray);
        lat = Coder.bytesToDouble(latArray);
        member = null;
        break;
      default:
        byte[] memberArray = commandElems.get(2);
        radArray = commandElems.get(3);
        unit = new String(commandElems.get(4));
        member = new String(memberArray);
        ByteArrayWrapper hashWrapper = keyRegion.get(new ByteArrayWrapper(memberArray));
        if (hashWrapper == null) {
          throw new MemberNotFoundException();
        }

        centerHashPrecise = hashWrapper.toString();
        LatLong pos = GeoCoder.geoPos(centerHashPrecise);
        lon = pos.getLon();
        lat = pos.getLat();
        break;
    }

    distScale = GeoCoder.parseUnitScale(unit);
    radius = Coder.bytesToDouble(radArray) / distScale;

    int i = (cmdType == CommandType.GEORADIUS) ? 6 : 5;

    boolean showDist = false;
    boolean showCoord = false;
    boolean showHash = false;
    for (; i < commandElems.size() && (new String(commandElems.get(i))).contains("with"); i++) {
      String elem = new String(commandElems.get(i));

      if (elem.equals("withdist")) {
        showDist = true;
      }
      if (elem.equals("withcoord")) {
        showCoord = true;
      }
      if (elem.equals("withhash")) {
        showHash = true;
      }
    }
    withDist = showDist;
    withCoord = showCoord;
    withHash = showHash;

    if (i < commandElems.size() && (new String(commandElems.get(i))).equals("count")) {
      count = Coder.bytesToInt(commandElems.get(++i));
      i++;
    } else {
      count = null;
    }

    if (i < commandElems.size() && (new String(commandElems.get(i))).contains("sc")) {
      String elem = new String(commandElems.get(i++));

      if (elem.equals("asc")) {
        order = SortOrder.ASC;
      } else if (elem.equals("desc")) {
        order = SortOrder.DESC;
      } else {
        order = SortOrder.UNSORTED;
      }
    } else {
      order = SortOrder.UNSORTED;
    }

    if (i < commandElems.size()) {
      throw new RedisCommandParserException();
    }
  }
}
