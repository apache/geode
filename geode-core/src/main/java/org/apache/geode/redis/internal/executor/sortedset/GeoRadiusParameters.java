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

import org.apache.geode.cache.Region;
import org.apache.geode.redis.internal.ByteArrayWrapper;
import org.apache.geode.redis.internal.Coder;
import org.apache.geode.redis.internal.CoderException;
import org.apache.geode.redis.internal.GeoCoder;
import org.apache.geode.redis.internal.GeoCoord;
import org.apache.geode.redis.internal.RedisCommandParserException;

public class GeoRadiusParameters {
  double lon;
  double lat;
  double radius;
  String unit;
  String member;

  boolean withDist = false;
  boolean withCoord = false;
  boolean withHash = false;
  Integer count = null;
  Boolean ascendingOrder = null;

  Double distScale;
  char[] centerHashPrecise;

  public enum CommandType {
    GEORADIUS, GEORADIUSBYMEMBER
  }

  public GeoRadiusParameters(Region<ByteArrayWrapper, ByteArrayWrapper> keyRegion,
      List<byte[]> commandElems, CommandType cmdType) throws IllegalArgumentException,
      RedisCommandParserException, CoderException {
    byte[] radArray;
    if (cmdType == CommandType.GEORADIUS) {
      byte[] lonArray = commandElems.get(2);
      byte[] latArray = commandElems.get(3);
      radArray = commandElems.get(4);
      unit = new String(commandElems.get(5));
      centerHashPrecise = GeoCoder.geoHashBits(lonArray, latArray, GeoCoder.LEN_GEOHASH);
      lon = Coder.bytesToDouble(lonArray);
      lat = Coder.bytesToDouble(latArray);
    } else {
      byte[] memberArray = commandElems.get(2);
      radArray = commandElems.get(3);
      unit = new String(commandElems.get(4));
      member = new String(memberArray);
      ByteArrayWrapper hashWrapper = keyRegion.get(new ByteArrayWrapper(memberArray));
      centerHashPrecise = hashWrapper.toString().toCharArray();
      GeoCoord pos = GeoCoder.geoPos(centerHashPrecise);
      lon = pos.getLongitude();
      lat = pos.getLatitude();
    }

    distScale = GeoCoder.parseUnitScale(unit);
    radius = Coder.bytesToDouble(radArray) / distScale;

    int i = (cmdType == CommandType.GEORADIUS) ? 6 : 5;
    for (; i < commandElems.size() && (new String(commandElems.get(i))).contains("with"); i++) {
      String elem = new String(commandElems.get(i));

      if (elem.equals("withdist"))
        withDist = true;
      if (elem.equals("withcoord"))
        withCoord = true;
      if (elem.equals("withhash"))
        withHash = true;
    }

    if (i < commandElems.size() && (new String(commandElems.get(i))).equals("count")) {
      count = Coder.bytesToInt(commandElems.get(++i));
      i++;
    }

    if (i < commandElems.size() && (new String(commandElems.get(i))).contains("sc")) {
      String elem = new String(commandElems.get(i++));

      if (elem.equals("asc"))
        ascendingOrder = true;
      else if (elem.equals("desc"))
        ascendingOrder = false;
    }

    if (i < commandElems.size()) {
      throw new RedisCommandParserException();
    }
  }
}
