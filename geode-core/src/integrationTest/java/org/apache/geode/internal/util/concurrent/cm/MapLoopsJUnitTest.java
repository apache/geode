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
/*
 * @test
 *
 * @synopsis Exercise multithreaded maps, by default ConcurrentHashMap. Each thread does a random
 * walk though elements of "key" array. On each iteration, it checks if table includes key. If
 * absent, with probablility pinsert it inserts it, and if present, with probablility premove it
 * removes it. (pinsert and premove are expressed as percentages to simplify parsing from command
 * line.)
 */
/*
 * Written by Doug Lea with assistance from members of JCP JSR-166 Expert Group and released to the
 * public domain. Use, modify, and redistribute this code in any way without acknowledgement.
 */
package org.apache.geode.internal.util.concurrent.cm;

import java.util.Map;
import java.util.Random;
import java.util.concurrent.CyclicBarrier;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import org.junit.Test;

import org.apache.geode.util.JSR166TestCase;

public class MapLoopsJUnitTest extends JSR166TestCase { // TODO: reformat

  static int nkeys = 1000;
  static int pinsert = 60;
  static int premove = 2;
  static int maxThreads = 100;
  static int nops = 1000000;
  static int removesPerMaxRandom;
  static int insertsPerMaxRandom;

  static final ExecutorService pool = Executors.newCachedThreadPool();

  @Test
  public void testMapLoops() throws Exception {
    main(new String[0]);
  }

  public static void main(String[] args) throws Exception {

    Class mapClass = null;
    if (args.length > 0) {
      try {
        mapClass = Class.forName(args[0]);
      } catch (ClassNotFoundException e) {
        throw new RuntimeException("Class " + args[0] + " not found.");
      }
    } else {
      mapClass = MAP_CLASS;
    }

    if (args.length > 1) {
      maxThreads = Integer.parseInt(args[1]);
    }

    if (args.length > 2) {
      nkeys = Integer.parseInt(args[2]);
    }

    if (args.length > 3) {
      pinsert = Integer.parseInt(args[3]);
    }

    if (args.length > 4) {
      premove = Integer.parseInt(args[4]);
    }

    if (args.length > 5) {
      nops = Integer.parseInt(args[5]);
    }

    // normalize probabilities wrt random number generator
    removesPerMaxRandom = (int) (((double) premove / 100.0 * 0x7FFFFFFFL));
    insertsPerMaxRandom = (int) (((double) pinsert / 100.0 * 0x7FFFFFFFL));

    System.out.print("Class: " + mapClass.getName());
    System.out.print(" threads: " + maxThreads);
    System.out.print(" size: " + nkeys);
    System.out.print(" ins: " + pinsert);
    System.out.print(" rem: " + premove);
    System.out.print(" ops: " + nops);
    System.out.println();

    int k = 1;
    int warmups = 2;
    for (int i = 1; i <= maxThreads;) {
      Thread.sleep(100);
      test(i, nkeys, mapClass);
      if (warmups > 0) {
        --warmups;
      } else if (i == k) {
        k = i << 1;
        i = i + (i >>> 1);
      } else if (i == 1 && k == 2) {
        i = k;
        warmups = 1;
      } else {
        i = k;
      }
    }
    pool.shutdown();
  }

  static Integer[] makeKeys(int n) {
    LoopHelpers.SimpleRandom rng = new LoopHelpers.SimpleRandom();
    Integer[] key = new Integer[n];
    for (int i = 0; i < key.length; ++i) {
      key[i] = rng.next();
    }
    return key;
  }

  static void shuffleKeys(Integer[] key) {
    Random rng = new Random();
    for (int i = key.length; i > 1; --i) {
      int j = rng.nextInt(i);
      Integer tmp = key[j];
      key[j] = key[i - 1];
      key[i - 1] = tmp;
    }
  }

  static void test(int i, int nkeys, Class mapClass) throws Exception {
    System.out.print("Threads: " + i + "\t:");
    Map map = (Map) mapClass.newInstance();
    Integer[] key = makeKeys(nkeys);
    // Uncomment to start with a non-empty table
    // for (int j = 0; j < nkeys; j += 4) // start 1/4 occupied
    // map.put(key[j], key[j]);
    shuffleKeys(key);
    LoopHelpers.BarrierTimer timer = new LoopHelpers.BarrierTimer();
    CyclicBarrier barrier = new CyclicBarrier(i + 1, timer);
    for (int t = 0; t < i; ++t) {
      pool.execute(new Runner(t, map, key, barrier));
    }
    barrier.await();
    barrier.await();
    long time = timer.getTime();
    long tpo = time / (i * (long) nops);
    System.out.print(LoopHelpers.rightJustify(tpo) + " ns per op");
    double secs = (double) (time) / 1000000000.0;
    System.out.println("\t " + secs + "s run time");
    map.clear();
  }

  static class Runner implements Runnable {
    final Map map;
    final Integer[] key;
    final LoopHelpers.SimpleRandom rng;
    final CyclicBarrier barrier;
    int position;
    int total;

    Runner(int id, Map map, Integer[] key, CyclicBarrier barrier) {
      this.map = map;
      this.key = key;
      this.barrier = barrier;
      position = key.length / 2;
      rng = new LoopHelpers.SimpleRandom((id + 1) * 8862213513L);
      rng.next();
    }

    int step() {
      // random-walk around key positions, bunching accesses
      int r = rng.next();
      position += (r & 7) - 3;
      while (position >= key.length) {
        position -= key.length;
      }
      while (position < 0) {
        position += key.length;
      }

      Integer k = key[position];
      Integer x = (Integer) map.get(k);

      if (x != null) {
        if (x.intValue() != k.intValue()) {
          throw new Error("bad mapping: " + x + " to " + k);
        }

        if (r < removesPerMaxRandom) {
          if (map.remove(k) != null) {
            position = total % key.length; // move from position
            return 2;
          }
        }
      } else if (r < insertsPerMaxRandom) {
        ++position;
        map.put(k, k);
        return 2;
      }

      // Uncomment to add a little computation between accesses
      // total += LoopHelpers.compute1(k.intValue());
      total += r;
      return 1;
    }

    @Override
    public void run() {
      try {
        barrier.await();
        int ops = nops;
        while (ops > 0) {
          ops -= step();
        }
        barrier.await();
      } catch (Exception ex) {
        ex.printStackTrace();
      }
    }
  }
}
