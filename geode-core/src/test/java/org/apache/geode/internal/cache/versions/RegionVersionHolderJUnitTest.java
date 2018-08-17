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
package org.apache.geode.internal.cache.versions;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

import java.net.InetAddress;
import java.util.ArrayList;
import java.util.BitSet;
import java.util.List;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import org.apache.geode.distributed.internal.membership.InternalDistributedMember;
import org.apache.geode.internal.Assert;

public class RegionVersionHolderJUnitTest {

  private int originalBitSetWidth;
  private InternalDistributedMember member;

  @Before
  public final void setUp() throws Exception {
    originalBitSetWidth = RegionVersionHolder.BIT_SET_WIDTH;
    member = new InternalDistributedMember(InetAddress.getLocalHost(), 12345);
    postSetUp();
  }

  protected void postSetUp() throws Exception {}

  @After
  public final void tearDown() throws Exception {
    RegionVersionHolder.BIT_SET_WIDTH = originalBitSetWidth;
  }

  protected InternalDistributedMember member() {
    return member;
  }

  @Test
  public void test48066_1() {
    RegionVersionHolder vh1 = new RegionVersionHolder(member);
    for (int i = 1; i <= 3; i++) {
      vh1.recordVersion(i);
    }
    System.out.println("vh1=" + vh1);

    RegionVersionHolder vh2 = vh1.clone();
    System.out.println("after clone, vh2=" + vh2);

    {
      RegionVersionHolder vh3 = new RegionVersionHolder(member);
      for (int i = 1; i <= 10; i++) {
        vh3.recordVersion(i);
      }

      // create special exception 10(3-11), bitsetVerson=3
      vh3.initializeFrom(vh2);
      System.out.println("after init, vh3=" + vh3);
      assertEquals(3, vh3.getVersion());

      // to make bsv3,bs=[0,1]
      vh3.recordVersion(4);
      System.out.println("after record 4, vh3=");
      assertEquals(4, vh3.getVersion());

      vh3.recordVersion(7);
      System.out.println("after record 7, vh3=" + vh3);
      assertEquals(7, vh3.getVersion());
    }
  }

  @Test
  public void test48066() {
    RegionVersionHolder vh = new RegionVersionHolder(member);
    BitSet bs = new BitSet();
    bs.set(0, 8679);
    bs.set(8705, 8713);
    recordVersions(vh, bs);
    System.out.println("init:\t\t" + vh);
    // This is what happens when we apply an RVV from a member that doesn't
    // have an entry for a member that we do have an entry from
    vh.initializeFrom(new RegionVersionHolder(0));
    System.out.println("init from:\t" + vh);
    vh.recordVersion(1);
    vh.recordVersion(2);
    vh.recordVersion(14);
    vh.recordVersion(62);
    vh.recordVersion(95);
    vh.recordVersion(96);
    vh.recordVersion(97);
    vh.recordVersion(98);
    vh.recordVersion(99);
    vh.recordVersion(100);
    vh.recordVersion(123);
    vh.recordVersion(144);
    vh.recordVersion(146);
    vh.recordVersion(147);
    vh.recordVersion(148);
    vh.recordVersion(149);
    vh.recordVersion(150);
    vh.recordVersion(151);
    vh.recordVersion(152);
    vh.recordVersion(153);
    vh.recordVersion(154);
    vh.recordVersion(155);
    vh.recordVersion(156);
    vh.recordVersion(157);
    vh.recordVersion(158);
    vh.recordVersion(159);
    vh.recordVersion(160);
    vh.recordVersion(161);
    vh.recordVersion(184);
    vh.recordVersion(185);
    vh.recordVersion(186);
    vh.recordVersion(187);
    vh.recordVersion(188);
    vh.recordVersion(189);
    vh.recordVersion(190);
    vh.recordVersion(191);
    vh.recordVersion(192);
    vh.recordVersion(193);
    vh.recordVersion(194);
    vh.recordVersion(195);
    vh.recordVersion(197);
    vh.recordVersion(196);
    vh.recordVersion(201);
    vh.recordVersion(202);
    vh.recordVersion(203);
    vh.recordVersion(204);
    vh.recordVersion(205);
    vh.recordVersion(206);
    vh.recordVersion(207);
    vh.recordVersion(208);
    vh.recordVersion(209);
    vh.recordVersion(210);
    vh.recordVersion(211);
    vh.recordVersion(212);
    vh.recordVersion(213);
    vh.recordVersion(214);
    vh.recordVersion(215);
    vh.recordVersion(216);
    vh.recordVersion(217);
    vh.recordVersion(218);
    vh.recordVersion(219);
    vh.recordVersion(220);
    vh.recordVersion(221);
    vh.recordVersion(222);
    vh.recordVersion(224);
    vh.recordVersion(223);
    vh.recordVersion(238);
    vh.recordVersion(261);
    vh.recordVersion(262);
    vh.recordVersion(263);
    vh.recordVersion(264);
    vh.recordVersion(265);
    vh.recordVersion(266);
    vh.recordVersion(267);
    vh.recordVersion(268);
    vh.recordVersion(269);
    vh.recordVersion(270);
    vh.recordVersion(271);
    vh.recordVersion(272);
    vh.recordVersion(285);
    vh.recordVersion(286);
    vh.recordVersion(308);
    vh.recordVersion(309);
    vh.recordVersion(311);
    vh.recordVersion(312);
    vh.recordVersion(360);
    vh.recordVersion(361);
    vh.recordVersion(362);
    vh.recordVersion(363);
    vh.recordVersion(364);
    vh.recordVersion(365);
    vh.recordVersion(390);
    vh.recordVersion(391);
    vh.recordVersion(392);
    vh.recordVersion(393);
    vh.recordVersion(394);
    vh.recordVersion(395);
    vh.recordVersion(396);
    vh.recordVersion(397);
    vh.recordVersion(399);
    vh.recordVersion(398);
    vh.recordVersion(400);
    vh.recordVersion(401);
    vh.recordVersion(402);
    vh.recordVersion(409);
    vh.recordVersion(410);
    vh.recordVersion(412);
    vh.recordVersion(413);
    vh.recordVersion(417);
    vh.recordVersion(418);
    vh.recordVersion(425);
    vh.recordVersion(427);
    vh.recordVersion(426);
    vh.recordVersion(428);
    vh.recordVersion(429);
    vh.recordVersion(431);
    vh.recordVersion(430);
    vh.recordVersion(432);
    vh.recordVersion(433);
    vh.recordVersion(448);
    vh.recordVersion(449);
    vh.recordVersion(456);
    vh.recordVersion(457);
    vh.recordVersion(483);
    vh.recordVersion(484);
    vh.recordVersion(485);
    vh.recordVersion(490);
    vh.recordVersion(491);
    vh.recordVersion(492);
    vh.recordVersion(515);
    vh.recordVersion(516);
    vh.recordVersion(517);
    vh.recordVersion(518);
    vh.recordVersion(546);
    vh.recordVersion(548);
    vh.recordVersion(555);
    vh.recordVersion(556);
    vh.recordVersion(557);
    vh.recordVersion(574);
    vh.recordVersion(575);
    vh.recordVersion(577);
    vh.recordVersion(576);
    vh.recordVersion(578);
    vh.recordVersion(583);
    vh.recordVersion(584);
    vh.recordVersion(585);
    vh.recordVersion(586);
    vh.recordVersion(587);
    vh.recordVersion(613);
    vh.recordVersion(632);
    vh.recordVersion(656);
    vh.recordVersion(657);
    vh.recordVersion(658);
    vh.recordVersion(659);
    vh.recordVersion(660);
    vh.recordVersion(661);
    vh.recordVersion(662);
    vh.recordVersion(684);
    vh.recordVersion(697);
    vh.recordVersion(698);
    vh.recordVersion(699);
    vh.recordVersion(700);
    vh.recordVersion(701);
    vh.recordVersion(717);
    vh.recordVersion(718);
    vh.recordVersion(722);
    vh.recordVersion(723);
    vh.recordVersion(741);
    vh.recordVersion(742);
    vh.recordVersion(743);
    vh.recordVersion(762);
    vh.recordVersion(782);
    vh.recordVersion(783);
    vh.recordVersion(784);
    vh.recordVersion(785);
    vh.recordVersion(802);
    vh.recordVersion(803);
    vh.recordVersion(816);
    vh.recordVersion(837);
    vh.recordVersion(857);
    vh.recordVersion(860);
    vh.recordVersion(877);
    vh.recordVersion(878);
    vh.recordVersion(879);
    vh.recordVersion(881);
    vh.recordVersion(880);
    vh.recordVersion(882);
    vh.recordVersion(883);
    vh.recordVersion(902);
    vh.recordVersion(903);
    vh.recordVersion(904);
    vh.recordVersion(929);
    vh.recordVersion(940);
    vh.recordVersion(941);
    vh.recordVersion(966);
    vh.recordVersion(967);
    vh.recordVersion(968);
    vh.recordVersion(979);
    vh.recordVersion(980);
    vh.recordVersion(981);
    vh.recordVersion(982);
    vh.recordVersion(983);
    vh.recordVersion(1083);
    vh.recordVersion(1107);
    vh.recordVersion(1108);
    vh.recordVersion(1130);
    vh.recordVersion(1147);
    vh.recordVersion(1148);
    vh.recordVersion(1149);
    vh.recordVersion(1150);
    vh.recordVersion(1151);
    vh.recordVersion(1152);
    vh.recordVersion(1156);
    vh.recordVersion(1157);
    vh.recordVersion(1158);
    vh.recordVersion(1167);
    vh.recordVersion(1185);
    vh.recordVersion(1186);
    vh.recordVersion(1187);
    vh.recordVersion(1188);
    vh.recordVersion(1189);
    vh.recordVersion(1190);
    vh.recordVersion(1191);
    vh.recordVersion(1200);
    vh.recordVersion(1201);
    vh.recordVersion(1202);
    vh.recordVersion(1222);
    vh.recordVersion(1258);
    vh.recordVersion(1259);
    vh.recordVersion(1260);
    vh.recordVersion(1261);
    vh.recordVersion(1262);
    vh.recordVersion(1263);
    vh.recordVersion(1289);
    vh.recordVersion(1292);
    vh.recordVersion(1293);
    vh.recordVersion(1294);
    vh.recordVersion(1322);
    vh.recordVersion(1323);
    vh.recordVersion(1324);
    vh.recordVersion(1325);
    vh.recordVersion(1327);
    vh.recordVersion(1326);
    vh.recordVersion(1328);
    vh.recordVersion(1329);
    vh.recordVersion(1330);
    vh.recordVersion(1351);
    vh.recordVersion(1352);
    vh.recordVersion(1353);
    vh.recordVersion(1354);
    vh.recordVersion(1418);
    vh.recordVersion(1433);
    vh.recordVersion(1434);
    vh.recordVersion(1455);
    vh.recordVersion(2428);
    vh.recordVersion(2429);
    vh.recordVersion(2430);
    vh.recordVersion(2431);
    vh.recordVersion(2432);
    vh.recordVersion(2433);
    vh.recordVersion(2434);
    vh.recordVersion(2435);
    vh.recordVersion(2436);
    vh.recordVersion(2437);
    vh.recordVersion(2438);
    vh.recordVersion(2439);
    vh.recordVersion(2440);
    vh.recordVersion(2441);
    vh.recordVersion(2442);
    vh.recordVersion(2443);
    vh.recordVersion(2444);
    vh.recordVersion(2445);
    vh.recordVersion(2446);
    vh.recordVersion(2447);
    vh.recordVersion(2468);
    vh.recordVersion(2521);
    vh.recordVersion(2522);
    vh.recordVersion(2523);
    vh.recordVersion(2524);
    vh.recordVersion(2525);
    vh.recordVersion(2526);
    vh.recordVersion(2527);
    vh.recordVersion(2538);
    vh.recordVersion(2539);
    vh.recordVersion(2540);
    vh.recordVersion(2541);
    vh.recordVersion(2566);
    vh.recordVersion(2592);
    vh.recordVersion(2593);
    vh.recordVersion(2594);
    vh.recordVersion(2595);
    vh.recordVersion(2596);
    vh.recordVersion(2603);
    vh.recordVersion(2604);
    vh.recordVersion(2605);
    vh.recordVersion(2606);
    vh.recordVersion(2607);
    vh.recordVersion(2608);
    vh.recordVersion(2609);
    vh.recordVersion(2610);
    vh.recordVersion(2611);
    vh.recordVersion(2612);
    vh.recordVersion(2613);
    vh.recordVersion(2648);
    vh.recordVersion(2649);
    vh.recordVersion(2650);
    vh.recordVersion(2651);
    vh.recordVersion(2652);
    vh.recordVersion(2653);
    vh.recordVersion(2654);
    vh.recordVersion(2713);
    vh.recordVersion(2733);
    vh.recordVersion(2734);
    vh.recordVersion(2735);
    vh.recordVersion(2736);
    vh.recordVersion(2737);
    vh.recordVersion(2763);
    vh.recordVersion(2785);
    vh.recordVersion(2786);
    vh.recordVersion(2812);
    vh.recordVersion(2813);
    vh.recordVersion(2814);
    vh.recordVersion(2815);
    vh.recordVersion(2831);
    vh.recordVersion(2832);
    vh.recordVersion(2833);
    vh.recordVersion(2834);
    vh.recordVersion(2870);
    vh.recordVersion(2881);
    vh.recordVersion(2882);
    vh.recordVersion(2904);
    vh.recordVersion(2905);
    vh.recordVersion(2906);
    vh.recordVersion(2907);
    vh.recordVersion(2908);
    vh.recordVersion(2912);
    vh.recordVersion(2913);
    vh.recordVersion(2914);
    vh.recordVersion(2915);
    vh.recordVersion(2916);
    vh.recordVersion(2917);
    vh.recordVersion(2918);
    vh.recordVersion(2919);
    vh.recordVersion(2920);
    vh.recordVersion(2921);
    vh.recordVersion(2922);
    vh.recordVersion(2924);
    vh.recordVersion(2925);
    vh.recordVersion(2926);
    vh.recordVersion(2927);
    vh.recordVersion(2928);
    vh.recordVersion(2929);
    vh.recordVersion(2930);
    vh.recordVersion(2931);
    vh.recordVersion(2932);
    vh.recordVersion(2933);
    vh.recordVersion(2934);
    vh.recordVersion(2935);
    vh.recordVersion(2936);
    vh.recordVersion(2937);
    vh.recordVersion(2971);
    vh.recordVersion(2988);
    vh.recordVersion(2989);
    vh.recordVersion(2990);
    vh.recordVersion(2991);
    vh.recordVersion(2992);
    vh.recordVersion(3003);
    vh.recordVersion(3004);
    vh.recordVersion(3005);
    vh.recordVersion(3006);
    vh.recordVersion(3007);
    vh.recordVersion(3008);
    vh.recordVersion(3009);
    vh.recordVersion(3011);
    vh.recordVersion(3052);
    vh.recordVersion(3053);
    vh.recordVersion(3054);
    vh.recordVersion(3055);
    vh.recordVersion(3056);
    vh.recordVersion(3058);
    vh.recordVersion(3071);
    vh.recordVersion(3072);
    vh.recordVersion(3075);
    vh.recordVersion(3076);
    vh.recordVersion(3077);
    vh.recordVersion(3078);
    vh.recordVersion(3079);
    vh.recordVersion(3080);
    vh.recordVersion(3081);
    vh.recordVersion(3094);
    vh.recordVersion(3095);
    vh.recordVersion(3105);
    vh.recordVersion(3106);
    vh.recordVersion(3107);
    vh.recordVersion(3108);
    vh.recordVersion(3109);
    vh.recordVersion(3115);
    vh.recordVersion(3116);
    vh.recordVersion(3141);
    vh.recordVersion(3142);
    vh.recordVersion(3143);
    vh.recordVersion(3144);
    vh.recordVersion(3169);
    vh.recordVersion(3170);
    vh.recordVersion(3203);
    vh.recordVersion(3204);
    vh.recordVersion(3205);
    vh.recordVersion(3206);
    vh.recordVersion(3208);
    vh.recordVersion(3207);
    vh.recordVersion(3212);
    vh.recordVersion(3213);
    vh.recordVersion(3214);
    vh.recordVersion(3215);
    vh.recordVersion(3217);
    vh.recordVersion(3218);
    vh.recordVersion(3219);
    vh.recordVersion(3230);
    vh.recordVersion(3259);
    vh.recordVersion(3260);
    vh.recordVersion(3281);
    vh.recordVersion(3282);
    vh.recordVersion(3283);
    vh.recordVersion(3284);
    vh.recordVersion(3285);
    vh.recordVersion(3286);
    vh.recordVersion(3287);
    vh.recordVersion(3288);
    vh.recordVersion(3289);
    vh.recordVersion(3290);
    vh.recordVersion(3296);
    vh.recordVersion(3297);
    vh.recordVersion(3298);
    vh.recordVersion(3302);
    vh.recordVersion(3324);
    vh.recordVersion(3370);
    vh.recordVersion(3371);
    vh.recordVersion(3372);
    vh.recordVersion(3398);
    vh.recordVersion(3399);
    vh.recordVersion(3419);
    vh.recordVersion(3420);
    vh.recordVersion(3422);
    vh.recordVersion(3423);
    vh.recordVersion(3424);
    vh.recordVersion(3425);
    vh.recordVersion(3426);
    vh.recordVersion(3427);
    vh.recordVersion(3428);
    vh.recordVersion(3429);
    vh.recordVersion(3445);
    vh.recordVersion(3446);
    vh.recordVersion(3447);
    vh.recordVersion(3448);
    vh.recordVersion(3464);
    vh.recordVersion(3465);
    vh.recordVersion(3466);
    vh.recordVersion(3467);
    vh.recordVersion(3468);
    vh.recordVersion(3469);
    vh.recordVersion(3470);
    vh.recordVersion(3471);
    vh.recordVersion(3472);
    vh.recordVersion(3473);
    vh.recordVersion(3478);
    vh.recordVersion(3500);
    vh.recordVersion(3501);
    vh.recordVersion(3537);
    vh.recordVersion(3538);
    vh.recordVersion(3559);
    vh.recordVersion(3566);
    vh.recordVersion(3594);
    vh.recordVersion(3595);
    vh.recordVersion(3596);
    vh.recordVersion(3597);
    vh.recordVersion(3662);
    vh.recordVersion(3663);
    vh.recordVersion(3664);
    vh.recordVersion(3665);
    vh.recordVersion(3666);
    vh.recordVersion(3667);
    vh.recordVersion(3668);
    vh.recordVersion(3669);
    vh.recordVersion(3670);
    vh.recordVersion(3673);
    vh.recordVersion(3674);
    vh.recordVersion(3691);
    vh.recordVersion(3692);
    vh.recordVersion(3693);
    vh.recordVersion(3694);
    vh.recordVersion(3720);
    vh.recordVersion(3721);
    vh.recordVersion(3722);
    vh.recordVersion(3723);
    vh.recordVersion(3724);
    vh.recordVersion(3725);
    vh.recordVersion(3757);
    vh.recordVersion(3765);
    vh.recordVersion(3766);
    vh.recordVersion(3767);
    vh.recordVersion(3768);
    vh.recordVersion(3769);
    vh.recordVersion(3770);
    vh.recordVersion(3771);
    vh.recordVersion(3772);
    vh.recordVersion(3773);
    vh.recordVersion(3774);
    vh.recordVersion(3775);
    vh.recordVersion(3776);
    vh.recordVersion(3777);
    vh.recordVersion(3778);
    vh.recordVersion(3779);
    vh.recordVersion(3796);
    vh.recordVersion(3797);
    vh.recordVersion(3798);
    vh.recordVersion(3799);
    vh.recordVersion(3800);
    vh.recordVersion(3821);
    vh.recordVersion(3822);
    vh.recordVersion(3823);
    vh.recordVersion(3840);
    vh.recordVersion(3863);
    vh.recordVersion(3864);
    vh.recordVersion(3865);
    vh.recordVersion(3867);
    vh.recordVersion(3866);
    vh.recordVersion(3890);
    vh.recordVersion(3891);
    vh.recordVersion(3892);
    vh.recordVersion(3893);
    vh.recordVersion(3894);
    vh.recordVersion(3895);
    vh.recordVersion(3915);
    vh.recordVersion(3916);
    vh.recordVersion(3917);
    vh.recordVersion(3918);
    vh.recordVersion(3919);
    vh.recordVersion(3920);
    vh.recordVersion(3929);
    vh.recordVersion(3948);
    vh.recordVersion(3955);
    vh.recordVersion(3975);
    vh.recordVersion(3976);
    vh.recordVersion(3984);
    vh.recordVersion(3985);
    vh.recordVersion(3986);
    vh.recordVersion(3987);
    vh.recordVersion(3990);
    vh.recordVersion(4007);
    vh.recordVersion(4008);
    vh.recordVersion(4009);
    vh.recordVersion(4010);
    vh.recordVersion(4025);
    vh.recordVersion(4026);
    vh.recordVersion(4027);
    vh.recordVersion(4047);
    vh.recordVersion(4067);
    vh.recordVersion(4068);
    vh.recordVersion(4088);
    vh.recordVersion(4104);
    vh.recordVersion(4105);
    vh.recordVersion(4106);
    vh.recordVersion(4114);
    vh.recordVersion(4126);
    vh.recordVersion(4167);
    vh.recordVersion(4175);
    vh.recordVersion(4176);
    vh.recordVersion(4177);
    vh.recordVersion(4179);
    vh.recordVersion(4178);
    vh.recordVersion(4180);
    vh.recordVersion(4187);
    vh.recordVersion(4188);
    vh.recordVersion(4189);
    vh.recordVersion(4190);
    vh.recordVersion(4191);
    vh.recordVersion(4192);
    vh.recordVersion(4193);
    vh.recordVersion(4194);
    vh.recordVersion(4195);
    vh.recordVersion(4196);
    vh.recordVersion(4197);
    vh.recordVersion(4198);
    vh.recordVersion(4199);
    vh.recordVersion(4200);
    vh.recordVersion(4201);
    vh.recordVersion(4202);
    vh.recordVersion(4208);
    vh.recordVersion(4209);
    vh.recordVersion(4275);
    vh.recordVersion(4276);
    vh.recordVersion(4277);
    vh.recordVersion(4278);
    vh.recordVersion(4279);
    vh.recordVersion(4280);
    vh.recordVersion(4281);
    vh.recordVersion(4282);
    vh.recordVersion(4283);
    vh.recordVersion(4284);
    vh.recordVersion(4303);
    vh.recordVersion(4304);
    vh.recordVersion(4305);
    vh.recordVersion(4306);
    vh.recordVersion(4320);
    vh.recordVersion(4321);
    vh.recordVersion(4322);
    vh.recordVersion(4323);
    vh.recordVersion(4324);
    vh.recordVersion(4325);
    vh.recordVersion(4326);
    vh.recordVersion(4327);
    vh.recordVersion(4328);
    vh.recordVersion(4352);
    vh.recordVersion(4353);
    vh.recordVersion(4354);
    vh.recordVersion(4355);
    vh.recordVersion(4356);
    vh.recordVersion(4374);
    vh.recordVersion(4375);
    vh.recordVersion(4376);
    vh.recordVersion(4377);
    vh.recordVersion(4378);
    vh.recordVersion(4379);
    vh.recordVersion(4380);
    vh.recordVersion(4381);
    vh.recordVersion(4382);
    vh.recordVersion(4383);
    vh.recordVersion(4384);
    vh.recordVersion(4385);
    vh.recordVersion(4386);
    vh.recordVersion(4387);
    vh.recordVersion(4388);
    vh.recordVersion(4429);
    vh.recordVersion(4430);
    vh.recordVersion(4431);
    vh.recordVersion(4432);
    vh.recordVersion(4433);
    vh.recordVersion(4434);
    vh.recordVersion(4435);
    vh.recordVersion(4436);
    vh.recordVersion(4438);
    vh.recordVersion(4439);
    vh.recordVersion(4440);
    vh.recordVersion(4441);
    vh.recordVersion(4442);
    vh.recordVersion(4443);
    vh.recordVersion(4444);
    vh.recordVersion(4445);
    vh.recordVersion(4446);
    vh.recordVersion(4447);
    vh.recordVersion(4448);
    vh.recordVersion(4449);
    vh.recordVersion(4468);
    vh.recordVersion(4471);
    vh.recordVersion(4472);
    vh.recordVersion(4483);
    vh.recordVersion(4484);
    vh.recordVersion(4485);
    vh.recordVersion(4486);
    vh.recordVersion(4487);
    vh.recordVersion(4489);
    vh.recordVersion(4488);
    vh.recordVersion(4490);
    vh.recordVersion(4491);
    vh.recordVersion(4492);
    vh.recordVersion(4493);
    vh.recordVersion(4501);
    vh.recordVersion(4540);
    vh.recordVersion(4541);
    vh.recordVersion(4542);
    vh.recordVersion(4543);
    vh.recordVersion(4544);
    vh.recordVersion(4545);
    vh.recordVersion(4546);
    vh.recordVersion(4547);
    vh.recordVersion(4567);
    vh.recordVersion(4568);
    vh.recordVersion(4569);
    vh.recordVersion(4570);
    vh.recordVersion(4571);
    vh.recordVersion(4572);
    vh.recordVersion(4573);
    vh.recordVersion(4574);
    vh.recordVersion(4575);
    vh.recordVersion(4600);
    vh.recordVersion(4601);
    vh.recordVersion(4602);
    vh.recordVersion(4603);
    vh.recordVersion(4616);
    vh.recordVersion(4617);
    vh.recordVersion(4620);
    vh.recordVersion(4621);
    vh.recordVersion(4622);
    vh.recordVersion(4623);
    vh.recordVersion(4624);
    vh.recordVersion(4625);
    vh.recordVersion(4626);
    vh.recordVersion(4627);
    vh.recordVersion(4629);
    vh.recordVersion(4630);
    vh.recordVersion(4631);
    vh.recordVersion(4632);
    vh.recordVersion(4633);
    vh.recordVersion(4634);
    vh.recordVersion(4658);
    vh.recordVersion(4659);
    vh.recordVersion(4678);
    vh.recordVersion(4691);
    vh.recordVersion(4711);
    vh.recordVersion(4737);
    vh.recordVersion(4738);
    vh.recordVersion(4739);
    vh.recordVersion(4740);
    vh.recordVersion(4745);
    vh.recordVersion(4746);
    vh.recordVersion(4747);
    vh.recordVersion(4748);
    vh.recordVersion(4761);
    vh.recordVersion(4762);
    vh.recordVersion(4763);
    vh.recordVersion(4779);
    vh.recordVersion(4780);
    vh.recordVersion(4781);
    vh.recordVersion(4782);
    vh.recordVersion(4807);
    vh.recordVersion(4808);
    vh.recordVersion(4809);
    vh.recordVersion(4810);
    vh.recordVersion(4811);
    vh.recordVersion(4812);
    vh.recordVersion(4813);
    vh.recordVersion(4814);
    vh.recordVersion(4840);
    vh.recordVersion(4859);
    vh.recordVersion(4877);
    vh.recordVersion(4878);
    vh.recordVersion(4879);
    vh.recordVersion(4905);
    vh.recordVersion(4906);
    vh.recordVersion(4910);
    vh.recordVersion(4911);
    vh.recordVersion(4948);
    vh.recordVersion(4949);
    vh.recordVersion(4950);
    vh.recordVersion(4951);
    vh.recordVersion(4952);
    vh.recordVersion(4953);
    vh.recordVersion(4954);
    vh.recordVersion(4955);
    vh.recordVersion(4956);
    vh.recordVersion(4964);
    vh.recordVersion(4976);
    vh.recordVersion(4977);
    vh.recordVersion(4978);
    vh.recordVersion(4979);
    vh.recordVersion(4980);
    vh.recordVersion(5016);
    vh.recordVersion(5026);
    vh.recordVersion(5027);
    vh.recordVersion(5028);
    vh.recordVersion(5029);
    vh.recordVersion(5030);
    vh.recordVersion(5052);
    vh.recordVersion(5053);
    vh.recordVersion(5071);
    vh.recordVersion(5072);
    vh.recordVersion(5094);
    vh.recordVersion(5095);
    vh.recordVersion(5096);
    vh.recordVersion(5097);
    vh.recordVersion(5098);
    vh.recordVersion(5099);
    vh.recordVersion(5100);
    vh.recordVersion(5101);
    vh.recordVersion(5102);
    vh.recordVersion(5103);
    vh.recordVersion(5104);
    vh.recordVersion(5105);
    vh.recordVersion(5106);
    vh.recordVersion(5107);
    vh.recordVersion(5108);
    vh.recordVersion(5109);
    vh.recordVersion(5110);
    vh.recordVersion(5112);
    vh.recordVersion(5138);
    vh.recordVersion(5139);
    vh.recordVersion(5140);
    vh.recordVersion(5141);
    vh.recordVersion(5142);
    vh.recordVersion(5143);
    vh.recordVersion(5144);
    vh.recordVersion(5145);
    vh.recordVersion(5146);
    vh.recordVersion(5147);
    vh.recordVersion(5148);
    vh.recordVersion(5149);
    vh.recordVersion(5150);
    vh.recordVersion(5151);
    vh.recordVersion(5152);
    vh.recordVersion(5163);
    vh.recordVersion(5164);
    vh.recordVersion(5165);
    vh.recordVersion(5166);
    vh.recordVersion(5167);
    vh.recordVersion(5168);
    vh.recordVersion(5169);
    vh.recordVersion(5170);
    vh.recordVersion(5171);
    vh.recordVersion(5172);
    vh.recordVersion(5173);
    vh.recordVersion(5174);
    vh.recordVersion(5175);
    vh.recordVersion(5176);
    vh.recordVersion(5184);
    vh.recordVersion(5185);
    vh.recordVersion(5186);
    vh.recordVersion(5187);
    vh.recordVersion(5205);
    vh.recordVersion(5206);
    vh.recordVersion(5207);
    vh.recordVersion(5208);
    vh.recordVersion(5209);
    vh.recordVersion(5210);
    vh.recordVersion(5211);
    vh.recordVersion(5212);
    vh.recordVersion(5213);
    vh.recordVersion(5214);
    vh.recordVersion(5215);
    vh.recordVersion(5216);
    vh.recordVersion(5217);
    vh.recordVersion(5218);
    vh.recordVersion(5219);
    vh.recordVersion(5221);
    vh.recordVersion(5220);
    vh.recordVersion(5222);
    vh.recordVersion(5224);
    vh.recordVersion(5223);
    vh.recordVersion(5225);
    vh.recordVersion(5226);
    vh.recordVersion(5227);
    vh.recordVersion(5228);
    vh.recordVersion(5229);
    vh.recordVersion(5252);
    vh.recordVersion(5269);
    vh.recordVersion(5270);
    vh.recordVersion(5271);
    vh.recordVersion(5272);
    vh.recordVersion(5273);
    vh.recordVersion(5314);
    vh.recordVersion(5315);
    vh.recordVersion(5316);
    vh.recordVersion(5317);
    vh.recordVersion(5336);
    vh.recordVersion(5337);
    vh.recordVersion(5345);
    vh.recordVersion(5346);
    vh.recordVersion(5347);
    vh.recordVersion(5348);
    vh.recordVersion(5354);
    vh.recordVersion(5363);
    vh.recordVersion(5364);
    vh.recordVersion(5365);
    vh.recordVersion(5366);
    vh.recordVersion(5367);
    vh.recordVersion(5369);
    vh.recordVersion(5368);
    vh.recordVersion(5370);
    vh.recordVersion(5371);
    vh.recordVersion(5372);
    vh.recordVersion(5380);
    vh.recordVersion(5381);
    vh.recordVersion(5382);
    vh.recordVersion(5383);
    vh.recordVersion(5384);
    vh.recordVersion(5385);
    vh.recordVersion(5386);
    vh.recordVersion(5387);
    vh.recordVersion(5406);
    vh.recordVersion(5407);
    vh.recordVersion(5408);
    vh.recordVersion(5410);
    vh.recordVersion(5409);
    vh.recordVersion(5411);
    vh.recordVersion(5427);
    vh.recordVersion(5428);
    vh.recordVersion(5429);
    vh.recordVersion(5430);
    vh.recordVersion(5431);
    vh.recordVersion(5449);
    vh.recordVersion(5450);
    vh.recordVersion(5451);
    vh.recordVersion(5452);
    vh.recordVersion(5453);
    vh.recordVersion(5454);
    vh.recordVersion(5475);
    vh.recordVersion(5476);
    vh.recordVersion(5477);
    vh.recordVersion(5478);
    vh.recordVersion(5524);
    vh.recordVersion(5531);
    vh.recordVersion(5532);
    vh.recordVersion(5533);
    vh.recordVersion(5534);
    vh.recordVersion(5535);
    vh.recordVersion(5536);
    vh.recordVersion(5537);
    vh.recordVersion(5538);
    vh.recordVersion(5539);
    vh.recordVersion(5580);
    vh.recordVersion(5581);
    vh.recordVersion(5587);
    vh.recordVersion(5588);
    vh.recordVersion(5589);
    vh.recordVersion(5590);
    vh.recordVersion(5591);
    vh.recordVersion(5592);
    vh.recordVersion(5593);
    vh.recordVersion(5594);
    vh.recordVersion(5595);
    vh.recordVersion(5596);
    vh.recordVersion(5597);
    vh.recordVersion(5598);
    vh.recordVersion(5599);
    vh.recordVersion(5600);
    vh.recordVersion(5601);
    vh.recordVersion(5602);
    vh.recordVersion(5628);
    vh.recordVersion(5629);
    vh.recordVersion(5652);
    vh.recordVersion(5653);
    vh.recordVersion(5654);
    vh.recordVersion(5655);
    vh.recordVersion(5656);
    vh.recordVersion(5668);
    vh.recordVersion(5669);
    vh.recordVersion(5698);
    vh.recordVersion(5699);
    vh.recordVersion(5700);
    vh.recordVersion(5701);
    vh.recordVersion(5702);
    vh.recordVersion(5711);
    vh.recordVersion(5712);
    vh.recordVersion(5713);
    vh.recordVersion(5714);
    vh.recordVersion(5715);
    vh.recordVersion(5716);
    vh.recordVersion(5717);
    vh.recordVersion(5718);
    vh.recordVersion(5719);
    vh.recordVersion(5737);
    vh.recordVersion(5763);
    vh.recordVersion(5786);
    vh.recordVersion(5787);
    vh.recordVersion(5788);
    vh.recordVersion(5807);
    vh.recordVersion(5808);
    vh.recordVersion(5831);
    vh.recordVersion(5832);
    vh.recordVersion(5856);
    vh.recordVersion(5857);
    vh.recordVersion(5864);
    vh.recordVersion(5865);
    vh.recordVersion(5866);
    vh.recordVersion(5867);
    vh.recordVersion(5868);
    vh.recordVersion(5869);
    vh.recordVersion(5870);
    vh.recordVersion(5898);
    vh.recordVersion(5899);
    vh.recordVersion(5900);
    vh.recordVersion(5901);
    vh.recordVersion(5902);
    vh.recordVersion(5903);
    vh.recordVersion(5908);
    vh.recordVersion(5923);
    vh.recordVersion(5924);
    vh.recordVersion(5925);
    vh.recordVersion(5926);
    vh.recordVersion(5927);
    vh.recordVersion(5928);
    vh.recordVersion(5929);
    vh.recordVersion(5930);
    vh.recordVersion(5931);
    vh.recordVersion(5932);
    vh.recordVersion(5933);
    vh.recordVersion(5940);
    vh.recordVersion(5941);
    vh.recordVersion(5942);
    vh.recordVersion(5943);
    vh.recordVersion(5944);
    vh.recordVersion(5945);
    vh.recordVersion(5968);
    vh.recordVersion(5969);
    vh.recordVersion(5970);
    vh.recordVersion(5971);
    vh.recordVersion(5972);
    vh.recordVersion(5973);
    vh.recordVersion(5974);
    vh.recordVersion(5975);
    vh.recordVersion(6032);
    vh.recordVersion(6048);
    vh.recordVersion(6049);
    vh.recordVersion(6050);
    vh.recordVersion(6051);
    vh.recordVersion(6063);
    vh.recordVersion(6064);
    vh.recordVersion(6065);
    vh.recordVersion(6066);
    vh.recordVersion(6067);
    vh.recordVersion(6068);
    vh.recordVersion(6084);
    vh.recordVersion(6098);
    vh.recordVersion(6099);
    vh.recordVersion(6123);
    vh.recordVersion(6124);
    vh.recordVersion(6125);
    vh.recordVersion(6126);
    vh.recordVersion(6127);
    vh.recordVersion(6128);
    vh.recordVersion(6129);
    vh.recordVersion(6141);
    vh.recordVersion(6169);
    vh.recordVersion(6170);
    vh.recordVersion(6171);
    vh.recordVersion(6190);
    vh.recordVersion(6199);
    vh.recordVersion(6200);
    vh.recordVersion(6218);
    vh.recordVersion(6219);
    vh.recordVersion(6220);
    vh.recordVersion(6221);
    vh.recordVersion(6222);
    vh.recordVersion(6223);
    vh.recordVersion(6224);
    vh.recordVersion(6225);
    vh.recordVersion(6293);
    vh.recordVersion(6302);
    vh.recordVersion(6303);
    vh.recordVersion(6304);
    vh.recordVersion(6305);
    vh.recordVersion(6306);
    vh.recordVersion(6307);
    vh.recordVersion(6308);
    vh.recordVersion(6323);
    vh.recordVersion(6322);
    vh.recordVersion(6325);
    vh.recordVersion(6324);
    vh.recordVersion(6326);
    vh.recordVersion(6327);
    vh.recordVersion(6328);
    vh.recordVersion(6329);
    vh.recordVersion(6347);
    vh.recordVersion(6348);
    vh.recordVersion(6349);
    vh.recordVersion(6350);
    vh.recordVersion(6351);
    vh.recordVersion(6352);
    vh.recordVersion(6353);
    vh.recordVersion(6354);
    vh.recordVersion(6355);
    vh.recordVersion(6356);
    vh.recordVersion(6357);
    vh.recordVersion(6358);
    vh.recordVersion(6359);
    vh.recordVersion(6360);
    vh.recordVersion(6361);
    vh.recordVersion(6362);
    vh.recordVersion(6387);
    vh.recordVersion(6388);
    vh.recordVersion(6398);
    vh.recordVersion(6399);
    vh.recordVersion(6400);
    vh.recordVersion(6401);
    vh.recordVersion(6422);
    vh.recordVersion(6430);
    vh.recordVersion(6431);
    vh.recordVersion(6432);
    vh.recordVersion(6433);
    vh.recordVersion(6434);
    vh.recordVersion(6435);
    vh.recordVersion(6436);
    vh.recordVersion(6450);
    vh.recordVersion(6451);
    vh.recordVersion(6452);
    vh.recordVersion(6456);
    vh.recordVersion(6457);
    vh.recordVersion(6458);
    vh.recordVersion(6459);
    vh.recordVersion(6460);
    vh.recordVersion(6461);
    vh.recordVersion(6462);
    vh.recordVersion(6463);
    vh.recordVersion(6466);
    vh.recordVersion(6467);
    vh.recordVersion(6468);
    vh.recordVersion(6474);
    vh.recordVersion(6475);
    vh.recordVersion(6476);
    vh.recordVersion(6477);
    vh.recordVersion(6478);
    vh.recordVersion(6479);
    vh.recordVersion(6480);
    vh.recordVersion(6481);
    vh.recordVersion(6482);
    vh.recordVersion(6483);
    vh.recordVersion(6484);
    vh.recordVersion(6485);
    vh.recordVersion(6486);
    vh.recordVersion(6487);
    vh.recordVersion(6488);
    vh.recordVersion(6489);
    vh.recordVersion(6528);
    vh.recordVersion(6581);
    vh.recordVersion(6596);
    vh.recordVersion(6597);
    vh.recordVersion(6598);
    vh.recordVersion(6599);
    vh.recordVersion(6600);
    vh.recordVersion(6601);
    vh.recordVersion(6602);
    vh.recordVersion(6628);
    vh.recordVersion(6676);
    vh.recordVersion(6690);
    vh.recordVersion(6704);
    vh.recordVersion(6705);
    vh.recordVersion(6706);
    vh.recordVersion(6768);
    vh.recordVersion(6769);
    vh.recordVersion(6770);
    vh.recordVersion(6771);
    vh.recordVersion(6772);
    vh.recordVersion(6773);
    vh.recordVersion(6774);
    vh.recordVersion(6776);
    vh.recordVersion(6777);
    vh.recordVersion(6778);
    vh.recordVersion(6779);
    vh.recordVersion(6790);
    vh.recordVersion(6791);
    vh.recordVersion(6792);
    vh.recordVersion(6793);
    vh.recordVersion(6794);
    vh.recordVersion(6795);
    vh.recordVersion(6796);
    vh.recordVersion(6797);
    vh.recordVersion(6798);
    vh.recordVersion(6816);
    vh.recordVersion(6817);
    vh.recordVersion(6818);
    vh.recordVersion(6819);
    vh.recordVersion(6820);
    vh.recordVersion(6821);
    vh.recordVersion(6844);
    vh.recordVersion(6843);
    vh.recordVersion(6845);
    vh.recordVersion(6846);
    vh.recordVersion(6847);
    vh.recordVersion(6848);
    vh.recordVersion(6849);
    vh.recordVersion(6850);
    vh.recordVersion(6852);
    vh.recordVersion(6853);
    vh.recordVersion(6855);
    vh.recordVersion(6854);
    vh.recordVersion(6878);
    vh.recordVersion(6884);
    vh.recordVersion(6885);
    vh.recordVersion(6908);
    vh.recordVersion(6907);
    vh.recordVersion(6909);
    vh.recordVersion(6910);
    vh.recordVersion(6911);
    vh.recordVersion(6912);
    vh.recordVersion(6913);
    vh.recordVersion(6936);
    vh.recordVersion(6937);
    vh.recordVersion(6938);
    vh.recordVersion(6939);
    vh.recordVersion(6967);
    vh.recordVersion(6968);
    vh.recordVersion(6989);
    vh.recordVersion(7006);
    vh.recordVersion(7021);
    vh.recordVersion(7022);
    vh.recordVersion(7023);
    vh.recordVersion(7024);
    vh.recordVersion(7028);
    vh.recordVersion(7060);
    vh.recordVersion(7061);
    vh.recordVersion(7062);
    vh.recordVersion(7063);
    vh.recordVersion(7064);
    vh.recordVersion(7065);
    vh.recordVersion(7066);
    vh.recordVersion(7067);
    vh.recordVersion(7068);
    vh.recordVersion(7069);
    vh.recordVersion(7070);
    vh.recordVersion(7071);
    vh.recordVersion(7072);
    vh.recordVersion(7073);
    vh.recordVersion(7074);
    vh.recordVersion(7075);
    vh.recordVersion(7076);
    vh.recordVersion(7094);
    vh.recordVersion(7095);
    vh.recordVersion(7104);
    vh.recordVersion(7105);
    vh.recordVersion(7106);
    vh.recordVersion(7107);
    vh.recordVersion(7108);
    vh.recordVersion(7109);
    vh.recordVersion(7111);
    vh.recordVersion(7110);
    vh.recordVersion(7112);
    vh.recordVersion(7113);
    vh.recordVersion(7114);
    vh.recordVersion(7115);
    vh.recordVersion(7116);
    vh.recordVersion(7117);
    vh.recordVersion(7118);
    vh.recordVersion(7119);
    vh.recordVersion(7120);
    vh.recordVersion(7121);
    vh.recordVersion(7122);
    vh.recordVersion(7123);
    vh.recordVersion(7134);
    vh.recordVersion(7135);
    vh.recordVersion(7136);
    vh.recordVersion(7137);
    vh.recordVersion(7138);
    vh.recordVersion(7187);
    vh.recordVersion(7188);
    vh.recordVersion(7189);
    vh.recordVersion(7190);
    vh.recordVersion(7191);
    vh.recordVersion(7192);
    vh.recordVersion(7193);
    vh.recordVersion(7194);
    vh.recordVersion(7195);
    vh.recordVersion(7196);
    vh.recordVersion(7221);
    vh.recordVersion(7222);
    vh.recordVersion(7223);
    vh.recordVersion(7234);
    vh.recordVersion(7235);
    vh.recordVersion(7236);
    vh.recordVersion(7237);
    vh.recordVersion(7238);
    vh.recordVersion(7244);
    vh.recordVersion(7246);
    vh.recordVersion(7245);
    vh.recordVersion(7247);
    vh.recordVersion(7248);
    vh.recordVersion(7249);
    vh.recordVersion(7280);
    vh.recordVersion(7281);
    vh.recordVersion(7282);
    vh.recordVersion(7283);
    vh.recordVersion(7303);
    vh.recordVersion(7304);
    vh.recordVersion(7305);
    vh.recordVersion(7306);
    vh.recordVersion(7307);
    vh.recordVersion(7308);
    vh.recordVersion(7309);
    vh.recordVersion(7310);
    vh.recordVersion(7336);
    vh.recordVersion(7340);
    vh.recordVersion(7383);
    vh.recordVersion(7400);
    vh.recordVersion(7422);
    vh.recordVersion(7441);
    vh.recordVersion(7442);
    vh.recordVersion(7443);
    vh.recordVersion(7444);
    vh.recordVersion(7445);
    vh.recordVersion(7446);
    vh.recordVersion(7447);
    vh.recordVersion(7448);
    vh.recordVersion(7449);
    vh.recordVersion(7450);
    vh.recordVersion(7451);
    vh.recordVersion(7452);
    vh.recordVersion(7453);
    vh.recordVersion(7454);
    vh.recordVersion(7455);
    vh.recordVersion(7456);
    vh.recordVersion(7457);
    vh.recordVersion(7458);
    vh.recordVersion(7477);
    vh.recordVersion(7506);
    vh.recordVersion(7507);
    vh.recordVersion(7508);
    vh.recordVersion(7509);
    vh.recordVersion(7515);
    vh.recordVersion(7516);
    vh.recordVersion(7517);
    vh.recordVersion(7518);
    vh.recordVersion(7519);
    vh.recordVersion(7521);
    vh.recordVersion(7520);
    vh.recordVersion(7522);
    vh.recordVersion(7523);
    vh.recordVersion(7524);
    vh.recordVersion(7525);
    vh.recordVersion(7526);
    vh.recordVersion(7527);
    vh.recordVersion(7528);
    vh.recordVersion(7530);
    vh.recordVersion(7531);
    vh.recordVersion(7532);
    vh.recordVersion(7533);
    vh.recordVersion(7534);
    vh.recordVersion(7535);
    vh.recordVersion(7536);
    vh.recordVersion(7537);
    vh.recordVersion(7538);
    vh.recordVersion(7539);
    vh.recordVersion(7540);
    vh.recordVersion(7563);
    vh.recordVersion(7564);
    vh.recordVersion(7565);
    vh.recordVersion(7566);
    vh.recordVersion(7567);
    vh.recordVersion(7568);
    vh.recordVersion(7569);
    vh.recordVersion(7570);
    vh.recordVersion(7571);
    vh.recordVersion(7572);
    vh.recordVersion(7573);
    vh.recordVersion(7574);
    vh.recordVersion(7586);
    vh.recordVersion(7587);
    vh.recordVersion(7588);
    vh.recordVersion(7589);
    vh.recordVersion(7590);
    vh.recordVersion(7591);
    vh.recordVersion(7592);
    vh.recordVersion(7593);
    vh.recordVersion(7594);
    vh.recordVersion(7603);
    vh.recordVersion(7604);
    vh.recordVersion(7610);
    vh.recordVersion(7611);
    vh.recordVersion(7612);
    vh.recordVersion(7613);
    vh.recordVersion(7623);
    vh.recordVersion(7624);
    vh.recordVersion(7625);
    vh.recordVersion(7656);
    vh.recordVersion(7659);
    vh.recordVersion(7676);
    vh.recordVersion(7677);
    vh.recordVersion(7678);
    vh.recordVersion(7679);
    vh.recordVersion(7680);
    vh.recordVersion(7704);
    vh.recordVersion(7726);
    vh.recordVersion(7727);
    vh.recordVersion(7728);
    vh.recordVersion(7729);
    vh.recordVersion(7730);
    vh.recordVersion(7731);
    vh.recordVersion(7732);
    vh.recordVersion(7733);
    vh.recordVersion(7734);
    vh.recordVersion(7735);
    vh.recordVersion(7736);
    vh.recordVersion(7744);
    vh.recordVersion(7745);
    vh.recordVersion(7746);
    System.out.println("updated:\t" + vh);
    vh.removeExceptionsOlderThan(8712);
    System.out.println("after GC:\t" + vh);


    RegionVersionHolder clone = vh.clone();
    System.out.println("clone: \t\t" + clone);
    assertTrue("Expected a version greater than 7746 but got this: " + clone,
        clone.getVersion() >= 7746);

  }

  // Using huge values here to ensure we're efficiently creating canonicalExceptions
  private static final int NUM_TEST_EXCEPTIONS = 10;
  private static final int TEST_EXCEPTION_SIZE = 200000;

  @Test
  public void testCanonicalExceptions() {
    List<RVVException> exceptionList = new ArrayList<>();
    for (int i = NUM_TEST_EXCEPTIONS; i > 0; --i) {
      long start = i * TEST_EXCEPTION_SIZE;
      long end = start + TEST_EXCEPTION_SIZE;
      RVVException testException = RVVException.createException(start, end);
      for (long j = start + 2; j < end; j += 2) {
        testException.add(j);
      }
      exceptionList.add(testException);
    }

    List<RVVException> canonicalExceptions = RegionVersionHolder.canonicalExceptions(exceptionList);

    long expectedStart = NUM_TEST_EXCEPTIONS * TEST_EXCEPTION_SIZE + TEST_EXCEPTION_SIZE - 2;
    for (RVVException exception : canonicalExceptions) {
      assertEquals(expectedStart, exception.previousVersion);
      assertEquals(expectedStart + 2, exception.nextVersion);
      assertTrue(exception.isEmpty());
      expectedStart -= 2;
    }
  }

  /**
   * Test merging two version holders
   */
  @Test
  public void testInitializeFrom() {
    testInitializeFrom(false);
    testInitializeFrom(true);
  }

  private void testInitializeFrom(boolean withTreeSets) {
    RVVException.UseTreeSetsForTesting = withTreeSets;
    try {
      // Create a bit set that represents some seen version
      // with some exceptions
      BitSet bs1 = new BitSet();
      bs1.set(1, 6);
      bs1.set(10, 13);
      bs1.set(30, 51);
      bs1.set(60, 66);
      bs1.set(68, 101);

      // Get a version holder that has seen these exceptions
      RegionVersionHolder vh1 = buildHolder(bs1);
      validateExceptions(vh1);


      {
        // Simple case, initialize an empty version holder
        RegionVersionHolder vh2 = new RegionVersionHolder(member);
        vh2.initializeFrom(vh1);

        assertEquals("RVV=" + vh2, 100, vh2.getVersion());

        compareWithBitSet(bs1, vh2);
        validateExceptions(vh2);
      }

      {
        // Initialize a vector that has seen some later versions, and has some older
        // exceptions

        BitSet bs2 = new BitSet();

        // Add an exception that doesn't overlap with vh1 exceptions
        bs2.set(80, 106);
        bs2.set(72, 74);

        // Add an exception that is contained within a vh1 exception
        bs2.set(59, 70);
        bs2.set(57);

        // Add exceptions that partially overlap a vh1 exception on either end
        bs2.set(35, 56);
        bs2.set(15, 26);
        bs2.set(1, 4);

        RegionVersionHolder vh2 = buildHolder(bs2);

        validateExceptions(vh2);
        vh2.initializeFrom(vh1);

        assertEquals(105, vh2.version);
        assertEquals(100, vh2.getVersion());
        compareWithBitSet(bs1, vh2);

        RegionVersionHolder vh4 = vh2.clone();
        assertEquals(105, vh4.version);
        assertEquals(100, vh4.getVersion());
        compareWithBitSet(bs1, vh4);

        // use vh1 to overwrite vh2
        vh1.version = 105;
        vh1.addException(100, 106);
        assertTrue(vh2.sameAs(vh1));
        validateExceptions(vh2);
      }
    } finally {
      RVVException.UseTreeSetsForTesting = false;
    }
  }

  /**
   * Construct a region version holder that matches the seen revisions passed in the bit set.
   *
   */
  private RegionVersionHolder buildHolder(BitSet bs) {

    // Createa version holder
    RegionVersionHolder vh = new RegionVersionHolder(member);

    // Record all of the version in the holder
    recordVersions(vh, bs);

    // Make sure the holder looks matches the bitset.
    compareWithBitSet(bs, vh);

    return vh;
  }

  /**
   * Record all the versions represented in the bit set in the RegionVersionHolder. This method is
   * overridden in subclasses to change the recording order.
   */
  protected void recordVersions(RegionVersionHolder vh, BitSet bs) {
    // System.out.println("vh="+vh);
    for (int i = 1; i < bs.length(); i++) {
      if (bs.get(i)) {
        vh.recordVersion(i);
        // System.out.println("after adding " + i + ", vh="+vh);
      }
    }
  }

  /**
   * Test a case in 46522 where the received exceptions end up not being in the RVV interval.
   */
  @Test
  public void testConsumeReceivedRevisions() {
    testConsumeReceivedRevisions(false);
    testConsumeReceivedRevisions(true);
  }

  private void testConsumeReceivedRevisions(boolean useTreeSets) {
    try {
      RVVException.UseTreeSetsForTesting = useTreeSets;
      // Create a bit set which matches the seen versions of vh1
      BitSet bs1 = new BitSet();
      bs1.set(1, 6);
      bs1.set(27, 29);
      bs1.set(30, 41);
      bs1.set(42, 43);
      bs1.set(47, 49);
      bs1.set(50, 101);

      RegionVersionHolder vh1 = buildHolder(bs1);
      validateExceptions(vh1);
      compareWithBitSet(bs1, vh1);

      {
        // Initialize a vector that has seen some later versions, and has some older
        // exceptions
        BitSet bs2 = new BitSet();

        // Add an exception that doesn't overlap with vh1 exceptions
        bs2.set(1, 6);
        bs2.set(20, 44);
        bs2.set(49, 101);

        RegionVersionHolder vh2 = buildHolder(bs2);
        validateExceptions(vh2);
        vh2.initializeFrom(vh1);
        // bs2.or(bs1);
        assertEquals(100, vh2.version);
        compareWithBitSet(bs1, vh2);
        validateExceptions(vh2);
      }
    } finally {
      RVVException.UseTreeSetsForTesting = false;
    }
  }

  @Test
  public void testChangeSetForm() {
    try {
      RVVException.UseTreeSetsForTesting = true;
      // Create a bit set which matches the seen versions of vh1
      BitSet bs1 = new BitSet();
      bs1.set(1024);

      RegionVersionHolder vh1 = buildHolder(bs1);
      bs1.set(510);
      bs1.set(511);
      bs1.set(512);
      recordVersions(vh1, bs1);
      validateExceptions(vh1);
      compareWithBitSet(bs1, vh1);

    } finally {
      RVVException.UseTreeSetsForTesting = false;
    }
  }

  private void compareWithBitSet(BitSet bitSet, RegionVersionHolder versionHolder) {
    for (int i = 1; i < bitSet.length(); i++) {
      assertEquals("For entry " + i + " version=" + versionHolder, bitSet.get(i),
          versionHolder.contains(i));
    }
  }

  /**
   * Test the dominates relation between RegionVersionHolders.
   *
   * This test currently fails in the last case with two different holders with the same exception
   * list expressed differently. Hence it is disabled.
   *
   * See bug 47106
   */
  @Test
  public void testDominates() {
    testDominates(false);
    testDominates(true);
  }

  private void testDominates(boolean useTreeSets) {
    try {
      RVVException.UseTreeSetsForTesting = useTreeSets;

      // Simple case, - vh2 has a greater version than vh1
      {
        RegionVersionHolder vh1 = new RegionVersionHolder(member);
        RegionVersionHolder vh2 = new RegionVersionHolder(member);

        vh1.recordVersion(100);
        BitSet bs1 = new BitSet();
        bs1.set(1, 100);
        recordVersions(vh1, bs1);

        BitSet bs2 = new BitSet();
        bs2.set(1, 105);
        recordVersions(vh2, bs2);
        assertFalse(vh1.dominates(vh2));
        assertTrue(vh2.dominates(vh1));
      }

      // test with a gap between the bitsetVersion and the version
      {
        RegionVersionHolder vh1 = new RegionVersionHolder(member);
        RegionVersionHolder vh2 = new RegionVersionHolder(member);

        BitSet bs1 = new BitSet();
        bs1.set(1, 101);
        recordVersions(vh1, bs1);
        // this test assumes that the width is much greater than 100, so use the
        // original RegionVersionHolder.BIT_SET_WIDTH in order for the assertions to be correct
        vh1.recordVersion(2 * originalBitSetWidth);

        BitSet bs2 = new BitSet();
        bs2.set(1, 2 * originalBitSetWidth + 1);
        recordVersions(vh2, bs2);

        // vh1={rv2048 bsv100 bs={0, 1948}} or {rv2048 bsv2048 bs={0}; [e(n=2048 p=100)]} after
        // merging
        // vh2={rv2048 bsv2047 bs={0, 1}} or {rv2048 bsv2048 bs={0}} after merging

        assertFalse(vh1.dominates(vh2));
        assertTrue(vh2.dominates(vh1));
      }

      // More complicated. vh2 has a higher version, but has
      // some exceptions that vh1 does not have.
      {
        RegionVersionHolder vh1 = new RegionVersionHolder(member);
        RegionVersionHolder vh2 = new RegionVersionHolder(member);
        BitSet bs1 = new BitSet();
        bs1.set(1, 21);
        bs1.set(30, 101);
        recordVersions(vh1, bs1);

        BitSet bs2 = new BitSet();
        bs2.set(1, 50);
        bs2.set(60, 105);
        recordVersions(vh2, bs2);

        // double check that we didn't screw up the bit sets
        assertFalse(dominates(bs2, bs1));
        assertFalse(dominates(bs1, bs2));

        assertFalse(vh1.dominates(vh2));
        assertFalse(vh2.dominates(vh1));
      }

      // Same version, but both have some exceptions the other doesn't
      {
        // VH2 will have exceptions for 20-25, 26-30, 41-43
        RegionVersionHolder vh1 = new RegionVersionHolder(member);
        BitSet bs1 = new BitSet();

        // record some versions to generate two exceptions
        bs1.set(1, 21);
        bs1.set(30, 41);
        bs1.set(43, 101);
        recordVersions(vh1, bs1);

        // now populate some older versions.
        bs1.set(25);
        bs1.set(26);
        recordVersions(vh1, bs1);

        // VH2 will have exceptions for 20-25, 26-30, 42-45
        RegionVersionHolder vh2 = new RegionVersionHolder(member);
        BitSet bs2 = new BitSet();
        bs2.set(1, 21);
        bs2.set(25, 27);
        bs2.set(30, 43);
        bs2.set(45, 101);
        recordVersions(vh2, bs2);
        assertFalse(vh1.dominates(vh2));
        assertFalse(vh2.dominates(vh1));
      }

      // Same version, but vh2 has an exception that vh1 doesn't
      {
        RegionVersionHolder vh1 = new RegionVersionHolder(member);
        BitSet bs1 = new BitSet();
        bs1.set(1, 101);
        recordVersions(vh1, bs1);

        RegionVersionHolder vh2 = new RegionVersionHolder(member);
        BitSet bs2 = new BitSet();
        bs2.set(1, 43);
        bs2.set(45, 101);
        recordVersions(vh2, bs2);

        assertTrue(vh1.dominates(vh2));
        assertFalse(vh2.dominates(vh1));
      }

      // Same version, but vh2 has an exception that vh1 doesn't
      // With overlapping exception
      {
        RegionVersionHolder vh1 = new RegionVersionHolder(member);
        BitSet bs1 = new BitSet();
        bs1.set(1, 44);
        bs1.set(45, 101);
        recordVersions(vh1, bs1);

        RegionVersionHolder vh2 = new RegionVersionHolder(member);
        BitSet bs2 = new BitSet();
        bs2.set(1, 40);
        bs2.set(45, 101);
        recordVersions(vh1, bs1);
        assertTrue(vh1.dominates(vh2));
        assertFalse(vh2.dominates(vh1));
      }

      // Same version, but vh2 has an exception that vh1 doesn't
      // With some differently expressed
      // but equivalent exceptions.
      {
        RegionVersionHolder vh1 = new RegionVersionHolder(member);
        BitSet bs1 = new BitSet();
        bs1.set(1, 20);
        bs1.set(30, 101);
        recordVersions(vh1, bs1);

        bs1.set(25);
        bs1.set(26);
        recordVersions(vh1, bs1);

        RegionVersionHolder vh2 = new RegionVersionHolder(member);
        BitSet bs2 = new BitSet();
        bs2.set(1, 20);
        bs2.set(30, 101);
        bs2.set(25);
        bs2.set(26);
        recordVersions(vh2, bs2);
        // vh1.sameAs(vh2); vh2.sameAs(vh1); //force bitsets to be merged
        System.out.println("vh1=" + vh1);
        System.out.println("vh2=" + vh2);
        assertTrue(vh1.dominates(vh2));
        assertTrue(vh2.dominates(vh1));
      }
    } finally {
      RVVException.UseTreeSetsForTesting = false;
    }

  }

  /**
   * Return true if bs1 dominates bs2 - meaning that at least all of the bits set in bs2 are set in
   * bs1.
   *
   */
  private boolean dominates(BitSet bs1, BitSet bs2) {
    // bs1 dominates bs2 if it has set at least all of the bits in bs1.
    BitSet copy = new BitSet();
    // Make copy a copy of bit set 2
    copy.or(bs2);

    // Clear all of the bits in copy that are set in bit set 1
    copy.andNot(bs1);

    // If all of the bits have been cleared in copy, that means
    // bit set 1 had at least all of the bits set that were set in
    // bs2
    return copy.isEmpty();

  }

  /**
   * For test purposes, make sure this version holder is internally consistent.
   */
  private void validateExceptions(RegionVersionHolder<?> holder) {
    if (holder.getExceptionForTest() != null) {
      for (RVVException ex : holder.getExceptionForTest()) {
        // now it allows the special exception whose nextVersion==holder.version+1
        if (ex.nextVersion > holder.version + 1) {
          Assert.assertTrue(false, "next version too large next=" + ex.nextVersion
              + " holder version " + holder.version);
        }

        if (ex.nextVersion <= ex.previousVersion) {
          Assert.assertTrue(false,
              "bad next and previous next=" + ex.nextVersion + ", previous=" + ex.previousVersion);
        }

        for (RVVException.ReceivedVersionsReverseIterator it =
            ex.receivedVersionsReverseIterator(); it.hasNext();) {
          Long received = it.next();
          if (received >= ex.nextVersion) {
            Assert.assertTrue(false, "received greater than next next=" + ex.nextVersion
                + ", received=" + received + " exception=" + ex);
          }

          if (received <= ex.previousVersion) {
            Assert.assertTrue(false, "received less than previous prev=" + ex.previousVersion
                + ", received=" + received);
          }
        }
        if (ex.nextVersion - ex.previousVersion > 1000000) {
          Assert.assertTrue(false, "to large a gap in exceptions prev=" + ex.previousVersion
              + ", next=" + ex.nextVersion);
        }
      }
    }
  }

}
