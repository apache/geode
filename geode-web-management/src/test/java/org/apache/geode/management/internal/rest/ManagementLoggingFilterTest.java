/*
 *
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
 *
 */

package org.apache.geode.management.internal.rest;

import static org.assertj.core.api.Assertions.assertThat;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

public class ManagementLoggingFilterTest {
  ManagementLoggingFilter filter;

  @BeforeEach
  public void before() {
    filter = new ManagementLoggingFilter();
  }

  @Test
  public void stripFileConetent_no_java_archive() {
    String payload = "{a:b}\n";
    assertThat(filter.stripMultiPartFileContent(payload)).isEqualTo(payload);
  }

  @Test
  public void stripFileConetent_with_java_archive() {
    String payLoad = "--FLaQNajADavDbycIwdHAkrQ6YIf3ubGg\n"
        + "Content-Disposition: form-data; name=\"file\"; filename=\"DeployCommandRedeployDUnitTestA.jar\"\n"
        + "Content-Type: application/java-archive\n"
        + "Content-Length: 731\n"
        + "\n"
        + "PK\u0003\u0004\u0014\u0000\b\b\b\u0000â\\ÉT\u0000\u0000\u0000\u0000\u0000\u0000\u0000\u0000\u0000\u0000\u0000\u0000)\u0000\u0004\u0000DeployCommandRedeployDUnitFunctionA.classþÊ\u0000\u0000\u0095\u0092ÏnÓ@\u0010Æ¿Í?§IhChhÓ\u0092\u0096¶Ð:\tÔBp+B\u008A\u0012E EP5P$n\u008E³J·rìh½F\u0085gâ\u0000\u0017P9ð\u0000<\u0014bÖqUp\u0085H\u000F\u009Eµw\u007Fóyæ\u009Býùëû\u000F\u0000Oð°\u0080<îäQ/b\u0003\u009ByÜ-b\u000BÛ\u0005\u0018Ø1P7pÏÀ}\u0003»\f¹§Â\u0013ê\u0019CÚl\u001C3d:þ\u00883,õ\u0085Ç_\u0086\u0093!\u0097¯í¡K;Ù1W/F\fËf£\u007Fj¿·-×öÆÖ@Iá\u008D\u000F(O«0ÔÌÙa¨\u0084k\u001DJ\u007FÊ¥\u0012<8ÐÒ\u0006?ãN¨Hë±Ù÷åØ²§¶sÂ\u00AD1§_ZNô\u001E#V/ô\u001C%|¯ã{\u008A\u009F©(\u007FáÄ\u000E\u008Ex\u0010º**ö\u001DCÙ\u009F*1\u0011\u001FyÏ\u0097o¥ÐÒ\u0019\u0011<o3¬þUeÇ\u009FLm©û J\u000B\u0003?\u0094\u000Eï\tÝ\u0095ÙåS×ÿ@ÀÄöFG|\u0014}vßP3\u0017%´÷µP\tE\u0094\u0018væà\n"
        + "ì\u0095`¢Á\u0090?æ2 \u00ADG\u0006\u009A%´ð \u0084%\u0094©îËÒ^\n"
        + "O¹C\n"
        + "íÍi\bÃÖ?È.wÜY\u0093\fû×³\u0097æMÓ\u009Dy;àÞ\u0088K\u0006\u008B\u001Cü\u008FÊ\u009F\tdlë\u001A8\u008DÁµ\u0003u1ÎªÙOZB#§\u001BkÐ-Na\u0001\u0005ºÔy0=\u0004\u008A7è«N+£5Ûü\u0006ö\u0085^\u0018\u0016)æ¢Í*Er:F×c4]I}N\u0080+\u0014o¢\u0012\u00835zR\u009AHêi\u0085[X\u008E±&A\u001A[l\u009D#ÍPÉ\u009C#\u009BÂeN!:Ý v\u0093vª¸\u001Dç\u00ADÅu¤2\u009F\u0012úÛ\u0014W®ré$·Kqu\u000E®YnSÿµ+V}E.é\u0080¶j-ê{ý7PK\u0007\b4|'\u0086\u000F\u0002\u0000\u0000=\u0004\u0000\u0000PK\u0001\u0002\u0014\u0000\u0014\u0000\b\b\b\u0000â\\ÉT4|'\u0086\u000F\u0002\u0000\u0000=\u0004\u0000\u0000)\u0000\u0004\u0000\u0000\u0000\u0000\u0000\u0000\u0000\u0000\u0000\u0000\u0000\u0000\u0000\u0000\u0000DeployCommandRedeployDUnitFunctionA.classþÊ\u0000\u0000PK\u0005\u0006\u0000\u0000\u0000\u0000\u0001\u0000\u0001\u0000[\u0000\u0000\u0000j\u0002\u0000\u0000\u0000\u0000\n"
        + "--FLaQNajADavDbycIwdHAkrQ6YIf3ubGg\n"
        + "Content-Disposition: form-data; name=\"config\"\n"
        + "Content-Type: application/json\n"
        + "{\"class\":\"org.apache.geode.management.configuration.Deployment\",\"group\":null,\"deployedTime\":null,\"deployedBy\":null,\"fileName\":\"DeployCommandRedeployDUnitTestA.jar\"}\n"
        + "--FLaQNajADavDbycIwdHAkrQ6YIf3ubGg--\n";
    String stripped = "--FLaQNajADavDbycIwdHAkrQ6YIf3ubGg\n"
        + "Content-Disposition: form-data; name=\"file\"; filename=\"DeployCommandRedeployDUnitTestA.jar\"\n"
        + "Content-Type: application/java-archive\n"
        + "{File Content Logging Skipped}\n"
        + "--FLaQNajADavDbycIwdHAkrQ6YIf3ubGg\n"
        + "Content-Disposition: form-data; name=\"config\"\n"
        + "Content-Type: application/json\n"
        + "{\"class\":\"org.apache.geode.management.configuration.Deployment\",\"group\":null,\"deployedTime\":null,\"deployedBy\":null,\"fileName\":\"DeployCommandRedeployDUnitTestA.jar\"}\n"
        + "--FLaQNajADavDbycIwdHAkrQ6YIf3ubGg--\n";
    assertThat(filter.stripMultiPartFileContent(payLoad)).isEqualTo(stripped);
  }
}
