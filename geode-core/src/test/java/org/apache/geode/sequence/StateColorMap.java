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
package org.apache.geode.sequence;

import java.awt.*;
import java.util.HashMap;
import java.util.Map;

import static java.lang.Math.random;

/**
 * Created by IntelliJ IDEA.
 * User: dsmith
 * Date: Nov 12, 2010
 * Time: 5:05:49 PM
 * To change this template use File | Settings | File Templates.
 */
public class StateColorMap {
    private Map<Object, Color> colors = new HashMap<Object, Color>();
    private static Color[] PREDEFINED_COLORS = new Color[] { Color.BLUE, Color.BLACK, Color.PINK, Color.CYAN, Color.ORANGE, Color.GREEN};

    private ColorList colorList = new ColorList();

    public StateColorMap() {
        colors.put("created", Color.GREEN);
        colors.put("destroyed", Color.WHITE);
        colors.put("persisted", new Color(0, 150, 0));
    }


    public Color getColor(Object state) {
        Color color = colors.get(state);
        if(color == null) {
            color = colorList.nextColor();
            colors.put(state, color);
        }

        return color;
    }

    private static class ColorList {
        int colorIndex;

        public Color nextColor() {
            if(colorIndex < PREDEFINED_COLORS.length) {
                return PREDEFINED_COLORS[colorIndex++];
            } else {
                return Color.getHSBColor((float)random(), (float)random(), (float)random());
            }
        }
    }
}
