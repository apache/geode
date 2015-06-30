/*=========================================================================
 * Copyright (c) 2010-2014 Pivotal Software, Inc. All Rights Reserved.
 * This product is protected by U.S. and international copyright
 * and intellectual property laws. Pivotal products are covered by
 * one or more patents listed at http://www.pivotal.io/patents.
 *=========================================================================
 */
package com.gemstone.sequence;

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
