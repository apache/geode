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

package org.apache.geode.gradle.plugins

import java.awt.Frame
import javax.swing.Box
import javax.swing.JButton
import javax.swing.JDialog
import javax.swing.JLabel
import javax.swing.JPasswordField
import javax.swing.SwingUtilities

class PasswordDialog {
  static String askPassword(String prompt) {
    def password = ''
    SwingUtilities.invokeAndWait {
      JDialog dialog = new JDialog((Frame) null, 'Password', true)
      JPasswordField input = new JPasswordField()
      JButton ok = new JButton('OK')
      ok.addActionListener {
        password = new String(input.password) // Set pass variable to value of input field
        dialog.dispose() // Close dialog
      }

      Box box = Box.createVerticalBox()
      box.add(new JLabel(prompt))
      box.add(input)
      box.add(ok)
      dialog.contentPane.add(box)
      dialog.rootPane.defaultButton = ok
      dialog.alwaysOnTop = true
      dialog.pack()
      dialog.locationRelativeTo = null
      dialog.visible = true
    }
    return password
  }
}
