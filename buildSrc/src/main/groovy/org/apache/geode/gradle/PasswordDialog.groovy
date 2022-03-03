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
import groovy.swing.SwingBuilder
class PasswordDialog {
  static String askPassword(String prompt) {
    def password = ''
    new SwingBuilder().edt {
    dialog(modal: true, 
        title: 'Password',
        alwaysOnTop: true, 
        locationRelativeTo: null,
        pack: true, 
        show: true
    ) {
      vbox { 
        label(text: prompt)
        input = passwordField()
        button(defaultButton: true, text: 'OK', actionPerformed: {
          password = input.password.toString() // Set pass variable to value of input field
          dispose() // Close dialog
        })
      }
      }
    }
    return password
  }
}
