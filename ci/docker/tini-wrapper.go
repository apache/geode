package main

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

import (
	"os"
	"syscall"
	"os/exec"
	"log"
)

func main() {
	tini, err := exec.LookPath("tini")
	if err != nil {
		log.Panicf("Unable to find 'tini' in PATH")
	}

	os.Setenv("TINI_SUBREAPER", "1")

	tiniArgs := []string{"-s", os.Args[1], "--"}
	for _, v := range os.Args[2:] {
		tiniArgs = append(tiniArgs, v)
	}

	err = syscall.Exec(tini, tiniArgs, os.Environ())
	if err != nil {
		panic(err)
	}
}
