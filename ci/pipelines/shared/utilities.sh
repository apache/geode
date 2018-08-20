#!/usr/bin/env bash


getSanitizedBranch () {
  echo ${1} | tr "/" "-" | tr '[:upper:]' '[:lower:]' | cut -c 1-20
}

getSanitizedFork () {
  echo ${1} | tr "/" "-" | tr '[:upper:]' '[:lower:]' | cut -c 1-16
}

