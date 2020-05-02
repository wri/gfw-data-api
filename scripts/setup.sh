#!/usr/bin/env bash

set -e

detect-secrets scan > .secrets.baseline
pre-commit install