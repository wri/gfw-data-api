#!/usr/bin/env bash

detect-secrets scan > .secrets.baseline
pre-commit install