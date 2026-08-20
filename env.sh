#!/bin/bash
export PATH=$PATH:`pwd`/target/release:`pwd`/personas/cassnmbrs/target/release
eval "$(nmbrs completions bash)"
