#!/bin/bash
export PATH=$PATH:`pwd`/target/release:`pwd`/personas/cassnbrs/target/release
eval "$(nbrs completions bash)"
