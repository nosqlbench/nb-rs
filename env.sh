#!/bin/bash
export PATH=$PATH:`pwd`/target/release
eval "$(nbrs completions bash)"
