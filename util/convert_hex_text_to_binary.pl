#!/usr/bin/env perl

use strict;
use warnings;

while (<>) {
    chomp;
    s/\s*//g;
    print pack("H*", $_);
}
