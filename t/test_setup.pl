#!perl

# Will set strict/warnings on all test scripts
require strict and strict->import();
require warnings and warnings->import();

use strict;
use warnings;

use Test2::V0;
use Capture::Tiny ':all';
use File::Slurp qw(read_file);

sub run_decoder {
    my (@args) = @_;
    system($^X, "ams_han_decoder.pl", @args);
}

1;

