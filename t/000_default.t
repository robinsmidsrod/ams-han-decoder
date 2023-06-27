#!perl

BEGIN { do "./t/test_setup.pl" or die( $@ || $! ) }

plan 2;

ok( -x "ams_han_decoder.pl", "Script exists and is executable" );
like( capture_stderr { run_decoder() }, qr/^Usage:/, "Running without params shows help" );

1;
