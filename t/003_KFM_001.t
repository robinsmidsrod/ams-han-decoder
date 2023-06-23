#!perl

BEGIN { do "./t/test_setup.pl" or die( $@ || $! ) }

plan 3;

my $obis_map = 'KFM_001';

my @files = qw(
    MA304H3E-1-ok
    MA304H3E-4-ok
    MA304T4-3-ok
);

foreach my $file ( @files ) {
    my ($stdout, $stderr, @result) = capture { run_decoder('-m', $obis_map, "t/data/$obis_map/$file.bin") };
    is(
        $stdout,
        scalar read_file("t/data/$obis_map/$file.jsonl"),
        "Decoding $obis_map/$file.bin to JSON works",
    );
    note($stderr);
}

1;
