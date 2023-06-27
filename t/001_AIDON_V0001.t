#!perl

BEGIN { do "./t/test_setup.pl" or die( $@ || $! ) }

plan 3;

my $obis_map = 'AIDON_V0001';

my @files = qw(
    6515-1-ok
    6525-3-FC722-ok
    6525-2-TSS721-frame-checksum-failures-on-large-frames
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
