#!perl

BEGIN { do "./t/test_setup.pl" or die( $@ || $! ) }

plan 1;

my $obis_map = 'Kamstrup_V0001';

my @files = qw(
    6841121BN243101040-1-ok-not-norwegian
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
