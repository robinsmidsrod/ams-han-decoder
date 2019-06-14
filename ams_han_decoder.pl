#!/usr/bin/env perl
#
# Decoder for binary serial stream from HAN port of power meters in Norway.
#
# Author: Robin Smidsrød <robin@smidsrod.no>
#
# Based on documentation from https://www.nek.no/info-ams-han-utviklere/
# and https://github.com/roarfred/AmsToMqttBridge/tree/master/Documentation
#
# Input is either a file with already dumped binary stream or a serial
# character device connected to an appropriate MBUS slave adapter.
# 
# Tested with Aidon 6525 power meter and a PL2303-based USB-to-MBUS adapter (FC722) from AliExpress:
# https://www.aliexpress.com/item/USB-transfer-MBUS-module-slave-module-communication-debug-alternative-TSS721/32719562958.html
#
# NB: As mentioned on https://www.hjemmeautomasjon.no/forums/topic/2873-lesing-av-han-the-easy-way-tm-wip/
# the USB-to-MBUS adapter with plastic casing is tested and does cut off long messages as mentioned.
# It is not supported. For reference, here is the link to the broken product on AliExpress:
# https://www.aliexpress.com/item/Freeshipping-USB-to-MBUS-slave-module-discrete-component-non-TSS721-circuit-M-BUS-bus-data-monitor/32814808312.html
#
# If you use this with system perl, it should be enough to install libjson-perl
# to get it to run, or use cpanm and the provided cpanfile for installation of
# dependencies.
#
# Run the script with -h for help.  If you're having problems with decoding
# the HDLC frames from your MBUS adapter, try to use the frame_dumper.pl
# program to get a better understanding of your stream.
#
# The OBIS code mapping tables for Kamstrup and Kaifa meters are incomplete. 
# Pull requests to complete them are most welcome.

use strict;
use warnings;

use JSON ();
use Carp qw(confess);
use Getopt::Std;

STDOUT->autoflush(1);
STDERR->autoflush(1);

my $opts = {};
getopts('cdhm:q', $opts);

if ( $opts->{'h'} or not $opts->{'m'} ) {
    print STDERR <<"EOM";
Usage: $0 [options] [<file|device>]
    -m OBIS code mapping table (required)
    -c Compact JSON output (one meter reading per line)
    -d Show debug information
    -q Show as little information as possible
    -h This help

If you specify a character device, stty will be run to configure its serial
settings. 2400 8E1 is the default serial settings. Edit the script if you
need something else. If you don't specify a file or device, standard input
will be opened and used.

An OBIS code mapping table must be specified.  The currently supported
values are as follows: AIDON_V0001, Kamstrup_V0001, KFM_001

You can also set the environment variable AMS_OBIS_MAP.  If both are set,
the command-line option takes precedence.
EOM
    exit 1;
}

my $is_compact = $opts->{'c'} ? 1 : 0;
my $is_pretty  = $opts->{'c'} ? 0 : 1;
my $json_coder = JSON->new->canonical;
$json_coder->pretty() if $is_pretty;

my $obis_map = get_obis_map( meter_type() );

my $file = shift @ARGV;
if ( defined $file and -r $file ) {
    configure_serial_port($file);
    open my $fh, '<:raw', $file or die("Can't open $file: $!\n");
    print STDERR "Reading from file $file...\n"
        unless QUIET();
    parse_stream($fh);
    close($fh);
}
else {
    print STDERR "Reading from standard input...\n"
        unless QUIET();
    binmode *STDIN, ':raw';
    parse_stream(*STDIN);
}

exit;

sub QUIET {
    return $opts->{'q'} ? 1 : 0;
}

sub DEBUG {
    return $opts->{'d'} ? 1 : 0;
}

sub meter_type {
    return $opts->{'m'} // $ENV{'AMS_OBIS_MAP'};
}

# Set serial port to 2400 baud 8E1
sub configure_serial_port {
    my ($file) = @_;
    # Don't configure anything unless the file is a character device
    return 0 unless -c $file;
    system('stty',
        '-F', $file,    # device to modify
        'sane',         # reset to sane settings
        'raw',          # set device to be a data channel, not an interactive terminal
        2400,           # 2400 baud rate
        'cs8',          # 8 data bits
        '-parodd',      # even parity
        '-cstopb',      # 1 stop bit
        '-onlcr',       # don't translate newline to carriage return-newline
        '-iexten',      # disable non-POSIX special characters
        '-echo',        # don't echo input characters
        '-echoe',       # don't echo erase characters as backspace-space-backspace
        '-echok',       # don't echo a newline after a kill character
        '-echoctl',     # don't echo control characters in hat notation ('^c')
        '-echoke',      # kill all line by obeying the echoctl and echok settings
    );
    return 1;
}

sub parse_stream {
    my ($stream) = @_;

    my $read_stream = sub {
        return read($stream, $_[1], $_[0]);
    };


    my $rc;
    while ( $rc = $read_stream->(1, my $flag) ) {
        last unless $rc;
        # Start frame flag not found, just noise
        if ( unpack('C', $flag) != 0x7e ) {
            print as_hex($flag) if DEBUG;
            next;
        }
        print "\n" . as_hex($flag) . " (flag:$rc) "
            if DEBUG;
        $rc = $read_stream->(1, my $frame_format);
        last unless $rc;
        # Start frame flag was end of frame flag, so check again if start
        # frame flag was found instead of frame format value
        if ( unpack('C', $frame_format) == 0x7e ) {
            print as_hex($frame_format) . " (frame-format-as-flag:$rc) "
                if DEBUG;
            $rc = $read_stream->(2, my $buffer);
            last unless $rc;
            $frame_format = $buffer;
        }
        else {
            $rc = $read_stream->(1, my $buffer);
            last unless $rc;
            $frame_format .= $buffer;
        }
        print as_hex($frame_format) . " (frame format:$rc) "
            if DEBUG;
        my ($length, $segmentation, $type)= decode_hdlc_frame_format($frame_format);
        next unless defined $length and $length > 2;
        $rc = $read_stream->( $length - 2, my $frame );
        last unless $rc;
        eval { handle_hdlc_frame($frame_format . $frame, $length, $segmentation, $type); };
        print STDERR "Decoding HDLC frame failed: $@"
            if $@ and DEBUG;
    }

    print STDERR "read from stream failed: $!\n" unless defined $rc;
    return;
}

sub as_hex {
    return unpack('H*', $_[0]);
}

sub decode_hdlc_frame_format {
    my ($frame_format) = @_;
    if ( length($frame_format) != 2 ) {
        warn("Unexpected frame format field: " . as_hex($frame_format) . "\n");
        return;
    }
    # Frame format field: 2 bytes
    # MSB                                    LSB
    # | Type(4) | Segmentation(1) | Length(11) |
    my $length       = vec( $frame_format, 0, 16) & 0x07ff;
    my $segmentation = vec( $frame_format, 0, 16) & 0x0800 >> 11;
    my $type         = vec( $frame_format, 0, 16) & 0xf000 >> 12;
    return ( $length, $segmentation, $type );
}

# Format of binary messages are documented in Excerpt GB8, pages 48 and onward
# HDLC frame format type 3 (Annex H.4 of ISO/IEC 13239) - not really type 3 (from Aidon 6525)
#
# | Flag | Frame format | Dest. address | Src. address | Control | HCS | Information | FCS | Flag |
#
sub handle_hdlc_frame {
    my ($frame, $length, $segmentation, $type) = @_;
    return unless defined $frame;
    return unless length $frame > 0;

    print STDERR "FRAME: " . as_hex($frame) . " (" . length($frame) . ")\n"
        if DEBUG;

    my $index = 0;
    my $read_bytes = sub {
        my ($len) = @_;
        my $bytes = substr($frame, $index, $len);
        confess("Read bytes doesn't match requested length")
            if length($bytes) != $len;
        $index += $len;
        return $bytes;
    };

    my @fields;
    my $read_bits = sub {
        my ($bits, $name) = @_;
        $bits //= 8;
        $name //= 'unknown';
        my $raw = $read_bytes->($bits / 8);
        my $hex = as_hex($raw);
        my $rec = { raw => $raw, name => $name, hex => $hex };
        push @fields, $rec;
    };

    # Aidon 6525 example: List 2 sending (1-phase) (from documentation)
    # 7e a0d2 41 0883 13 82d6 e6e700
    #     0f 40000000 00
    #     0109
    #         0202 0906 0101000281ff 0a0b 4149444f4e5f5630303031
    #         0202 0906 0000600100ff 0a10 37333539393932383930393431373432
    #         0202 0906 0000600107ff 0a04 36353135
    #         0203 0906 0100010700ff 06 00000552 0202 0f00 161b
    #         0203 0906 0100020700ff 06 00000000 0202 0f00 161b
    #         0203 0906 0100030700ff 06 000003e4 0202 0f00 161d
    #         0203 0906 0100040700ff 06 00000000 0202 0f00 161d
    #         0203 0906 01001f0700ff 10 005d     0202 0fff 1621
    #         0203 0906 0100200700ff 12 09c4     0202 0fff 1623
    # e0c4 7e

    # Aidon 6525 actual data: every 2.5 seconds
    # 7e a02a 41 0883 13 0413 e6e700
    #     0f 40000000 00
    #     0101
    #         0203 0906 0100010700ff 06 00000e90 0202 0f00 161b
    # 7724 7e

    # Aidon 6525 actual data: every 10 seconds
    # 7e a10b 41 0883 13 fa7c e6e700
    #     0f 40000000 00
    #     010c
    #         0202 0906 0101000281ff 0a0b 4149444f4e5f5630303031
    #         0202 0906 0000600100ff 0a10 3733XXXXXXXXXXXXXXXXXXXXXXX13130
    #         0202 0906 0000600107ff 0a04 36353235
    #         0203 0906 0100010700ff 06 00000e90 0202 0f00 161b
    #         0203 0906 0100020700ff 06 00000000 0202 0f00 161b
    #         0203 0906 0100030700ff 06 0000001c 0202 0f00 161d
    #         0203 0906 0100040700ff 06 00000000 0202 0f00 161d
    #         0203 0906 01001f0700ff 10 0091     0202 0fff 1621
    #         0203 0906 0100470700ff 10 0090     0202 0fff 1621
    #         0203 0906 0100200700ff 12 0932     0202 0fff 1623
    #         0203 0906 0100340700ff 12 091e     0202 0fff 1623
    #         0203 0906 0100480700ff 12 0933     0202 0fff 1623
    # 95d4 7e

    # Aidon 6525 actual data: every 1 hour
    # 7e a177 41 0883 13 391e e6e700
    #     0f 40000000 00
    #     0111
    #         0202 0906 0101000281ff 0a0b 4149444f4e5f5630303031
    #         0202 0906 0000600100ff 0a10 3733XXXXXXXXXXXXXXXXXXXXXXXX3130
    #         0202 0906 0000600107ff 0a04 36353235
    #         0203 0906 0100010700ff 06 00000da6 0202 0f00 161b
    #         0203 0906 0100020700ff 06 00000000 0202 0f00 161b
    #         0203 0906 0100030700ff 06 00000000 0202 0f00 161d
    #         0203 0906 0100040700ff 06 00000066 0202 0f00 161d
    #         0203 0906 01001f0700ff 10 0083     0202 0fff 1621
    #         0203 0906 0100470700ff 10 0085     0202 0fff 1621
    #         0203 0906 0100200700ff 12 0953     0202 0fff 1623
    #         0203 0906 0100340700ff 12 0939     0202 0fff 1623
    #         0203 0906 0100480700ff 12 094c     0202 0fff 1623
    #         0202 0906 0000010000ff 090c 07e3 06 0c 03 17 00 00 ff 003c 00
    #         0203 0906 0100010800ff 06 0021684d 0202 0f01 161e
    #         0203 0906 0100020800ff 06 00000000 0202 0f01 161e
    #         0203 0906 0100030800ff 06 00008251 0202 0f01 1620
    #         0203 0906 0100040800ff 06 00011ba5 0202 0f01 1620
    # 41ea 7e

    # Line 1 (HDLC header)
    $read_bits->(16, 'hdlc_frame_format');
    $read_bits->(8,  'hdlc_client_addr');
    $read_bits->(16, 'hdlc_server_addr');
    $read_bits->(8,  'hdlc_control');
    $read_bits->(16, 'hdlc_hcs'); # FCS16 checksum of HDLC header (not verified)
    $read_bits->(8,  'llc_dst_lsap'); # LLC PDU, see GB8 page 47
    $read_bits->(8,  'llc_src_lsap'); # LLC PDU, see GB8 page 47
    $read_bits->(8,  'llc_control'); # LLC PDU, see GB8 page 47
    # Line 2 (APDU not ecnrypted)
    $read_bits->(8,  'apdu_tag');
    $read_bits->(32, 'apdu_invoke_id_and_priority');
    $read_bits->(8,  'apdu_datetime'); # not really
    # Line 3 (payload)
    $read_bits->(8,  'payload_datatype'); # seems to always be 01
    $read_bits->(8,  'payload_register_count'); # varies depending on hdlc_frame_format type
    # Decode registers (as many as 'payload_register_count' specifies)
    my $register_count = unpack('C', $fields[-1]->{'raw'} );
    my @registers;
    my $register_index = 1;
    for ( $register_index..$register_count ) {
        my @register;
        push @register, as_hex( $read_bytes->(2) ); # unknown - values: 0202 / 0203
        push @register, as_hex( $read_bytes->(2) ); # unknown - values: 0906
        push @register, sprintf('%d-%d:%d.%d.%d.%d', unpack('C*', $read_bytes->(6) ) ); # OBIS code
        push @register, unpack('C', $read_bytes->(1) ); # data type (DLMS page 34)
        if    ( $register[3] == 0x0a ) {
            # visible string
            push @register, unpack('C', $read_bytes->(1) ); # length
            push @register, $read_bytes->( $register[4] ); # ascii string
        }
        elsif ( $register[3] == 0x06 ) {
            # double long unsigned, big-endian (32-bit integer)
            push @register, unpack('L>', $read_bytes->(4) ); # value
            push @register, as_hex( $read_bytes->(2) ); # unknown - values: 0202
            push @register, as_hex( $read_bytes->(2) ); # unknown - values: 0f00 / 0f01
            push @register, as_hex( $read_bytes->(2) ); # unknown - values: 161b / 161d / 161e / 1620
        }
        elsif ( $register[3] == 0x10 ) {
            # long signed, big-endian (16-bit integer)
            push @register, unpack('s>', $read_bytes->(2) ); # value
            push @register, as_hex( $read_bytes->(2) ); # unknown - values: 0202
            push @register, as_hex( $read_bytes->(2) ); # unknown - values: 0fff
            push @register, as_hex( $read_bytes->(2) ); # unknown - values: 1621
        }
        elsif ( $register[3] == 0x12 ) {
            # long unsigned, big-endian (16-bit integer)
            push @register, unpack('S>', $read_bytes->(2) ); # value
            push @register, as_hex( $read_bytes->(2) ); # unknown - values: 0202
            push @register, as_hex( $read_bytes->(2) ); # unknown - values: 0fff
            push @register, as_hex( $read_bytes->(2) ); # unknown - values: 1623
        }
        elsif ( $register[3] == 0x09 ) {
            # octet string
            push @register, unpack('C', $read_bytes->(1) ); # length
            my $octets = $read_bytes->( $register[4] ); # binary string
            if ( $register[2] eq '0-0:1.0.0.255' ) {
                # clock value, DLMS page 35-37
                # big-endian, first and next-to-last value is 16-bit integer, rest are 8-bit integers
                my ($year, $month, $day, $dow, $hour, $min, $sec, $frac, $offset, $status) = unpack('s>CCC CCCC s>C', $octets);
                # formatted as a string (almost ISO format)
                push @register, sprintf('%u-%02u-%02u %02u:%02u:%02u,%u %+d (%b)', $year, $month, $day, $hour, $min, $sec, $frac, $offset, $status);
                # formatted as an arrayref (pay attention to the day-of-week in the middle and status at the end)
                #push @register, [ $year, $month, $day, $dow, $hour, $min, $sec, $frac, $offset, $status ];
            }
            else {
                push @register, as_hex( $octets );
            }
        }
        else {
            # something unsupported, will probably break
            confess(join(" ",
                "Unsupported DLMS data type", sprintf('%X', $register[3]),
                "for register", $register_index,
                "with OBIS code", $register[2],
            ));
        }
        push @registers, \@register;
        $register_index++;
    }
    # Last line (HDLC footer)
    $read_bits->(16, 'hdlc_fcs'); # FCS16 checksum of entire HDLC frame (not verified)
    
    # This prints the remaining bytes of the message that has not yet been decoded
    print STDERR "REMAIN:" . ( " " x ($index * 2) ) . as_hex( substr($frame, $index) ) . "\n"
        if DEBUG;

    # Output frame information as JSON
    print $json_coder->encode({
        frame => {
            hdlc_length => $length,
            hdlc_segmentation => $segmentation,
            hdlc_type => $type,
            (
                map { $_->{'name'} => $_->{'hex'} }
                @fields
            )
        },
        registers => {
            map { convert_register($_) }
            @registers
        },
    });
    print "\n" if $is_compact;

    return 1;
}

sub convert_register {
    my ($register) = @_;
    return unless defined $register;
    return unless ref $register eq ref [];
    return if scalar @$register < 6;
    my $obis_code = $register->[2];
    my $data_type = $register->[3];
    return extend_obis( $obis_code, $register->[5] ) if $data_type == 0x0a; # visible-string
    return extend_obis( $obis_code, $register->[4] ) if $data_type == 0x06; # double long unsigned
    return extend_obis( $obis_code, $register->[4] ) if $data_type == 0x10; # long signed
    return extend_obis( $obis_code, $register->[4] ) if $data_type == 0x12; # long unsigned
    return extend_obis( $obis_code, $register->[5] ) if $data_type == 0x09; # octet-string
    return;
}

sub extend_obis {
    my ($obis_code, $value) = @_;
    my $meta = $obis_map->{$obis_code} // [];
    my ($key, $desc, $unit, $factor) = @$meta;
    $key ||= $obis_code;
    return $key, {
        obis_code => $obis_code,
        defined $factor ? ( value => $value * $factor ) : ( value => $value ),
        defined $desc ? ( description => $desc ) : (),
        defined $unit ? ( unit => $unit ) : (),
    };
}

# See https://www.nek.no/info-ams-han-utviklere/ for latest version of OBIS code documentation
sub get_obis_map {
    my ($meter_type) = @_;

    # AIDON_V0001 - 10.05.2016 - Aidon HAN Interface specification 1.1 A - tested with Aidon 6525
    return {
        "1-1:0.2.129.255" => [ "obis_version",          "OBIS list version identifier", ],
        "0-0:96.1.0.255"  => [ "meter_id",              "Meter ID (GIAI GS1)", ],
        "0-0:96.1.7.255"  => [ "meter_type",            "Meter type", ],
        "0-0:1.0.0.255"   => [ "meter_timestamp",       "Meter timestamp", ],

        "1-0:1.7.0.255"   => [ "power_active_import",   "Active power import (Q1+Q4)",                                'W',     1.0, ],
        "1-0:2.7.0.255"   => [ "power_active_export",   "Active power export (Q2+Q3)",                                'W',     1.0, ],
        "1-0:3.7.0.255"   => [ "power_reactive_import", "Reactive power import (Q1+Q2)",                              'VAr',   1.0, ],
        "1-0:4.7.0.255"   => [ "power_reactive_export", "Reactive power export (Q3+Q4)",                              'VAr',   1.0, ],

        "1-0:31.7.0.255"  => [ "phase_current_l1", "IL1 Current phase L1",                                            'A',     0.1, ],
        "1-0:51.7.0.255"  => [ "phase_current_l2", "IL2 Current phase L2",                                            'A',     0.1, ],
        "1-0:71.7.0.255"  => [ "phase_current_l3", "IL3 Current phase L3",                                            'A',     0.1, ],

        "1-0:32.7.0.255"  => [ "phase_voltage_l1", "UL1 Phase voltage 4W meter, line voltage 3W meter",               'V',     0.1, ],
        "1-0:52.7.0.255"  => [ "phase_voltage_l2", "UL2 Phase voltage 4W meter, line voltage 3W meter",               'V',     0.1, ],
        "1-0:72.7.0.255"  => [ "phase_voltage_l3", "UL3 Phase voltage 4W meter, line voltage 3W meter",               'V',     0.1, ],

        "1-0:1.8.0.255"   => [ "energy_active_cum_import",   "Cumulative hourly active import energy (A+) (Q1+Q4)",   'kWh',   0.01, ],
        "1-0:2.8.0.255"   => [ "energy_active_cum_export",   "Cumulative hourly active export energy (A-) (Q2+Q3)",   'kWh',   0.01, ],
        "1-0:3.8.0.255"   => [ "energy_reactive_cum_import", "Cumulative hourly reactive import energy (R+) (Q1+Q2)", 'kVArh', 0.01, ],
        "1-0:4.8.0.255"   => [ "energy_reactive_cum_export", "Cumulative hourly reactive export energy (R-) (Q3+Q4)", 'kVArh', 0.01, ],
    } if $meter_type eq 'AIDON_V0001';

    # Kamstrup_V0001 - 03.05.2016 - untested (incomplete)
    return {
        "1-1:0.0.5.255"   => [ "meter_id",              "Meter ID (GIAI GS1)", ],
        "1-1:96.1.1.255"  => [ "meter_type",            "Meter type", ],

        "1-1:1.7.0.255"   => [ "power_active_import",   "Active power import (Q1+Q4)",                                'W',     1.0, ],
        "1-1:2.7.0.255"   => [ "power_active_export",   "Active power export (Q2+Q3)",                                'W',     1.0, ],
    } if $meter_type eq 'Kamstrup_V0001';

    # KFM_001 - 09.11.2018 - untested (incomplete)
    return {
        "0-0:96.1.0.255"  => [ "meter_id",              "Meter ID (GIAI GS1)", ],
    } if $meter_type eq 'KFM_001';
    
    confess("Unsupported meter type specified");
};

1;
