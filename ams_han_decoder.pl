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
# Also tested with a Kaifa MA304H4D and packom.net M-Bus Master Hat for the Raspberry Pi:
# https://www.packom.net/m-bus-master-hat/
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

use strict;
use warnings;
use feature 'state';

use JSON ();
use Encode ();
use Digest::CRC ();
use Carp qw(confess);
use Getopt::Std;
use URI ();

STDOUT->autoflush(1);
STDERR->autoflush(1);

my $opts = {};
getopts('cdhkm:p:qit:ax:', $opts);

if ( $opts->{'h'} or not $opts->{'m'} ) {
    print STDERR <<"EOM";
Usage: $0 [options] [<file|device>]
    -m OBIS code mapping table (required)
    -t MQTT server to send messages to
    -a Enable Home Assistant MQTT discovery
    -x Home Assistant MQTT discovery prefix
    -p Program to pipe each JSON message to
    -k Don't close program (-p) after each sent message
    -c Compact JSON output (one meter reading per line)
    -d Show debug information
    -i Ignore checksum errors
    -q Show as little information as possible
    -h This help

If you specify a character device, stty will be run to configure its serial
settings. 2400 8E1 is the default serial settings. Edit the script if you
need something else. If you don't specify a file or device, standard input
will be opened and used.

An OBIS code mapping table must be specified. The currently supported values
are as follows: AIDON_V0001, Kamstrup_V0001, KFM_001

You can also set the environment variable AMS_OBIS_MAP. If both are set, the
command-line option takes precedence.

You can also set the environment variable AMS_HA_PREFIX. If both are set,
the command-line option takes precedence. Default value is 'homeassistant'.

If the environment variable MQTT_SERVER is set, it is used to set the -t
parameter. If bot are set, the command-line option takes precedence.

The path part of the MQTT server variable is used to set the MQTT topic
prefix. Default value is '/ams'.
EOM
    exit 1;
}

my $is_compact = $opts->{'c'} ? 1 : 0;
my $is_pretty  = $opts->{'c'} ? 0 : 1;
my $json_coder = JSON->new->canonical->utf8;
$json_coder->pretty() if $is_pretty;
my $mqtt_url = mqtt_url();
my $mqtt_topic_prefix = get_mqtt_topic_prefix( $mqtt_url );
my $mqtt = get_mqtt( $mqtt_url );
my $obis_map = get_obis_map( meter_type() );
my $unit_map = get_unit_map();

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

sub die_on_checksum_error {
    return $opts->{'i'} ? 0 : 1;
}

sub meter_type {
    return $opts->{'m'} // $ENV{'AMS_OBIS_MAP'};
}

sub ha_prefix {
    return $opts->{'x'} // $ENV{'AMS_HA_PREFIX'} // 'homeassistant';
}

sub mqtt_url {
    my $url = $opts->{'t'} // $ENV{'MQTT_SERVER'};
    return unless $url;
    return URI->new($url)->canonical;
}

sub get_mqtt_topic_prefix {
    my ($url) = @_;
    return unless $url;
    my $path = $url->path || '/ams';
    $path =~ s{^/*}{};
    $path =~ s{/*$}{};
    return $path;
}

sub require_module {
    my ($class) = @_;
    my $module_path = $class;
    $module_path =~ s!::!/!g;
    $module_path .= '.pm';
    return require $module_path;
}

sub get_mqtt {
    my ($url) = @_;
    return unless $url;
    my $class = $url->scheme eq 'mqtt' ? 'Net::MQTT::Simple'
              : $url->scheme eq 'mqtts' ? 'Net::MQTT::Simple::SSL'
              : '';
    return unless $class;
    require_module($class);
    my $mqtt = $class->new( $url->host );
    $mqtt->login( split /:/, $url->userinfo ) if $url->userinfo;
    return $mqtt;
}

sub get_pipe {
    my $program = $opts->{'p'};
    return unless $program;
    state $pipe;
    state $child_pid;
    return $pipe, $child_pid if defined $pipe and $opts->{'k'};
    $child_pid = open($pipe, '|-', $program) // confess("Can't pipe to $program: $!");
    binmode $pipe, ':raw';
    $pipe->autoflush(1);
    return $pipe, $child_pid;
}

sub maybe_close_pipe {
    my ($pipe, $child_pid) = @_;
    return if $opts->{'k'};
    close $pipe;
    waitpid $child_pid, 0;
}

sub send_json {
    my ($ds) = @_;
    my $json = $json_coder->encode($ds);
    $json .= "\n" if $is_compact;
    my $fallback = 1;
    if ( $opts->{'p'} ) {
        my ($pipe, $child_pid) = get_pipe();
        print $pipe $json;
        maybe_close_pipe($pipe, $child_pid);
        $fallback = 0;
    }
    if ( $mqtt ) {
        my $topic = join('/',
            $mqtt_topic_prefix,
            $ds->{'header'}->{'hdlc_addr_server'},
            $ds->{'header'}->{'hdlc_addr_client'},
        );
        foreach my $key ( sort keys %{ $ds->{'data'} } ) {
            configure_ha_mqtt_sensor(
                scalar $topic,
                scalar $key,
                scalar $ds->{'data'}->{$key},
            ) if $opts->{'a'};
            foreach my $k2 ( sort keys %{ $ds->{'data'}->{$key} } ) {
                my $t = join('/', $topic, $key, $k2);
                my $v = $ds->{'data'}->{$key}->{$k2};
                $mqtt->retain($t, $v);
            }
        }
        $fallback = 0;
    }
    if ( $fallback ) {
        print $json;
    }
    return 1;
}

sub configure_ha_mqtt_sensor {
    my ($device, $sensor, $ds) = @_;
    my $node_id = $device;
    $node_id =~ s{/}{_}g;
    my $device_name = uc($device);
    $device_name =~ s{/}{ }g;
    my $object_id = join('_', $node_id, $sensor);
    my $topic = join('/',
        ha_prefix(),
        'sensor',
        $node_id,
        $object_id,
        'config',
    );
    state $configured = {};
    return if $configured->{$topic};
    my $state_topic = join('/', $device, $sensor, 'value');
    my @state_class = (
        $sensor =~ m/_cum_/     ? ( 'state_class' => 'total_increasing' )
      : $sensor =~ m/phase_/    ? ( 'state_class' => 'measurement' )
      : $sensor =~ m/power_/    ? ( 'state_class' => 'measurement' )
                : ()
    );
    my @device_class = (
        $sensor =~ m/^power_/         ? ( 'device_class' => 'power' )
      : $sensor =~ m/^phase_current_/ ? ( 'device_class' => 'current' )
      : $sensor =~ m/^phase_voltage_/ ? ( 'device_class' => 'voltage' )
      : $sensor =~ m/^energy_/        ? ( 'device_class' => 'energy' )
      : ()
    );
    my @enabled = (
        ( $sensor =~ m/reactive_/ or not $ds->{'unit'} )
        ? (  'enabled_by_default' => \0 )
        : ()
    );
    my $device_ids = [
        $node_id,
        ( $sensor eq 'meter_id' ? $ds->{'value'} : () ),
    ];
    my @device_model = (
        $sensor eq 'meter_type'
      ? ( 'model' => $ds->{'value'} )
      : ()
    );
    my @device_manufacturer = (
        $sensor eq 'obis_version'
      ? ( 'manufacturer' => (split /_/, $ds->{'value'}, 2)[0] )
      : ()
    );
    my @device_sw_version = (
        $sensor eq 'obis_version'
      ? ( 'sw_version' => (split /_/, $ds->{'value'}, 2)[1] )
      : ()
    );
    my $config = {
        'unique_id' => $object_id,
        'device'    => {
            'identifiers' => $device_ids,
            @device_manufacturer,
            @device_model,
            'name'        => $device_name,
            @device_sw_version,
        },
        'name' => join(' ', $device_name, $ds->{'description'} ),
        ( $ds->{'unit'}
          ? (
              'unit_of_measurement' => $ds->{'unit'}
            )
          : ()
        ),
        'state_topic' => $state_topic,
        @device_class,
        @state_class,
        @enabled,
    };
    $mqtt->retain( $topic, $json_coder->encode($config) );
    $configured->{$topic} = 1;
    return 1;
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
            print STDERR as_hex($flag) if DEBUG;
            next;
        }
        print STDERR "\n" . as_hex($flag) . " (flag:$rc) "
            if DEBUG;
        $rc = $read_stream->(1, my $frame_format);
        last unless $rc;
        # Start frame flag was end of frame flag, so check again if start
        # frame flag was found instead of frame format value
        if ( unpack('C', $frame_format) == 0x7e ) {
            print STDERR as_hex($frame_format) . " (frame-format-as-flag:$rc) "
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
        print STDERR as_hex($frame_format) . " (frame format:$rc) "
            if DEBUG;
        my ($length, $segmentation, $type)= decode_hdlc_frame_format($frame_format);
        next unless defined $length and $length > 2;
        next unless defined $segmentation;
        next unless defined $type;
        $rc = $read_stream->( $length - 2, my $frame );
        last unless $rc;
        print STDERR "\n" if DEBUG;
        eval { decode_hdlc_frame($frame_format . $frame, $length, $segmentation, $type); };
        print STDERR "Decoding HDLC frame failed: $@"
            if $@ and not QUIET;
    }

    print STDERR "read from stream failed: $!\n" unless defined $rc;
    return;
}

# Format a string of octets as hex numbers
sub as_hex {
    return unpack('H*', $_[0]);
}

# CRC-16/X-25 according to https://crccalc.com/ (the "Check" value 0x906E is not used)
sub calc_checksum {
    my ($str) = @_;
    my $crc = Digest::CRC->new(
        width  => 16,
        poly   => 0x1021,
        init   => 0xFFFF,
        xorout => 0xFFFF,
        refout => 1,
        refin  => 1,
        cont   => 0, # not sure what this means, but false seems to do what we want
    );
    $crc->add($str);
    return $crc->digest;
}

# HDLC frame format: 2 bytes, big-endian unsigned 16-bit integer
# MSB: | Type(4) | Segmentation(1) | Length(11) | :LSB
sub decode_hdlc_frame_format {
    my ($frame_format) = @_;
    return if length $frame_format != 2;
    my $value = unpack('S>', $frame_format); # unsigned16, big-endian
    my $length       = $value & 0b0000_0111_1111_1111 >> 0;
    my $segmentation = $value & 0b0000_1000_0000_0000 >> 11;
    my $type         = $value & 0b1111_0000_0000_0000 >> 12;
    return ( $length, $segmentation, $type );
}

# Format of binary messages are documented in Excerpt GB8, pages 48 and onward
# HDLC frame format type 3 (Annex H.4 of ISO/IEC 13239) - not really type 3 (from Aidon 6525)
# | 1B   | 2B             | multiple B    | multiple B   | 1B      | 2B  | multiple B  | 2B  | 1B   |
# | Flag | Frame format   | Dest. address | Src. address | Control | HCS | Payload     | FCS | Flag |
# | 7E   |                |               |              | 13      |     |             |     | 7E   |
sub decode_hdlc_frame {
    my ($frame, $length, $segmentation, $type) = @_;
    return unless defined $frame;
    return unless length $frame >= 7; # that's the minimum frame length (without start/stop flag)

    # Function to read next X bytes from frame, moves index forward and returns bytes
    my $index = 0;
    my $read_bytes = sub {
        my ($len, $unpack_template) = @_;
        my $bytes = substr($frame, $index, $len);
        confess("Read bytes doesn't match requested length")
            if length($bytes) != $len;
        $index += $len;
        return unpack($unpack_template, $bytes) if $unpack_template;
        return $bytes;
    };

    my @fields;

    # Function to store named binary string as a field, while decoding hex
    # and numeric value using unpack template
    my $add_field = sub {
        my ($name, $raw, $unpack_template) = @_;
        my $rec = {
            raw  => $raw,
            name => $name,
            hex  => as_hex($raw),
            $unpack_template ? ( value => unpack($unpack_template, $raw) ) : (),
        };
        push @fields, $rec;
    };

    # Function to read an HDLC address (variable byte encoding)
    my $read_hdlc_addr = sub {
        my ($name) = @_;
        my $raw_addr = "";
        while (1) {
            my $raw = $read_bytes->(1);
            my $value = unpack('C', $raw);
            $raw_addr .= $raw;
            last if $value % 2 == 1; # odd number means last byte
        }
        $add_field->("hdlc_addr_$name", $raw_addr);
    };

    # Function that reads specified number of bits (power of 2) into named
    # field using optional unpack template
    my $read_bits = sub {
        my ($bits, $name, $unpack_template) = @_;
        $bits //= 8;
        $name //= 'unknown';
        my $raw = $read_bytes->($bits / 8);
        $add_field->($name, $raw, $unpack_template);
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

    # HDLC_START_FLAG HDLC_FRAME_FORMAT HDLC_ADDR_CLIENT HDLC_ADDR_SERVER HDLC_HCS LLC_DSAP/LLC_SSAP/LLC_CONTROL
    #     APDU_TAG APDU_INVOKE_ID_AND_PRIORITY APDU_DATETIME_LENGTH [APDU_DATETIME_OCTETS (only if length is non-zero)]
    #     COSEM_DATA_TYPE(S)...
    # HDLC_FCS HDLC_END_FLAG

    # HDLC and LLC is specified in chapter 8 of Excerpt_GB8 (pages 45-53)
    # APDU format briefly explained in Aidon HAN Interface specification 1.1A (page 9)
    # COSEM interface classes are explained in Excerpt_BB12 (pages 30-33)
    # COSEM registers (class_id = 3, as used here) are explained in Excerpt_BB12 (pages 48-51)
    # COSEM OBIS codes are explained in Excerpt_BB12 (pages 142-149)
    # Electrical OBIS codes are explained in Excerpt_BB12 (pages 129-134, 156-166)

    print STDERR "HDLC FRAME: " . as_hex($frame) . " (" . length($frame) . ")\n"
        if DEBUG;

    # Verify frame checksum is correct, before we try to mess around with it
    my $fcs = unpack('S<', substr($frame, -2, 2) ); # last two bytes of message, unsigned int 16, little-endian
    my $fcs_calc = calc_checksum( substr($frame, 0, -2) ); # entire message except checksum
    confess(
        sprintf("Calculated frame checksum %04X doesn't match specified frame checksum %04X",
            $fcs_calc,
            $fcs,
        )
    ) if die_on_checksum_error() and $fcs != $fcs_calc;

    # Line 1 (HDLC header)
    $read_bits->(16, 'hdlc_frame_format', 'S>'); # unsigned16, big-endian
    $read_hdlc_addr->('client');
    $read_hdlc_addr->('server');
    $read_bits->(8,  'hdlc_control', 'C');

    # Validate header checksum
    my $hdlc_header = substr($frame, 0, $index); # the parts of the frame, as of now
    $read_bits->(16, 'hdlc_hcs', 'S<'); # unsigned16, little-endian
    my $hcs = $fields[-1]->{'value'};
    my $hcs_calc = calc_checksum($hdlc_header);
    confess(
        sprintf("Calculated header checksum %04X doesn't match specified header checksum %04X",
            $hcs_calc,
            $hcs,
        )
    ) if die_on_checksum_error() and $hcs != $hcs_calc;

    # Read rest of header
    $read_bits->(8,  'llc_dst_svc_ap'); # LLC PDU, see GB8 page 47, always 0xE6
    $read_bits->(8,  'llc_src_svc_ap'); # LLC PDU, see GB8 page 47, always 0xE6 or 0xe/
    $read_bits->(8,  'llc_control'); # LLC PDU, see GB8 page 47, always 0x00 (reserved)

    # Line 2 (APDU not encrypted)
    $read_bits->(8,  'apdu_tag');
    $read_bits->(32, 'apdu_invoke_id_and_priority');

    # Line 2 and following (COSEM payload)
    my $payload = substr($frame, $index, -2); # the rest of the message is COSEM data
    my $cosem = decode_cosem_frame($payload, $index);
    $index += length $payload;

    # Last line (HDLC frame checksum)
    $read_bits->(16, 'hdlc_fcs', 'S<'); # unsigned int 16, little-endian (verified earlier)

    # This prints the remaining bytes of the message that has not yet been decoded (if any)
    print STDERR "REMAIN:" . ( " " x ($index * 2) ) . as_hex( substr($frame, $index) ) . "\n"
        if DEBUG and $index != length($frame);

    # Output frame information as JSON
    return send_json({
        'header' => {
            'hdlc_length'       => $length,
            'hdlc_segmentation' => $segmentation,
            'hdlc_type'         => $type,
            (
                map { $_->{'name'} => $_->{'hex'} }
                @fields
            )
        },
        'payload' => $cosem,
        'data' => decode_cosem_structure($cosem, $type),
    });
}

sub decode_cosem_frame {
    my ($frame, $offset) = @_;

    print STDERR "COSEM FRAME: " . ( " " x ( $offset + 13 + 2 ) ). as_hex($frame) . " (" . length($frame) . ")\n"
        if DEBUG;

    # Function to read next X bytes from frame, moves index forward and returns bytes
    my $index = 0;
    my $read_bytes = sub {
        my ($len, $unpack_template) = @_;
        my $bytes = substr($frame, $index, $len);
        confess("Read bytes doesn't match requested length")
            if length($bytes) != $len;
        $index += $len;
        return unpack($unpack_template, $bytes) if $unpack_template;
        return $bytes;
    };

    my $func_map = [];

    # Function to read a single byte as a datatype and execute function according to map
    my $read_datatype = sub {
        my $datatype = $read_bytes->(1, 'C'); # unsigned8
        unless ( defined $datatype ) {
            print STDERR sprintf("No datatype read at index %d.\n", $index - 1)
                if DEBUG;
            return undef;
        }
        my $func = $func_map->[$datatype];
        unless ( ref $func eq ref sub {} ) {
            print STDERR sprintf("No function found for datatype %02X at index %d.\n", $datatype, $index - 1)
                if DEBUG;
            return undef;
        }
        return $func->();
    };

    my $read_len = sub {
        return $read_bytes->(1, 'C'); # unsigned8
    };

    # COSEM data type reference is shown in Excerpt_BB12 (pages 34-38)
    # (LL in the table below means a single byte describing the length of following data, in bytes)

    # 0x01LL - array                (just an arrayref)
    # 0x02LL - structure            (just an arrayref)
    # 0x09LL - octet-string         (binary bytes)
    # 0x0aLL - visible-string       (ascii bytes)
    # 0x16   - enum                 (unsigned8)
    # 0x12   - long unsigned        (unsigned16)
    # 0x06   - double-long-unsigned (unsigned32)
    # 0x0f   - integer signed       (integer8)
    # 0x10   - long signed          (integer16)

    # arrayref
    $func_map->[0x01] = sub {
        my $len = $read_len->();
        my @elements;
        for ( 1..$len ) {
            push @elements, scalar $read_datatype->();
        }
        return \@elements;
    };

    # arrayref
    $func_map->[0x02] = sub {
        my $len = $read_len->();
        my @elements;
        for ( 1..$len ) {
            push @elements, scalar $read_datatype->() ;
        }
        return \@elements;
    };

    # utf8 characters
    $func_map->[0x0c] = sub {
        my $len = $read_len->();
        my $str = "";
        my $buffer = "";
        for (1..$len) {
            $buffer = $read_bytes->(1);
            my $char = "";
            my $runaway = 0;
            while ( length $buffer > 0 ) {
                $char = Encode::decode('UTF-8', $buffer, Encode::FB_QUIET);
                last if $runaway > 10;
                $runaway++;
            }
            $str .= $char;
        }
        return $str;
    };

    $func_map->[0x09] = sub { return $read_bytes->( $read_len->() ); }; # binary bytes (octets)
    $func_map->[0x0a] = sub { return $read_bytes->( $read_len->() ); }; # ascii bytes
    $func_map->[0x16] = sub { return $read_bytes->(1, 'C'); };  # unsigned8
    $func_map->[0x12] = sub { return $read_bytes->(2, 'S>'); }; # unsigned16, big-endian
    $func_map->[0x06] = sub { return $read_bytes->(4, 'L>'); }; # unsigned32, big-endian
    $func_map->[0x0f] = sub { return $read_bytes->(1, 'c'); };  # integer8
    $func_map->[0x10] = sub { return $read_bytes->(2, 's>'); }; # integer16, big-endian
    $func_map->[0x00] = sub { return undef; };                  # null

    my @items;
    while ( $index < length $frame ) {
        push @items, scalar $read_datatype->();
    }
    return \@items;
}

sub decode_cosem_structure {
    my ($cosem, $hdlc_type) = @_;
    my @items = @$cosem;
    if ( meter_type() eq 'AIDON_V0001' ) {
        my $timestamp = shift @items;
        my $ds = shift @items;
        return {} unless ref $ds eq ref [];
        return {
            map { convert_register($_) }
            grep { ref $_ eq ref [] }
            @$ds
        };
    }
    if ( meter_type() eq 'Kamstrup_V0001' ) {
        my $timestamp = shift @items;
        my $ds = shift @items;
        return {} unless ref $ds eq ref [];
        my @elements = @$ds;
        unshift @elements, encode_obis_code(1,1,0,2,129,255);
        my @out;
        for ( my $i = 0; $i < @elements; $i += 2 ) {
            push @out, [ $elements[$i], $elements[$i+1] ];
        };
        return {
            map { convert_register($_) }
            grep { ref $_ eq ref [] }
            @out
        };
    }
    if ( meter_type() eq 'KFM_001' ) {
        my $timestamp = shift @items;
        my $ds = shift @items;
        return {} unless ref $ds eq ref [];
        my @values = @$ds;
        my @keys;
        # List 1
        if ( $hdlc_type == 7 ) {
            @keys = (
                encode_obis_code(1,0,1,7,0,255), # 1
            );
        }
        # List 2 and list 3
        # Type 11 seen on MA304H4D
        if ( $hdlc_type == 8 or $hdlc_type == 9 or $hdlc_type == 10 or $hdlc_type == 11 ) {
            @keys = (
                encode_obis_code(1,1,0,2,129,255), # 2
                encode_obis_code(0,0,96,1,0,255),  # 3
                encode_obis_code(0,0,96,1,7,255),  # 4

                encode_obis_code(1,0,1,7,0,255),   # 5
                encode_obis_code(1,0,2,7,0,255),   # 6

                encode_obis_code(1,0,3,7,0,255),   # 7
                encode_obis_code(1,0,4,7,0,255),   # 8

                encode_obis_code(1,0,31,7,0,255),   # 9
                encode_obis_code(1,0,51,7,0,255),   # 10
                encode_obis_code(1,0,71,7,0,255),   # 11

                encode_obis_code(1,0,32,7,0,255),   # 12
                encode_obis_code(1,0,52,7,0,255),   # 13
                encode_obis_code(1,0,72,7,0,255),   # 14
            );
        }
        # List 3 (appended)
        # Type 11 seen on MA304H4D
        if ( $hdlc_type == 10 or $hdlc_type == 11 ) {
            push @keys, (
                encode_obis_code(0,0,1,0,0,255),    # 15

                encode_obis_code(1,0,1,8,0,255),    # 16
                encode_obis_code(1,0,2,8,0,255),    # 17
                encode_obis_code(1,0,3,8,0,255),    # 18
                encode_obis_code(1,0,4,8,0,255),    # 19
            );
        }
        my @out;
        for (my $i = 0; $i < @values; $i++) {
            push @out, [ $keys[$i], $values[$i] ];
        }
        return {
            map { convert_register($_) }
            grep { ref $_ eq ref [] }
            @out
        };
    }
    return {};
}

sub encode_obis_code {
    return pack('C*', @_);
}

sub decode_obis_code {
    my ($str) = @_;
    return sprintf('%d-%d:%d.%d.%d.%d', unpack('C*', $str) );
}

sub convert_register {
    my ($register) = @_;
    return unless defined $register;
    return unless ref $register eq ref [];
    return if scalar @$register < 2;

    my $obis_code = decode_obis_code( $register->[0] );
    my $value = $register->[1];
    my $scaler_unit = $register->[2];

    if ( $obis_code eq '0-0:1.0.0.255' ) {
        # clock value, DLMS page 35-37
        # big-endian, first and next-to-last value is 16-bit integer, rest are 8-bit integers
        my ($year, $month, $day, $dow, $hour, $min, $sec, $frac, $offset, $status) = unpack('s>CCC CCCC s>C', $value);
        # formatted as a string (almost ISO format)
        $value = sprintf('%u-%02u-%02u %02u:%02u:%02u,%u %+d (%b)', $year, $month, $day, $hour, $min, $sec, $frac, $offset, $status);
        # formatted as an arrayref (pay attention to the day-of-week in the middle and status at the end)
        #$value = [ $year, $month, $day, $dow, $hour, $min, $sec, $frac, $offset, $status ];
    }

    # Handle scaler unit, if present
    my $factor;
    my $unit_value;
    my $unit;
    if ( ref $scaler_unit eq ref [] ) {
        $factor = 10 ** $scaler_unit->[0];
        $unit_value = $scaler_unit->[1];
        $unit = $unit_map->[$unit_value];
    }

    # Lookup obis meta info
    my $meta = $obis_map->{$obis_code} // [];
    my ($key, $desc, $unit_meta, $factor_meta) = @$meta;
    $unit //= $unit_meta;
    $factor //= $factor_meta;
    $key ||= $obis_code;

    return $key, {
        obis_code => $obis_code,
        defined $factor ? ( value => $value * $factor ) : ( value => $value ),
        defined $desc ? ( description => $desc ) : (),
        defined $unit ? ( unit => $unit ) : (),
    };
}

# Explained in Excerpt_BB12 (pages 49-50)
sub get_unit_map {
    my $units = [
        undef,
        'a', # 1
        'mo',
        'wk',
        'd',
        'h',
        'min.',
        's',
        '°',
        '°C',
        'currency', # 10
        'm',
        'm/s',
        'm3',
        'm3',
        'm3/h',
        'm3/h',
        'm3/d',
        'm3/d',
        'l',
        'kg', # 20
        'N',
        'Nm',
        'Pa',
        'bar',
        'J',
        'J/h',
        'W',
        'VA',
        'VAr',
        'Wh', # 30
        'VAh',
        'VArh',
        'A',
        'C',
        'V',
        'V/m',
        'F',
        '',
        'm2/m',
        'Wb', # 40
        'T',
        'A/m',
        'H',
        'Hz',
        '1/(Wh)',
        '1/(VArh)',
        '1/(VAh)',
        'V2h',
        'A2h',
        'kg/s', # 50
        'S, mho',
        'K',
        '1/(V2h)',
        '1/(A2h)',
        '1/m3',
        '%',
        'Ah',
    ];
    $units->[60] = 'Wh/m3';
    $units->[61] = 'J/m3';
    $units->[62] = 'Mol %';
    $units->[63] = 'g/m3';
    $units->[64] = 'Pa s';
    $units->[65] = 'J/kg';
    $units->[70] = 'dBm';
    $units->[71] = 'dbμV';
    $units->[72] = 'dB';
    $units->[253] = 'reserved';
    $units->[254] = 'other';
    $units->[255] = '';
    return $units;
}

# See https://www.nek.no/info-ams-han-utviklere/ for latest version of OBIS code documentation
sub get_obis_map {
    my ($type) = @_;

    # A: Identifies the media (energy type) to which the metering is related.
    #    Non-media related information is handled as abstract data.
    # B: Generally, identifies the measurement channel number, i.e.  the number
    #    of the input of a metering equipment having several inputs for the
    #    measurement of energy of the same or different types (for example in
    #    data concentrators, registration units).  Data from different sources
    #    can thus be identified.  It may also identify the communication
    #    channel, and in some cases it may identify other elements.  The
    #    definitions for this value group are independent from the value group A.
    # C: Identifies abstract or physical data items related to the information
    #    source concerned, for example current, voltage, power, volume,
    #    temperature.  The definitions depend on the value in the value group A.
    #    Further processing, classification and storage methods are defined by
    #    value groups D, E and F.  For abstract data, value groups D to F
    #    provide further classification of data identified by value groups A to C.
    # D: Identifies types, or the result of the processing of physical
    #    quantities identified by values in value groups A and C, according to
    #    various specific algorithms.  The algorithms can deliver energy and demand
    #    quantities as well as other physical quantities.
    # E: Identifies further processing or classification of quantities
    #    identified by values in value groups A to D.
    # F: Identifies historical values of data, identified by values in value
    #    groups A to E, according to

    # OBIS codes common to all meters
    my %common = (
        "0-0:1.0.0.255"   => [ "meter_timestamp",       "Meter timestamp", ],

        "1-1:0.2.129.255" => [ "obis_version",          "OBIS list version identifier", ],

        "1-0:1.7.0.255"   => [ "power_active_import",   "Active power import (Q1+Q4)",                                'W',     1.0, ],
        "1-0:2.7.0.255"   => [ "power_active_export",   "Active power export (Q2+Q3)",                                'W',     1.0, ],
    );

    # AIDON_V0001 - 10.05.2016 - Aidon HAN Interface specification 1.1 A - tested with Aidon 6525
    return {

        %common,

        "0-0:96.1.0.255"  => [ "meter_id",              "Meter ID (GIAI GS1)", ],
        "0-0:96.1.7.255"  => [ "meter_type",            "Meter type", ],

        "1-0:3.7.0.255"   => [ "power_reactive_import", "Reactive power import (Q1+Q2)",                              'VAr',   1.0, ],
        "1-0:4.7.0.255"   => [ "power_reactive_export", "Reactive power export (Q3+Q4)",                              'VAr',   1.0, ],

        "1-0:31.7.0.255"  => [ "phase_current_l1", "IL1 Current phase L1",                                            'A',     0.1, ],
        "1-0:51.7.0.255"  => [ "phase_current_l2", "IL2 Current phase L2",                                            'A',     0.1, ],
        "1-0:71.7.0.255"  => [ "phase_current_l3", "IL3 Current phase L3",                                            'A',     0.1, ],

        "1-0:32.7.0.255"  => [ "phase_voltage_l1", "UL1 Phase voltage 4W meter, line voltage 3W meter",               'V',     0.1, ],
        "1-0:52.7.0.255"  => [ "phase_voltage_l2", "UL2 Phase voltage 4W meter, line voltage 3W meter",               'V',     0.1, ],
        "1-0:72.7.0.255"  => [ "phase_voltage_l3", "UL3 Phase voltage 4W meter, line voltage 3W meter",               'V',     0.1, ],

        "1-0:1.8.0.255"   => [ "energy_active_cum_import",   "Cumulative hourly active import energy (A+) (Q1+Q4)",   'kWh',   0.00001, ],
        "1-0:2.8.0.255"   => [ "energy_active_cum_export",   "Cumulative hourly active export energy (A-) (Q2+Q3)",   'kWh',   0.00001, ],
        "1-0:3.8.0.255"   => [ "energy_reactive_cum_import", "Cumulative hourly reactive import energy (R+) (Q1+Q2)", 'kVArh', 0.00001, ],
        "1-0:4.8.0.255"   => [ "energy_reactive_cum_export", "Cumulative hourly reactive export energy (R-) (Q3+Q4)", 'kVArh', 0.00001, ],

    } if $type eq 'AIDON_V0001';

    # Kamstrup_V0001 - 03.05.2016
    return {

        %common,

        "1-1:0.0.5.255"   => [ "meter_id",              "Meter ID (GIAI GS1)", ],
        "1-1:96.1.1.255"  => [ "meter_type",            "Meter type", ],

        "1-1:3.7.0.255"   => [ "power_reactive_import", "Reactive power import (Q1+Q2)",                              'VAr',   0.001, ],
        "1-1:4.7.0.255"   => [ "power_reactive_export", "Reactive power export (Q3+Q4)",                              'VAr',   0.001, ],

        "1-1:31.7.0.255"  => [ "phase_current_l1", "IL1 Current phase L1",                                            'A',     0.01, ],
        "1-1:51.7.0.255"  => [ "phase_current_l2", "IL2 Current phase L2",                                            'A',     0.01, ],
        "1-1:71.7.0.255"  => [ "phase_current_l3", "IL3 Current phase L3",                                            'A',     0.01, ],

        "1-1:32.7.0.255"  => [ "phase_voltage_l1", "UL1 Phase voltage 4W meter, line voltage 3W meter",               'V',     1.0, ],
        "1-1:52.7.0.255"  => [ "phase_voltage_l2", "UL2 Phase voltage 4W meter, line voltage 3W meter",               'V',     1.0, ],
        "1-1:72.7.0.255"  => [ "phase_voltage_l3", "UL3 Phase voltage 4W meter, line voltage 3W meter",               'V',     1.0, ],

        "1-0:1.8.0.255"   => [ "energy_active_cum_import",   "Cumulative hourly active import energy (A+) (Q1+Q4)",   'kWh',   0.00001, ],
        "1-0:2.8.0.255"   => [ "energy_active_cum_export",   "Cumulative hourly active export energy (A-) (Q2+Q3)",   'kWh',   0.00001, ],
        "1-0:3.8.0.255"   => [ "energy_reactive_cum_import", "Cumulative hourly reactive import energy (R+) (Q1+Q2)", 'kVArh', 0.00001, ],
        "1-0:4.8.0.255"   => [ "energy_reactive_cum_export", "Cumulative hourly reactive export energy (R-) (Q3+Q4)", 'kVArh', 0.00001, ],

    } if $type eq 'Kamstrup_V0001';

    # KFM_001 - 09.11.2018
    return {

        %common,

        "0-0:96.1.0.255"  => [ "meter_id",              "Meter ID (GIAI GS1)", ],
        "0-0:96.1.7.255"  => [ "meter_type",            "Meter type", ],

        "1-0:3.7.0.255"   => [ "power_reactive_import", "Reactive power import (Q1+Q2)",                              'VAr',   1.0, ],
        "1-0:4.7.0.255"   => [ "power_reactive_export", "Reactive power export (Q3+Q4)",                              'VAr',   1.0, ],

        "1-0:31.7.0.255"  => [ "phase_current_l1", "IL1 Current phase L1",                                            'A',     0.001, ],
        "1-0:51.7.0.255"  => [ "phase_current_l2", "IL2 Current phase L2",                                            'A',     0.001, ],
        "1-0:71.7.0.255"  => [ "phase_current_l3", "IL3 Current phase L3",                                            'A',     0.001, ],

        "1-0:32.7.0.255"  => [ "phase_voltage_l1", "UL1 Phase voltage 4W meter, line voltage 3W meter",               'V',     0.1, ],
        "1-0:52.7.0.255"  => [ "phase_voltage_l2", "UL2 Phase voltage 4W meter, line voltage 3W meter",               'V',     0.1, ],
        "1-0:72.7.0.255"  => [ "phase_voltage_l3", "UL3 Phase voltage 4W meter, line voltage 3W meter",               'V',     0.1, ],

        "1-0:1.8.0.255"   => [ "energy_active_cum_import",   "Cumulative hourly active import energy (A+) (Q1+Q4)",   'kWh',   0.001, ],
        "1-0:2.8.0.255"   => [ "energy_active_cum_export",   "Cumulative hourly active export energy (A-) (Q2+Q3)",   'kWh',   0.001, ],
        "1-0:3.8.0.255"   => [ "energy_reactive_cum_import", "Cumulative hourly reactive import energy (R+) (Q1+Q2)", 'kVArh', 0.001, ],
        "1-0:4.8.0.255"   => [ "energy_reactive_cum_export", "Cumulative hourly reactive export energy (R-) (Q3+Q4)", 'kVArh', 0.001, ],

    } if $type eq 'KFM_001';

    confess("Unsupported OBIS code mapping table specified: $type");
};

# https://github.com/mqtt/mqtt.org/wiki/URI-Scheme
package URI::mqtt;

use strict;
use warnings;

our $VERSION = '1.00';

use parent 'URI::http';

sub default_port { 1883 }

package URI::mqtts;

use strict;
use warnings;

our $VERSION = '1.00';

use parent 'URI::http';

sub default_port { 8883 }

sub secure { 1 }

1;
