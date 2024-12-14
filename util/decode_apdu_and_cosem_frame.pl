#!/usr/bin/env perl

use strict;
use warnings;

use JSON;
use Encode;

my $payload = pack('H*', shift);
my $apdu = decode_apdu($payload);
my $ds = decode_cosem_frame($payload, $apdu->{'cosem_offset'} );
my $coder = JSON->new->pretty->canonical->utf8;
print $coder->encode( $apdu ), "\n";
print $coder->encode( $ds ), "\n";

sub DEBUG { 1 }

sub decode_apdu {
    my ($frame) = @_;

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

    my $tag = $read_bytes->(1, 'C');                     # unsigned8
    my $invoke_id_and_priority = $read_bytes->(4, 'L>'); # unsigned32, big-endian
    my $datetime_len = $read_bytes->(1, 'C');           # unsigned8
    my $datetime_octets = $datetime_len > 0 ? $read_bytes->( $datetime_len ) : ''; # date+time, if present
    return {
        'apdu_tag'                    => scalar $tag,
        'apdu_invoke_id_and_priority' => scalar $invoke_id_and_priority,
        'apdu_datetime_length'        => scalar $datetime_len,
        'apdu_datetime_octets'        => scalar $datetime_octets,
        'apdu_datetime_decoded'       => scalar decode_datetime($datetime_octets),
        'cosem_offset'                => $index,
    };
}

sub decode_cosem_frame {
    my ($frame, $offset) = @_;

    #print STDERR "COSEM FRAME: " . ( " " x ( $offset + 13 + 2 ) ). as_hex($frame) . " (" . length($frame) . ")\n"
    #    if DEBUG;

    # Function to read next X bytes from frame, moves index forward and returns bytes
    my $index = $offset;
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

# Format a string of octets as hex numbers
sub as_hex {
    return unpack('H*', $_[0]);
}

sub decode_datetime {
    my ($value) = @_;
    return '' unless defined $value;
    return '' unless length $value;
    # clock value, DLMS page 35-37
    # big-endian, first and next-to-last value is 16-bit integer, rest are 8-bit integers
    my ($year, $month, $day, $dow, $hour, $min, $sec, $frac, $offset, $status) = unpack('s>CCC CCCC s>C', $value);
    # formatted as a string (almost ISO format)
    return sprintf('%u-%02u-%02u %02u:%02u:%02u,%u %+d (%b)', $year, $month, $day, $hour, $min, $sec, $frac, $offset, $status);
    # formatted as an arrayref (pay attention to the day-of-week in the middle and status at the end)
    #return [ $year, $month, $day, $dow, $hour, $min, $sec, $frac, $offset, $status ];
}
