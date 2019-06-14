#!/usr/bin/env perl

use strict;
use warnings;

STDOUT->autoflush(1);
STDERR->autoflush(1);

my $file = shift @ARGV;
if ( defined $file and -r $file ) {
    configure_serial_port($file);
    open my $fh, '<', $file or die("Can't open $file: $!\n");
    parse_stream($fh);
    close($fh);
}
else {
    parse_stream(*STDIN);
}
exit;

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

    binmode $stream, ':raw';

    my $frame = "";
    my $char;
    my $rc;

    while ( $rc = read($stream, $char, 1) ) {
        my $value = ord($char);
        my $prev_value = ord(substr($frame, -1, 1));
        # Start of "List 1" message
        # Previous value is first part of new message
        # Current value is second part of new message
        if ( $prev_value == 0x7e and $value == 0xa0 ) {
            my $new_frame = substr($frame, -1, 1);
            my $old_frame = substr($frame, 0, -1);
            handle_frame($old_frame);
            $frame = $new_frame . $char;
            next;
        }
        # Start of "List 2" message
        # Previous value is first part of new message
        # Current value is second part of new message
        if ( $prev_value == 0x7e and $value == 0xa1 ) {
            my $new_frame = substr($frame, -1, 1);
            my $old_frame = substr($frame, 0, -1);
            handle_frame($old_frame);
            $frame = $new_frame . $char;
            next;
        }
        # Start of "List 3" message (unconfirmed)
        # Previous value is first part of new message
        # Current value is second part of new message
        if ( $prev_value == 0x7e and $value == 0xa2 ) {
            my $new_frame = substr($frame, -1, 1);
            my $old_frame = substr($frame, 0, -1);
            handle_frame($old_frame);
            $frame = $new_frame . $char;
            next;
        }
        $frame .= $char;
    }

    print STDERR "read failed: $!\n" unless defined $rc;
    return 1;
}

# Format of binary messages are documented in Excerpt GB8, pages 48 and onward
# HDLC frame format type 3 (Annex H.4 of ISO/IEC 13239) - or so they say
#
# | Flag | Frame format | Dest. address | Src. address | Control | HCS | Information | FCS | Flag |
#

sub handle_frame {
    my ($frame) = @_;
    return unless defined $frame;
    return unless length $frame > 0;
    my $index = 0;
    my $read_bytes = sub {
        my ($len) = @_;
        my $bytes = substr($frame, $index, $len);
        $index += $len;
        return $bytes;
    };
    # Flag field: 1 byte, always set to 0x7E
    my $flag = vec( $read_bytes->(1), 0, 8 );
    print "FRAME: " . unpack('H*', $frame) . " (" . length($frame) . ")\n";
}
