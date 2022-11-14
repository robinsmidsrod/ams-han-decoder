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
        # Previous value is last part of old message
        # Current value is first part of new message
        # FIXME: Not perfect frame borders with this approach
        if ( $prev_value == 0x16 and $value == 0x68 ) {
            #my $new_frame = substr($frame, 0, 1);
            #my $old_frame = substr($frame, 0);
            handle_frame($frame);
            $frame = $char;
            next;
        }
        $frame .= $char;
    }

    print STDERR "read failed: $!\n" unless defined $rc;
    return 1;
}

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
    print "FRAME: " . unpack('H*', $frame) . " (" . length($frame) . ")\n";
}
