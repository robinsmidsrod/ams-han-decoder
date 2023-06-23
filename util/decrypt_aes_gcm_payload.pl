#!/usr/bin/env perl

use strict;
use warnings;

use Crypt::AuthEnc::GCM;

my $key = pack('H*', shift);
my $iv = pack('H*', shift ); # system title + frame counter
my $payload = pack('H*', shift);
my $ae = Crypt::AuthEnc::GCM->new("AES", $key, $iv);
my $plaintext = $ae->decrypt_add($payload);
my $tag = $ae->decrypt_done();

print STDERR "TAG: " . as_hex($tag) . "\n";
print as_hex($plaintext);

# Format a string of octets as hex numbers
sub as_hex {
    return unpack('H*', $_[0]);
}
