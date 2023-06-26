# ams-han-decoder

Decoder of binary serial stream from HAN port of power meters in Norway.

Requires MBUS slave-to-serial adapter hardware.  See main script for details
on working configurations.

# Running

See main script for details on how to use it. Run with `-h` for help.

# Installing dependencies

Requires `JSON` CPAN module to work.  Install dependencies using your system
package manager or favorite CPAN client. If you're using a Debian-based
system, it is usually enough to `apt-get install libjson-perl libwww-perl`.

If you're using a custom Perl environment or another installation method,
you can either use `cpanm --installdeps .` to install the dependencies where
your environment expects them, or use `carton install` to install them in
`local/` and use `carton exec ams_han_decoder.pl` to run the script. Or you
can set the environment variable `PERL5LIB` to `local/lib/perl5` and run the
binary. See [Carton](https://metacpan.org/pod/Carton) for more details.

# Installing as a systemd service shipping JSON over MQTT

This is an example of using the decoder together with `mosquitto_pub` to
ship the JSON messages over MQTT to its destination.

    [Unit]
    Description=AMS HAN decoder

    [Service]
    Environment=HOME=/root
    ExecStart=/usr/local/bin/ams-han-decoder -m AIDON_V0001 -k -c -p 'mosquitto_pub -l -t sensor/aidon' /dev/aidon
    # Avoid memory leak eating all memory
    MemoryHigh=50M
    MemoryMax=100M

    [Install]
    WantedBy=multi-user.target

# Caveats

A memory leak has been detected when you use the `-p` parameter. It is
somewhat diminished when using the `-k` parameter (to keep the program
running between messages). It is recommended to always use `-k` with `-p` to
minimize this memory leak. If you're using systemd, as above, then the
limits imposed on the service should keep it somewhat under control. If you
have suggestions for how to resolve this issue, please create a ticket for
it. Normal memory usage is around 9MB.
