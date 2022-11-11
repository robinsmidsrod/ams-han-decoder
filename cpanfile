requires 'JSON';
requires 'Digest::CRC';
requires 'Net::MQTT::Simple';
requires 'URI';
on 'test' => sub {
    requires 'Test2::V0';
    requires 'File::Slurp';
};
