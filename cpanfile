requires 'JSON';
requires 'Digest::CRC';
requires 'Net::MQTT::Simple';
requires 'URI';
requires 'Crypt::AuthEnc::GCM';
on 'test' => sub {
    requires 'Test2::V0';
    requires 'File::Slurp';
};
