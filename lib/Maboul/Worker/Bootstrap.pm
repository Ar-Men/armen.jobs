#######
##                                    _      __
##   ___ _______ _  ___ ___          (_)__  / /  ___
##  / _ `/ __/  ' \/ -_) _ \  _     / / _ \/ _ \(_-<
##  \_,_/_/ /_/_/_/\__/_//_/ (_) __/ /\___/_.__/___/
##                              |___/
##
####### Ecosystème basé sur les microservices ##################### (c) 2018 losyme ####### @(°_°)@

package Maboul::Worker::Bootstrap;

#md_# Maboul::Worker::Bootstrap
#md_

use Exclus::Exclus;
use JSON::MaybeXS qw(decode_json);
use Try::Tiny;
use Exclus::Data;
use Maboul::Worker::Process;

#md_## Les méthodes
#md_

#md_### bootstrap()
#md_
sub bootstrap {
    my ($socket, $name, $cfg) = @_;
    my $exit = -1;
    try {
        my $worker = Maboul::Worker::Process->new(
            socket => $socket,
            name => $name,
            cfg => Exclus::Data->new(data => decode_json($cfg))
        );
        $exit = $worker->process;
    }
    catch {
        syswrite $socket, "$_";

    };
    return $exit;
}

1;
__END__
