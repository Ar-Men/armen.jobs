#######
##                                    _      __
##   ___ _______ _  ___ ___          (_)__  / /  ___
##  / _ `/ __/  ' \/ -_) _ \  _     / / _ \/ _ \(_-<
##  \_,_/_/ /_/_/_/\__/_//_/ (_) __/ /\___/_.__/___/
##                              |___/
##
####### Ecosystème basé sur les microservices ##################### (c) 2018 losyme ####### @(°_°)@

package Maboul::Worker::Handler;

#md_# Maboul::Worker::Handler
#md_

use Exclus::Exclus;
use AnyEvent;
use AnyEvent::Fork;
use AnyEvent::Handle;
use JSON::MaybeXS qw(encode_json);
use Moo;
use Scalar::Util qw(looks_like_number);
use Types::Standard qw(Bool InstanceOf Maybe Str);
use namespace::clean;

extends qw(Obscur::Context);

#md_## Les attributs
#md_

#md_### name
#md_
has 'name' => (
    is => 'ro', isa => Str, => required => 1
);

#md_### _socket_handle
#md_
has '_socket_handle' => (
    is => 'rw', isa => Maybe[InstanceOf['AnyEvent::Handle']], default => sub { undef }, init_arg => undef
);

#md_### _ready
#md_
has '_ready' => (
    is => 'rw', isa => Bool, default => sub { 0 }, init_arg => undef
);

#md_## Les méthodes
#md_

#md_### _on_data()
#md_
sub _on_data {
    my ($self, $data) = @_;
    if (looks_like_number($data)) {
        $self->_ready(1);
    }
    else {
        $self->logger->error("Donnée inattendue", [data => $data]);
    }
}

#md_### _on_worker_events()
#md_
sub _on_worker_events {
    my ($self, $cb, $socket) = @_;
    my $handle = AnyEvent::Handle->new(
        fh => $socket,
        on_error => sub {
            my ($handle, $fatal, $message) = @_;
            $self->logger->error('Worker', [name => $self->name, fatal => $fatal, message => $message]);
            $handle->destroy;
            $cb->($self->name, $self->_ready);
        },
        on_read => sub {
            my $handle = shift;
            $self->_on_data(delete $handle->{rbuf});
        },
        on_eof => sub {
            my $handle = shift;
            $self->logger->info('<<== ' . $self->name);
            $handle->destroy;
            $cb->($self->name, $self->_ready);
        }
    );
    $self->_socket_handle($handle);
}

#md_### start_worker()
#md_
sub start_worker {
    my ($self, $cb) = @_;
    my $runner = $self->runner;
    my $cfg = $runner->cfg->get_hashref({default => {}}, qw(workers cfg));
    $cfg->{ARGV} = $runner->ARGV;
    $self->logger->info('==>> ' . ${self}->name);
    $self->_ready(0);
    my $module = 'Maboul::Worker::Bootstrap';
###----+----+----+----+----+----+----+----+----+----+----+----+----+----+----+----+----+----+----+----+----+----+----###
    AnyEvent::Fork
        ->new
        ->require($module)
        ->send_arg($self->name, encode_json($cfg))
        ->run("${module}::bootstrap", sub { $self->_on_worker_events($cb, @_) });
###----+----+----+----+----+----+----+----+----+----+----+----+----+----+----+----+----+----+----+----+----+----+----###
}

#md_### stop_worker()
#md_
sub stop_worker { $_[0]->_socket_handle->push_write('stop') }

1;
__END__
