#######
##                                    _      __
##   ___ _______ _  ___ ___          (_)__  / /  ___
##  / _ `/ __/  ' \/ -_) _ \  _     / / _ \/ _ \(_-<
##  \_,_/_/ /_/_/_/\__/_//_/ (_) __/ /\___/_.__/___/
##                              |___/
##
####### Ecosystème basé sur les microservices ##################### (c) 2018 losyme ####### @(°_°)@

package Maboul::Service;

#md_# Maboul::Service
#md_

use Exclus::Exclus;
use List::Util qw(max min);
use Moo;
use Try::Tiny;
use Types::Standard qw(ArrayRef HashRef InstanceOf);
use Exclus::Util qw(render_table);
use Maboul::Worker::Handler;
use namespace::clean;

extends qw(Obscur::Runner::Service);

#md_## Les attributs
#md_

###----+----+----+----+----+----+----+----+----+----+----+----+----+----+----+----+----+----+----+----+----+----+----###
has '+name'          => (default => sub { 'Maboul' });
has '+description'   => (default => sub { 'Le µs chargé de gérer les workers' });
has '+long_stopping' => (default => sub { 1 });
###----+----+----+----+----+----+----+----+----+----+----+----+----+----+----+----+----+----+----+----+----+----+----###

#md_### _workers
#md_
has '_workers' => (
    is => 'ro', isa => HashRef[InstanceOf['Maboul::Worker::Handler']], default => sub { {} }, init_arg => undef
);

#md_### ARGV
#md_
has 'ARGV' => (
    is => 'ro', isa => ArrayRef, default => sub { [@ARGV] }, init_arg => undef
);

#md_## Les méthodes
#md_

#md_### cmd_workers()
#md_
sub cmd_workers {
    my $self = shift;
    my $rows = [];
    while (my ($worker, $handler) = each %{$self->_workers}) {
        my $job = $handler->job;
        try {
            push @$rows, [
                $worker,
                $job
                    ? (
                        $job->get_str('id'         ),
                        $job->get_str('application'),
                        $job->get_str('type'       ),
                        $job->get_str('label'      )
                    )
                    : ()
            ];
        }
        catch {
            $self->error("$_");
        };
    }
    return render_table($rows, 'WORKER', '(JOB>) ID', qw(APPLICATION TYPE LABEL));
}

#md_### _create_worker_name()
#md_
sub _create_worker_name {
    my ($self) = @_;
    my $number = 0;
    for (;;) {
        my $name = sprintf('mwk.%02u', $number++);
        return
            $name unless exists $self->_workers->{$name};
    }
}

#md_### _create_worker()
#md_
sub _create_worker {
    my ($self) = @_;
    my $name = $self->_create_worker_name;
    my $handler = Maboul::Worker::Handler->new(runner => $self, name => $name);
    my $cb;
    $cb = sub {
        my ($name, $ready) = @_;
        if ($self->is_stopping) {
            delete $self->_workers->{$name};
        }
        elsif ($ready) {
            $self->_workers->{$name}->start_worker($cb);
        }
        else {
            delete $self->_workers->{$name};
            kill 'TERM', $$;
        }
    };
    $self->_workers->{$name} = $handler;
    $handler->start_worker($cb);
}

#md_### _start_workers()
#md_
sub _start_workers {
    my ($self) = @_;
    my $max = min(max($self->cfg->get({default => 1}, qw(workers max)), 1), 10);
    $self->_create_worker foreach 1..$max;
}

#md_### on_starting()
#md_
sub on_starting { $_[0]->_start_workers }

#md_### on_stopping()
#md_
sub on_stopping { $_->stop_worker foreach values %{$_[0]->_workers} }

#md_### is_ready_to_stop()
#md_
sub is_ready_to_stop { keys %{$_[0]->_workers} == 0 }

1;
__END__
