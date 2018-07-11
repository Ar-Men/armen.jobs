#######
##                                    _      __
##   ___ _______ _  ___ ___          (_)__  / /  ___
##  / _ `/ __/  ' \/ -_) _ \  _     / / _ \/ _ \(_-<
##  \_,_/_/ /_/_/_/\__/_//_/ (_) __/ /\___/_.__/___/
##                              |___/
##
####### Ecosystème basé sur les microservices ##################### (c) 2018 losyme ####### @(°_°)@

package Gadget::Job;

#md_# Gadget::Job
#md_

use Exclus::Exclus;
use Guard qw(guard);
use Moo;
use Safe::Isa qw($_isa);
use Try::Tiny;
use Types::Standard qw(ArrayRef Bool HashRef Int Maybe Str);
use Exclus::Util qw(create_uuid time_to_string to_priority);
use Gadget::Types qw(JobExclusivity JobStatus);
use namespace::clean;

extends qw(Obscur::Context);

#md_## Les attributs
#md_

#md_### id
#md_
has 'id' => (
    is => 'ro', isa => Str, default => sub { create_uuid() }
);

#md_### application
#md_
has 'application' => (
    is => 'ro', isa => Str, required => 1
);

#md_### type
#md_
has 'type' => (
    is => 'ro', isa => Str, required => 1
);

#md_### label
#md_
has 'label' => (
    is => 'ro', isa => Str, lazy => 1, default => sub { sprintf('_%s', substr($_[0]->id, 0, 8)) }
);

#md_### origin
#md_
has 'origin' => (
    is => 'ro', isa => Str, default => sub { 'armen' }
);

#md_### priority
#md_
has 'priority' => (
    is => 'ro', isa => Int, coerce => sub { to_priority($_[0]) }, required => 1
);

#md_### exclusivity
#md_
has 'exclusivity' => (
    is => 'ro', isa => JobExclusivity, default => sub { 'NO' }
);

#md_### category
#md_
has 'category' => (
    is => 'ro', isa => Maybe[Str], default => sub { undef }
);

#md_### group
#md_
has 'group' => (
    is => 'ro', isa => Maybe[Str], default => sub { undef }
);

#md_### reference_time
#md_
has 'reference_time' => (
    is => 'ro', isa => Int, default => sub { time }
);

#md_### cfg
#md_
has 'cfg' => (
    is => 'ro', isa => HashRef, default => sub { {} }
);

#md_### workflow_id
#md_
has 'workflow_id' => (
    is => 'ro', isa => Maybe[Str], default => sub { undef }
);

#md_### workflow_state
#md_
has 'workflow_state' => (
    is => 'ro', isa => Bool, default => sub { 1 }
);

#md_### created_at
#md_
has 'created_at' => (
    is => 'ro', isa => Int, default => sub { time }
);

#md_### status
#md_
has 'status' => (
    is => 'rw', isa => JobStatus, default => sub { 'TODO' }
);

#md_### run_after
#md_
has 'run_after' => (
    is => 'rw', isa => Int,  default => sub { 0 }
);

#md_### retry_count
#md_
has 'retry_count' => (
    is => 'rw', isa => Int, default => sub { 0 }
);

#md_### public
#md_
has 'public' => (
    is => 'ro', isa => HashRef, default => sub { {} }
);

#md_### private
#md_
has 'private' => (
    is => 'ro', isa => HashRef, default => sub { {} }
);

#md_### history
#md_
has 'history' => (
    is => 'ro', isa => ArrayRef[HashRef], default => sub { [] }
);

#md_### result
#md_
has 'result' => (
    is => 'rw', isa => Maybe[Str], default => sub { undef }
);

#md_## Les méthodes
#md_

#md_### unbless()
#md_
sub unbless {
    my ($self) = @_;
    my $data = {};
    $data->{$_} = $self->$_
        foreach qw(
            id application  type  label  origin  priority  exclusivity  category  group  reference_time  cfg
            workflow_id workflow_state created_at status run_after retry_count public private history result
        );
    return $data;
}

#md_### export()
#md_
sub export {
    my ($self) = @_;
    $self->runner->broker->try_publish(
        sprintf('job.export.%s.%s', $self->application, $self->origin),
        'NONE',
        $self->unbless
    );
}

#md_### add_history()
#md_
sub add_history {
    my ($self, $node, $worker) = @_;
    push @{$self->history}, {
        node     => $node,
        worker   => $worker,
        reserved => time,
        begin    => undef,
        end      => undef
    };
}

#md_### _get_last_history()
#md_
sub _get_last_history { return $_[0]->history->[-1] }

#md_### _set_history_status()
#md_
sub _set_history_status { return $_[0]->_get_last_history->{status} = $_[1] }

#md_### _set_history_run_after()
#md_
sub _set_history_run_after { return $_[0]->_get_last_history->{run_after} = $_[1] }

#md_### _set_history_begin()
#md_
sub _set_history_begin { $_[0]->_get_last_history->{begin} = time }

#md_### _set_history_end()
#md_
sub _set_history_end { $_[0]->_get_last_history->{end} = time }

#md_### _set_history_error()
#md_
sub _set_history_error { return $_[0]->_get_last_history->{error} = $_[1] }

#md_### _set_status()
#md_
sub _set_status {
    my ($self, $status) = @_;
    $self->status($self->_set_history_status($status));
}

#md_### _set_run_after()
#md_
sub _set_run_after {
    my ($self, $after) = @_;
    $self->run_after($self->_set_history_run_after($after));
}

#md_### set_run_after()
#md_
sub set_run_after {
    my ($self, $delay) = @_;
    $self->_set_run_after(time + ($self->retry_count * $delay + $delay) * 60);
}

#md_### pending()
#md_
sub pending {
    my ($self, $delay) = @_;
    $self->_set_run_after(time + $delay * 60);
    $self->_set_status('PENDING');
    $self->logger->info('Job continue', [run_after => time_to_string($self->run_after)]);
}

#md_### succeeded()
#md_
sub succeeded {
    my ($self) = @_;
    $self->_set_run_after(0);
    $self->_set_status('SUCCEEDED');
}

#md_### _prepare_logger()
#md_
sub _prepare_logger {
    my ($self) = @_;
    my $logger = $self->logger;
    my $data = $logger->runner_data;
    $logger->runner_data(substr($self->workflow_id ? $self->workflow_id : $self->id, 0, 8));
    return guard { $logger->runner_data($data) };
}

#md_### _before_run()
#md_
sub _before_run {
    my ($self) = @_;
    $self->logger->info('Job begin', [application => $self->application, type => $self->type, label => $self->label]);
    $self->_set_history_begin;
}

#md_### run()
#md_
sub run {
    my ($self) = @_;
sleep 1; #AFAC
}

#md_### _update()
#md_
sub _update {
    my ($self) = @_;
    $self->runner->broker->publish('job.update', $self->priority, $self->unbless);
}

#md_### _can_retry()
#md_
sub _can_retry {
    my ($self, $exception) = @_;
    my $delay;
    if ($exception->$_isa('EX') && ($delay = $exception->retry)) {
        my $max_attempts = $exception->max_attempts(100);
        undef($delay) if $self->retry_count >= $max_attempts;
    }
    return $delay;
}

#md_### _retry()
#md_
sub _retry {
    my ($self, $exception, $delay) = @_;
    $self->set_run_after($delay);
    my $retry_count = $self->retry_count;
    $self->retry_count(++$retry_count);
    $self->_set_status('PENDING');
    $self->_update;
    $self->logger->warning("$exception");
    if ($retry_count >= 10) {
        $self->logger->warning(
            "Le nombre de tentatives d'exécution pour cette tâche est conséquent",
            [
                id          => $self->id,
                application => $self->application,
                type        => $self->type,
                origin      => $self->origin,
                priority    => $self->priority,
                workflow    => $self->workflow,
                retry_count => $retry_count
            ]
        );
    }
    $self->logger->info('Job retry', [run_after => time_to_string($self->run_after)]);
}

#md_### _after_run()
#md_
sub _after_run {
    my ($self, $exception) = @_;
    $self->_set_history_end;
    if ($exception) {
        $self->_set_history_error("$exception");
        if (my $delay = $self->_can_retry($exception)) {
            $self->_retry($exception, $delay);
        }
        else {
            $self->_set_status('FAILED');
            $self->_update;
            $self->logger->error("$exception");
        }
    }
    else {
        $self->_update;
    }
    $self->logger->info(
        'Job end',
        [application => $self->application, type => $self->type, label => $self->label, status => $self->status]
    );
}

#md_### execute()
#md_
sub execute {
    my ($self) = @_;
    my $guard = $self->_prepare_logger;
    $self->_before_run;
    my $exception;
###----+----+----+----+----+----+----+----+----+----+----+----+----+----+----+----+----+----+----+----+----+----+----###
    try { $self->run } catch { $exception = $_ };
###----+----+----+----+----+----+----+----+----+----+----+----+----+----+----+----+----+----+----+----+----+----+----###
    $self->_after_run($exception);
}

1;
__END__
