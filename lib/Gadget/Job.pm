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
use Moo;
use Types::Standard qw(ArrayRef Bool HashRef Int Maybe Str);
use Exclus::Util qw(create_uuid);
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
    is => 'ro', isa => JobExclusivity, required => 1
);

#md_### category
#md_
has 'category' => (
    is => 'ro', isa => Maybe[Str], default => sub { undef }
);

#md_### run_group
#md_
has 'run_group' => (
    is => 'ro', isa => Maybe[Str], default => sub { undef }
);

#md_### run_time
#md_
has 'run_time' => (
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
    is => 'ro', isa => Int, default => sub { time }, init_arg => undef
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

#md_### shared
#md_
has 'shared' => (
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
            id  application  type  label  origin  priority   exclusivity  category  run_group  run_time  cfg
            workflow_id workflow_state created_at status run_after retry_count shared private history result
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

1;
__END__
