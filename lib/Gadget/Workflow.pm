#######
##                                    _      __
##   ___ _______ _  ___ ___          (_)__  / /  ___
##  / _ `/ __/  ' \/ -_) _ \  _     / / _ \/ _ \(_-<
##  \_,_/_/ /_/_/_/\__/_//_/ (_) __/ /\___/_.__/___/
##                              |___/
##
####### Ecosystème basé sur les microservices ##################### (c) 2018 losyme ####### @(°_°)@

package Gadget::Workflow;

#md_# Gadget::Job
#md_

use Exclus::Exclus;
use Moo;
use Types::Standard qw(ArrayRef HashRef Int Maybe Str);
use Exclus::Util qw(create_uuid to_priority);
use Gadget::Job;
use Gadget::Types qw(WorkflowStatus);
use namespace::clean;

extends qw(Obscur::Context);

#md_## Les attributs
#md_

#md_### id
#md_
has 'id' => (
    is => 'ro', isa => Str, default => sub { create_uuid() }
);

#md_### label
#md_
has 'label' => (
    is => 'ro', isa => Str, lazy => 1, default => sub { sprintf('_%s', substr($_[0]->id, 0, 8)) }
);

#md_### origin
#md_
has 'origin' => (
    is => 'ro', isa => Str, required => 1
);

#md_### title
#md_
has 'title' => (
    is => 'ro', isa => Str, required => 1
);

#md_### priority
#md_
has 'priority' => (
    is => 'ro', isa => Int, coerce => sub { to_priority($_[0]) }, required => 1
);

#md_### first_step
#md_
has 'first_step' => (
    is => 'ro', isa => Str, required => 1
);

#md_### all_steps
#md_
has 'all_steps' => (
    is => 'ro', isa => HashRef[HashRef], required => 1
);

#md_### created_at
#md_
has 'created_at' => (
    is => 'ro', isa => Int, default => sub { time }, init_arg => undef
);

#md_### status
#md_
has 'status' => (
    is => 'rw', isa => WorkflowStatus, default => sub { 'RUNNING' }
);

#md_### data
#md_
has 'data' => (
    is => 'rw', isa => HashRef, required => 1
);

#md_### history
#md_
has 'history' => (
    is => 'ro', isa => ArrayRef[HashRef], default => sub { [] }
);

#md_### finished_at
#md_
has 'finished_at' => (
    is => 'rw', isa => Maybe[Int], default => sub { undef }
);

#md_## Les méthodes
#md_

#md_### unbless()
#md_
sub unbless {
    my ($self) = @_;
    my $data = {};
    $data->{$_} = $self->$_
        foreach qw(id label origin title priority first_step all_steps created_at status data history finished_at);
    return $data;
};

#md_### export()
#md_
sub export {
    my ($self) = @_;
    $self->runner->broker->try_publish(sprintf('workflow.export.%s', $self->origin), 'NONE', $self->unbless);
}

1;
__END__
