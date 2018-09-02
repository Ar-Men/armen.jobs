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
use Ref::Util qw(is_ref is_hashref);
use Try::Tiny;
use Types::Standard -types;
use Exclus::Exceptions;
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
    is => 'ro', isa => Str, default => sub { 'armen' }
);

#md_### title
#md_
has 'title' => (
    is => 'ro', isa => Str, required => 1
);

#md_### priority
#md_
has 'priority' => (
    is => 'ro', isa => Int, coerce => sub { to_priority($_[0]) }, default => sub { 'NONE' }
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
    is => 'ro', isa => Int, default => sub { time }
);

#md_### status
#md_
has 'status' => (
    is => 'rw', isa => WorkflowStatus, default => sub { 'RUNNING' }
);

#md_### data
#md_
has 'data' => (
    is => 'rw', isa => HashRef, default => sub { {} }
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

#md_### _get_step()
#md_
sub _get_step {
    my ($self, $label) = @_;
    return $self->all_steps->{$label} if exists $self->all_steps->{$label};
    EX->throw({ ##//////////////////////////////////////////////////////////////////////////////////////////////////////
        message => "Cette étape n'existe pas pour ce workflow",
        params  => [label => $label, workflow => $self->id]
    });
}

#md_### _get_next_step()
#md_
sub _get_next_step {
    my ($self, $job) = @_;
    my ($label, $step);
    try {
        if ($job) {
            if ($job->next_step_label) {
                $label = $job->next_step_label;
            }
            else {
                my $step = $self->_get_step($job->label);
                my $next_step = exists $step->{next_step} ? $step->{next_step} : undef;
                if ($next_step) {
                    if (is_ref($next_step)) {
                        if (is_hashref($next_step)) {
                            if (defined $job->next_step_key && exists $next_step->{$job->next_step_key}) {
                                $label = $next_step->{$job->next_step_key};
                            }
                            elsif (exists $next_step->{$job->status}) {
                                $label = $next_step->{$job->status};
                            }
                            elsif (exists $next_step->{__default}) {
                                $label = $next_step->{__default};
                            }
                        }
                        if (!defined $label || is_ref($label)) {
                            EX->throw({ ##//////////////////////////////////////////////////////////////////////////////
                                message => "Il est impossible de déterminer la prochaîne étape de ce workflow",
                                params  => [current_step => $job->label, workflow => $self->id]
                            });
                        }
                    }
                    else {
                        $label = $next_step;
                    }
                }
                else {
                    $self->status(defined $job->workflow_failed ? 'FAILED' : 'SUCCEEDED');
                    $self->finished_at(time);
                }
            }
        }
        else {
            $label = $self->first_step;
        }
###----+----+----+----+----+----+----+----+----+----+----+----+----+----+----+----+----+----+----+----+----+----+----###
####### L'éventuelle prochaine étape
        $step = $self->_get_step($label)
            if $label;
###----+----+----+----+----+----+----+----+----+----+----+----+----+----+----+----+----+----+----+----+----+----+----###
    }
    catch {
        $self->logger->error("$_");
        $self->status('ABORTED');
        $self->finished_at(time);
    };
    return ($label, $step);
}

#md_### _get_next_job()
#md_
sub _get_next_job {
    my ($self, $job) = @_;
    my ($label, $step) = $self->_get_next_step($job);
    return unless $step;
    return Gadget::Job->new(
        runner          => $self->runner,
        label           => $label,
        origin          => $self->origin,
        priority        => $self->priority,
        reference_time  => $self->created_at,
        workflow_id     => $self->id,
        public          => $job ? $job->public          : $self->data,
        workflow_failed => $job ? $job->workflow_failed :       undef,
        %$step
    );
}

#md_### execute()
#md_
sub execute {
    my ($self, $job) = @_;
    push @{$self->history}, $job->unbless
        if $job;
    return $self->_get_next_job($job);
}

1;
__END__
