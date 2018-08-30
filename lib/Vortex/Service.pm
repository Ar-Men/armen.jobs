#######
##                                    _      __
##   ___ _______ _  ___ ___          (_)__  / /  ___
##  / _ `/ __/  ' \/ -_) _ \  _     / / _ \/ _ \(_-<
##  \_,_/_/ /_/_/_/\__/_//_/ (_) __/ /\___/_.__/___/
##                              |___/
##
####### Ecosystème basé sur les microservices ##################### (c) 2018 losyme ####### @(°_°)@

package Vortex::Service;

#md_# Vortex::Service
#md_

use Exclus::Exclus;
use Moo;
use Safe::Isa qw($_isa);
use Try::Tiny;
use Types::Standard qw(InstanceOf);
use Exclus::Util qw(plugin);
use Gadget::Job;
use Gadget::Workflow;
use Vortex::Bucket;
use namespace::clean;

extends qw(Obscur::Runner::Service);

#md_## Les attributs
#md_

###----+----+----+----+----+----+----+----+----+----+----+----+----+----+----+----+----+----+----+----+----+----+----###
has '+name'        => (default => sub { 'Vortex' });
has '+description' => (default => sub { 'Le µs chargé de gérer la persistence des jobs, workflows et notifications' });
###----+----+----+----+----+----+----+----+----+----+----+----+----+----+----+----+----+----+----+----+----+----+----###

#md_### _backend
#md_
has '_backend' => (
    is => 'lazy', isa => InstanceOf['Obscur::Object'], init_arg => undef
);

#md_## Les méthodes
#md_

#md_### _build__backend()
#md_
sub _build__backend {
    my $self = shift;
    my $config = $self->cfg->create('backend');
    return $self->load_object('Vortex::Backend', $config->get_str('use'), $config->create('cfg'));
}

#md_### _get_next_job()
#md_
sub _get_next_job {
    my ($self, $node, $worker) = @_;
    my $unlock = $self->sync->lock_w_unlock('buckets', 3000); ##@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@
    return unless (my $bucket = Vortex::Bucket->get_next_job($self, $self->_backend));
    my $job = $bucket->get_job;
    $job->add_history($node, $worker);
    $job->status('RUNNING');
    $bucket->update_job($job->unbless);
    $bucket->replace;
    return $job;
}

#md_### _try_get_next_job()
#md_
sub _try_get_next_job {
    my ($self, $respond, $rr, $p) = @_;
    my ($node, $worker) = @{$p->get_arrayref('args')};
    my $hjob;
    try {
        if (my $job = $self->_get_next_job($node, $worker)) {
            $job->export;
            # Soyons optimiste pour l'exécution à venir
            $job->succeeded;
            $hjob = $job->unbless;
        }
    }
    catch {
        $self->logger->log($_->$_isa('EX::Sync::UnableToLock') ? 'notice' : 'err', "$_");
    };
    $respond->($rr->payload($hjob)->render->finalize);
}

#md_### build_API()
#md_
sub build_API {
    my ($self, $api_key) = @_;
    $self->server->get("/$api_key/v0/next_job", sub { $self->_try_get_next_job(@_) });
}

#md_### _create_bucket()
#md_
sub _create_bucket {
    my $self = shift;
    return Vortex::Bucket->new(runner => $self, backend => $self->_backend, @_);
}

#md_### _declare_job()
#md_
sub _declare_job {
    my ($self, $job) = @_;
    $self->info(
        'New job',
        [
            id          => $job->id,
            application => $job->application,
            type        => $job->type,
            label       => $job->label,
            origin      => $job->origin,
            priority    => $job->priority,
            category    => $job->category,
            workflow    => $job->workflow_id
        ]
    );
    $job->export;
}

#md_### _insert_job()
#md_
sub _insert_job {
    my ($self, $message) = @_;
    my $job = Gadget::Job->new(runner => $self, %{$message->{payload}});
    my $inserted;
    {
        my $unlock = $self->sync->lock_w_unlock('buckets', 10000); ##@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@
        $inserted = $self->_create_bucket(job => $job->unbless)->maybe_insert($job->category);
    }
    if ($inserted) {
        $self->_declare_job($job);
    }
    else {
        $self->notice(
            'Un job identique de même catégorie existe déjà',
            [
                application => $job->application,
                type        => $job->type,
                category    => $job->category
            ]
        );
    }
}

#md_### _execute_workflow()
#md_
sub _execute_workflow {
    my ($self, $bucket, $workflow, $job) = @_;
    if ($job = $workflow->execute($job)) {
        $bucket->update_job($job->unbless, sub { $self->_declare_job($job) });
    }
    else {
        $bucket->update_job(
            undef,
            sub {
                $self->info(
                    'Workflow end',
                    [id => $workflow->id, label => $workflow->label, status => $workflow->status]
                );
            }
        );
    }
    $bucket->update_workflow(
        $workflow->unbless,
        sub {
            $workflow->export if $workflow->status ne 'RUNNING';
        }
    );
}

#md_### _update_job()
#md_
sub _update_job {
    my ($self, $message) = @_;
    my $job = Gadget::Job->new(runner => $self, %{$message->{payload}});
    if (my $bucket = Vortex::Bucket->get_bucket($self, $self->_backend, $job->id)) {
        $bucket->update_job($job->unbless, sub { $job->export });
        # Ce job appartient-il à un workflow ?
        if ($job->status ne 'PENDING' && $bucket->workflow) {
            my $workflow = Gadget::Workflow->new(runner => $self, %{$bucket->workflow});
            if ($job->workflow_id && $job->workflow_id eq $workflow->id) {
                $self->_execute_workflow($bucket, $workflow, $job);
            }
            else {
                $self->logger->unexpected_error;
            }
        }
        my $unlock = $self->sync->lock_w_unlock('buckets', 10000); ##@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@
        $bucket->replace;
    }
    else {
        $self->error(
            "Impossible de trouver le 'bucket' correspondant au job à mettre à jour",
            [
                job         => $job->id,
                application => $job->application,
                type        => $job->type
            ]
        );
    }
}

#md_### _notify_job()
#md_
sub _notify_job {
    my ($self, $message) = @_;
}

#md_### _insert_workflow()
#md_
sub _insert_workflow {
    my ($self, $message) = @_;
    my $workflow = Gadget::Workflow->new(runner => $self, %{$message->{payload}});
    my $bucket = $self->_create_bucket(workflow => $workflow->unbless, job => undef);
    $bucket->push_callback(
        sub {
            $self->info(
                'New workflow [begin]',
                [
                    id       => $workflow->id,
                    label    => $workflow->label,
                    origin   => $workflow->origin,
                    title    => $workflow->title,
                    priority => $workflow->priority
                ]
            );
            $workflow->export;
        }
    );
    $self->_execute_workflow($bucket, $workflow);
    my $unlock = $self->sync->lock_w_unlock('buckets', 10000); ##@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@
    $bucket->insert;
}

#md_### on_message()
#md_
sub on_message {
    my ($self, $type, $message) = @_;
       if ($type eq      'job.insert') { $self->_insert_job(     $message) }
    elsif ($type eq      'job.update') { $self->_update_job(     $message) }
    elsif ($type eq      'job.notify') { $self->_notify_job(     $message) }
    elsif ($type eq 'workflow.insert') { $self->_insert_workflow($message) }
    else {
        $self->logger->unexpected_error(type => $type);
    }
}

#md_### _clean_backend()
#md_
sub _clean_backend {
    my ($self) = @_;
    $self->info('Clean backend');
    $self->_backend->clean;
}

#md_### on_starting()
#md_
sub on_starting {
    my ($self) = @_;
    $self->scheduler->add_timer(17, 907, sub { $self->_clean_backend });
    $self->broker->consume($self->name);
}

1;
__END__
