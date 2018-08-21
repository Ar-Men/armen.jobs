#######
##                                    _      __
##   ___ _______ _  ___ ___          (_)__  / /  ___
##  / _ `/ __/  ' \/ -_) _ \  _     / / _ \/ _ \(_-<
##  \_,_/_/ /_/_/_/\__/_//_/ (_) __/ /\___/_.__/___/
##                              |___/
##
####### Ecosystème basé sur les microservices ##################### (c) 2018 losyme ####### @(°_°)@

package Maboul::Worker::Process;

#md_# Maboul::Worker::Process
#md_

use Exclus::Exclus;
use EV;
use AnyEvent;
use AnyEvent::Handle;
use Guard qw(scope_guard);
use JSON::MaybeXS qw(encode_json);
use List::Util qw(min);
use Module::Runtime qw(use_module);
use Moo;
use Safe::Isa qw($_isa);
use Try::Tiny;
use Types::Standard qw(FileHandle InstanceOf Int);
use namespace::clean;

extends qw(Obscur::Runner::Process);

#md_## Les attributs
#md_

###----+----+----+----+----+----+----+----+----+----+----+----+----+----+----+----+----+----+----+----+----+----+----###
has '+description' => (default => sub { "Un worker chargé d'exécuter des jobs" });
###----+----+----+----+----+----+----+----+----+----+----+----+----+----+----+----+----+----+----+----+----+----+----###

#md_### socket
#md_
has 'socket' => (
    is => 'ro', isa => FileHandle, required => 1
);

#md_### cfg
#md_
has 'cfg' => (
    is => 'ro', isa => InstanceOf['Exclus::Data'], required => 1
);

#md_### _socket_handle
#md_
has '_socket_handle' => (
    is => 'lazy', isa => InstanceOf['AnyEvent::Handle'], init_arg => undef
);

#md_### _cond_var
#md_
has '_cond_var' => (
    is => 'ro', isa => InstanceOf['AnyEvent::CondVar'], default => sub { AE::cv }, init_arg => undef
);

#md_### _job_counter
#md_
has '_job_counter' => (
    is => 'rw', isa => Int, default => sub { 0 }, init_arg => undef
);

#md_### _max_jobs
#md_
has '_max_jobs' => (
    is => 'ro',
    isa => Int,
    lazy => 1,
    default => sub { $_[0]->cfg->get_int({default => 100}, 'max_jobs') },
    init_arg => undef
);

#md_## Les méthodes
#md_

#md_### _build__socket_handle()
#md_
sub _build__socket_handle {
    my $self = shift;
    return AnyEvent::Handle->new(
        fh => $self->socket,
        on_error => sub{
            my ($handle, $fatal, $message) = @_;
            $self->error('Worker', [fatal => $fatal, message => $message]);
            $self->_cond_var->send;
        },
        on_read => sub {
            my $handle = shift;
            $self->_on_cmd(delete $handle->{rbuf});
        },
        on_eof => sub {
            $self->_cond_var->send;
        }
    );
}

#md_### _on_cmd()
#md_
sub _on_cmd {
    my ($self, $cmd) = @_;
    if ($cmd eq 'stop') {
        $self->_cond_var->send;
    }
    else {
        $self->error('Commande inattendue', [cmd => $cmd]);
    }
}

#md_### _setup_applications()
#md_
sub _setup_applications {
    my ($self) = @_;
    my $app = $self->config->create({default => undef}, 'applications');
    return unless $app;
    $app->foreach_key(
        {create => 1},
        sub {
            my ($name, $cfg) = @_;
            return if $cfg->get_bool({default => 0}, 'disabled');
            $self->debug('Application', [name => $name]);
            use_module("Application::$name")->setup($self, $cfg);
        }
    );
}

#md_### _send_notification()
#md_
sub _send_notification {
    my ($self, $type, $data) = @_;
    shift->_socket_handle->push_write(encode_json({type => $type, data => $data}))
}

#md_### _setup()
#md_
sub _setup {
    my ($self) = @_;
    $self->_setup_applications;
    $self->_send_notification('ready', $$);
}

#md_### _build_job()
#md_
sub _build_job {
    my ($self, $job) = @_;
    my $class = join('::', 'Application', $job->{application}, 'Jobs', $job->{type});
    return try {
        return use_module($class)->new(runner => $self, %$job);
    }
    catch {
        $self->error("Impossible d'instancier ce job", [class => $class, error => "$_"]);
        #TODO: publish(Job.Abort) -> Vortex
        return;
    };
}

#md_### _job_execute()
#md_
sub _job_execute {
    my ($self) = @_;
    my $hjob;
    try {
###----+----+----+----+----+----+----+----+----+----+----+----+----+----+----+----+----+----+----+----+----+----+----###
        $hjob = $self->client->get('Vortex', 'next_job', $self->node_name, $self->name);
###----+----+----+----+----+----+----+----+----+----+----+----+----+----+----+----+----+----+----+----+----+----+----###
        if ($hjob && (my $job = $self->_build_job($hjob))) {
            $self->_job_counter($self->_job_counter + 1);
            $self->info(sprintf('Job N° %d/%d', $self->_job_counter, $self->_max_jobs));
            $self->_send_notification
            (
                'job',
                {
                    id          => $job->id,
                    application => $job->application,
                    type        => $job->type,
                    label       => $job->label
                }
            );
            scope_guard { $self->_send_notification('job') };
            $job->execute;
        }
    }
    catch {
        if ($_->$_isa('EX::Client::NoEndpoint')) {
            $self->notice("$_");
        }
        else {
            $self->error("$_");
            #TODO: publish(Job.Abort) -> Vortex
        }
    };
    return defined $hjob;
}

#md_### _repeat()
#md_
sub _repeat {
    my ($self, $after) = @_;
    my $w;
    $w = AE::timer $after, 0, sub {
        undef $w;
        if ($self->_job_execute) {
            if ($self->_job_counter == $self->_max_jobs) {
                $self->_cond_var->send ;
            }
            else {
                $self->_repeat(0.1);
            }
        }
        else {
            $self->_repeat(min($after * 5, 30));
        }
    }
}

#md_### run()
#md_
sub run {
    my ($self) = @_;
    $self->info('Location', [node => $self->node_name]);
    $self->_setup;
    $self->_repeat(3);
    $self->info('READY.process', [description => $self->description]);
###----+----+----+----+----+----+----+----+----+----+----+----+----+----+----+----+----+----+----+----+----+----+----###
    $self->_cond_var->recv;
###----+----+----+----+----+----+----+----+----+----+----+----+----+----+----+----+----+----+----+----+----+----+----###
}

1;
__END__
