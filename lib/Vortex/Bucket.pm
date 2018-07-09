#######
##                                    _      __
##   ___ _______ _  ___ ___          (_)__  / /  ___
##  / _ `/ __/  ' \/ -_) _ \  _     / / _ \/ _ \(_-<
##  \_,_/_/ /_/_/_/\__/_//_/ (_) __/ /\___/_.__/___/
##                              |___/
##
####### Ecosystème basé sur les microservices ##################### (c) 2018 losyme ####### @(°_°)@

package Vortex::Bucket;

#md_# Vortex::Bucket
#md_

use Exclus::Exclus;
use Moo;
use Types::Standard qw(ArrayRef CodeRef HashRef InstanceOf Maybe Str);
use Gadget::Job;
use namespace::clean;

extends qw(Obscur::Context);

#md_## Les attributs
#md_

#md_### backend
#md_
has 'backend' => (
    is => 'ro', isa => InstanceOf['Obscur::Object'], required => 1
);

#md_### id
#md_
has 'id' => (
    is => 'lazy', isa => Str
);

#md_### jobs
#md_
has 'jobs' => (
    is => 'ro', isa => ArrayRef[HashRef], default => sub { [] }
);

#md_### workflow
#md_
has 'workflow' => (
    is => 'rw', isa => Maybe[HashRef], default => sub { undef }
);

#md_### _callbacks
#md_
has '_callbacks' => (
    is => 'ro', isa => ArrayRef[CodeRef], default => sub { [] }, init_arg => undef
);

#md_## Les méthodes
#md_

#md_### _build_id()
#md_
sub _build_id {
    my $self = shift;
    return $self->workflow ? $self->workflow->{id} : $self->jobs->[0]->{id};
}

#md_### _unbless()
#md_
sub _unbless {
    my ($self) = @_;
    my $data = {};
    $data->{$_} = $self->$_ foreach qw(id jobs workflow);
    return $data;
};

#md_### get_job()
#md_
sub get_job {
    my ($self) = @_;
    return Gadget::Job->new(runner => $self->runner, %{$self->jobs->[-1]});
}

#md_### maybe_insert()
#md_
sub maybe_insert {
    my ($self, $job) = @_;
    push @{$self->jobs}, $job->unbless;
    return $self->backend->maybe_insert_bucket($self->_unbless, $job->category);
}

#md_### push_callback()
#md_
sub push_callback { push @{shift->_callbacks}, @_ }

#md_### update_job()
#md_
sub update_job {
    my ($self, $job, $cb) = @_;
    $self->jobs->[-1] = $job->unbless;
    $self->push_callback($cb)
        if $cb;
}

#md_### replace()
#md_
sub replace {
    my ($self) = @_;
    $self->backend->replace_bucket($self->_unbless);
    $_->() foreach @{$self->_callbacks};
}

#md_## Les méthodes de la classe
#md_

#md_### _get_running_jobs()
#md_
sub _get_running_jobs {
    my ($class, $backend) = @_;
    my $running = {};
    $backend->foreach_running_buckets(
        sub {
            my $bucket = shift;
            my $job = $bucket->{jobs}->[-1];
            # Un job exclusif de type 'ALL' est en cours ?
            if ($job->{exclusivity} eq 'ALL') {
                $running = undef;
                return;
            }
            my $application = $job->{application};
            my $type = $job->{type};
            my $ref = $running->{$application}->{$type};
            $ref->{count} = 0 unless exists $ref->{count};
            $ref->{count} += 1;
            if (my $group = $job->{group}) {
                $ref->{$group} = 0 unless exists $ref->{$group};
                $ref->{$group} += 1;
            }
            return 1;
        }
    );
    return $running;
}

#md_### get_next_job()
#md_
sub get_next_job {
    my ($class, $runner, $backend) = @_;
    my $next;
    my $config = $runner->config;
    return unless defined (my $running = $class->_get_running_jobs($backend));
    $backend->foreach_ordered_buckets(
        sub {
            my $bucket = shift;
            my $job = $bucket->{jobs}->[-1];
            my $exclusivity = $job->{exclusivity};
            # Un job exclusif mais il y a encore des jobs en cours ?
            return
                if $exclusivity eq 'ALL' && scalar keys %$running;
            my $application = $job->{application};
            # Les jobs uniquement exclusifs avec tous ceux de la même application
            return 1
                if $exclusivity eq 'APP' && exists $running->{$application};
            my $type = $job->{type};
            my $ref = $running->{$application}->{$type};
            # Les jobs uniquement exclusifs avec eux-mêmes
            return 1
                if $exclusivity eq 'ITSELF' && defined $ref;
            if (my $app_cfg = $config->create({default => undef}, 'applications', $application)) {
                return 1
                    if $app_cfg->get_bool({default => 0}, 'disabled');
                # Le nombre de job de ce type est-il limité ?
                my $max = $app_cfg->maybe_get_int('jobs', $type, 'max');
                return 1
                    if defined $max && (!$max || (defined $ref && $ref->{count} >= $max));
                # Le nombre de job de ce type et de ce groupe est-il limité ?
                if (my $group = $job->{group}) {
                    my $max = $app_cfg->maybe_get_int('jobs', $type, 'group', $group);
                    return 1
                        if defined $max && (!$max || (defined $ref && exists $ref->{$group} && $ref->{count} >= $max));
                }
            }
###----+----+----+----+----+----+----+----+----+----+----+----+----+----+----+----+----+----+----+----+----+----+----###
########### Voici l'élu!
            $next = $class->new(runner => $runner, backend => $backend, %$bucket);
###----+----+----+----+----+----+----+----+----+----+----+----+----+----+----+----+----+----+----+----+----+----+----###
            return;
        }
    );
    return $next;
}

1;
__END__
