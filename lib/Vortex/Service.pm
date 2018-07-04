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
use Gadget::Job;
use namespace::clean;

extends qw(Obscur::Runner::Service);

#md_## Les attributs
#md_

###----+----+----+----+----+----+----+----+----+----+----+----+----+----+----+----+----+----+----+----+----+----+----###
has '+name'        => (default => sub { 'Vortex' });
has '+description' => (default => sub { 'Le µs chargé de gérer la persistence des jobs, workflows et notifications' });
###----+----+----+----+----+----+----+----+----+----+----+----+----+----+----+----+----+----+----+----+----+----+----###

#md_## Les méthodes
#md_

#md_### _insert_job()
#md_
sub _insert_job {
    my ($self, $message) = @_;
    my $job = Gadget::Job->new(runner => $self, %{$message->{payload}});
}

#md_### _update_job()
#md_
sub _update_job {
    my ($self, $message) = @_;
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

#md_### on_starting()
#md_
sub on_starting {
    my ($self) = @_;
    $self->broker->consume($self->name);
}

1;
__END__
