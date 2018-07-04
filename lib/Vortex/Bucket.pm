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
use Types::Standard qw(ArrayRef HashRef InstanceOf);
use namespace::clean;

extends qw(Obscur::Context);

#md_## Les attributs
#md_

#md_### backend
#md_
has 'backend' => (
    is => 'ro', isa => InstanceOf['Obscur::Object'], required => 1
);

#md_### jobs
#md_
has 'jobs' => (
    is => 'ro', isa => ArrayRef[HashRef], default => sub { [] }
);

#md_## Les méthodes
#md_

#md_### _unbless()
#md_
sub _unbless {
    my ($self) = @_;
    my $data = {};
    $data->{$_} = $self->$_ foreach qw(jobs);
    return $data;
};

#md_### maybe_insert()
#md_
sub maybe_insert {
    my ($self, $job) = @_;
    push @{$self->jobs}, $job->unbless;
    return $self->backend->maybe_insert_bucket($self->_unbless, $job->category);
}

1;
__END__
