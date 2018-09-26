#######
##                                    _      __
##   ___ _______ _  ___ ___          (_)__  / /  ___
##  / _ `/ __/  ' \/ -_) _ \  _     / / _ \/ _ \(_-<
##  \_,_/_/ /_/_/_/\__/_//_/ (_) __/ /\___/_.__/___/
##                              |___/
##
####### Ecosystème basé sur les microservices ##################### (c) 2018 losyme ####### @(°_°)@

package Gadget::Notification;

#md_# Gadget::Notification
#md_

use Exclus::Exclus;
use Moo;
use Types::Standard -types;
use Exclus::Data;
use Exclus::Util qw(create_uuid);
use namespace::clean;

#md_## Les attributs
#md_

#md_### id
#md_
has 'id' => (
    is => 'ro', isa => Str, default => sub { create_uuid() }
);

#md_### origin
#md_
has 'origin' => (
    is => 'ro', isa => Str, default => sub { 'armen' }
);

#md_### created_at
#md_
has 'created_at' => (
    is => 'ro', isa => Int, default => sub { time }
);

#md_### job_id
#md_
has 'job_id' => (
    is => 'ro', isa => Str, required => 1
);

#md_### data
#md_
has 'data' => (
    is => 'ro', isa => HashRef, required => 1
);

#md_## Les méthodes
#md_

#md_### unbless()
#md_
sub unbless {
    my ($self) = @_;
    my $data = {};
    $data->{$_} = $self->$_ foreach qw(id origin created_at job_id data);
    return $data;
};

#md_### get_data()
#md_
sub get_data { Exclus::Data->new(data => $_[0]->data) }

1;
__END__
