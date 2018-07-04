#######
##                                    _      __
##   ___ _______ _  ___ ___          (_)__  / /  ___
##  / _ `/ __/  ' \/ -_) _ \  _     / / _ \/ _ \(_-<
##  \_,_/_/ /_/_/_/\__/_//_/ (_) __/ /\___/_.__/___/
##                              |___/
##
####### Ecosystème basé sur les microservices ##################### (c) 2018 losyme ####### @(°_°)@

package Vortex::Backend::Plugin::MongoDB;

#md_# Vortex::Backend::Plugin::MongoDB
#md_

use Exclus::Exclus;
use Moo;
use Types::Standard qw(InstanceOf);
use namespace::clean;

extends qw(Obscur::Object);

#md_## Les attributs
#md_

#md_### _mongo
#md_
has '_mongo' => (
    is => 'ro',
    isa => InstanceOf['Exclus::Databases::MongoDB'],
    lazy => 1,
    default => sub { $_[0]->runner->build_resource('MongoDB', $_[0]->cfg) },
    init_arg => undef
);

#md_### _buckets
#md_
has '_buckets' => (
    is => 'ro',
    isa => InstanceOf['MongoDB::Collection'],
    lazy => 1,
    default => sub { $_[0]->_mongo->get_collection(qw(armen_Vortex buckets)) },
    init_arg => undef
);

#md_## Les méthodes
#md_

#md_### insert_bucket()
#md_
sub insert_bucket {
    my ($self, $bucket) = @_;
    $self->_buckets->insert_one($bucket);
}

#md_### maybe_insert_bucket()
#md_
sub maybe_insert_bucket {
    my ($self, $bucket, $category) = @_;
    return
        if defined $category && $self->_buckets->count_documents(
            {
                jobs => {
                    '$elemMatch' => {
                        category => $category,
                        '$or' => [
                            {status =>    'TODO'},
                            {status => 'RUNNING'},
                            {status => 'PENDING'}
                        ]
                    }
                }
            },
            {limit => 1}
        );
    $self->insert_bucket($bucket);
    return 1;
}

1;
__END__
