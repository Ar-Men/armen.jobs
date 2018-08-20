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

extends qw(Obscur::Databases::MongoDB);

#md_## Les attributs
#md_

#md_### _buckets
#md_
has '_buckets' => (
    is => 'ro',
    isa => InstanceOf['MongoDB::Collection'],
    lazy => 1,
    default => sub { $_[0]->get_collection(qw(armen_Vortex buckets)) },
    init_arg => undef
);

#md_## Les méthodes
#md_

#md_### foreach_running_buckets()
#md_
sub foreach_running_buckets {
    my ($self, $cb) = @_;
    my $bucket;
    my $cursor = $self->_buckets->find({'job.status' => 'RUNNING'});
    while ($bucket = $cursor->next) {
        last unless $cb->($bucket);
    }
}

#md_### foreach_ordered_buckets()
#md_
sub foreach_ordered_buckets {
    my ($self, $cb) = @_;
    my $bucket;
    my $cursor = $self->_buckets->find(
        {
            '$or' => [
                {'job.status' =>    'TODO'},
                {'job.status' => 'PENDING'}
            ],
            'job.run_after' => {'$lte' => time}
        },
        {sort => ['job.priority' => -1, 'job.reference_time' => 1]}
    );
    while ($bucket = $cursor->next) {
        last unless $cb->($bucket);
    }
}

#md_### get_bucket()
#md_
sub get_bucket {
    my ($self, $job_id) = @_;
    return $self->_buckets->find_one({'job.id' => $job_id});
}

#md_### insert_bucket()
#md_
sub insert_bucket {
    my ($self, $bucket) = @_;
    $self->_buckets->insert_one({_id => $bucket->id, %{$bucket->unbless}});
}

#md_### maybe_insert_bucket()
#md_
sub maybe_insert_bucket {
    my ($self, $bucket, $category) = @_;
    return
        if defined $category && $self->_buckets->count_documents(
            {
                'job.category' => $category,
                '$or' => [
                    {'job.status' =>    'TODO'},
                    {'job.status' => 'RUNNING'},
                    {'job.status' => 'PENDING'}
                ]
            },
            {limit => 1}
        );
    $self->insert_bucket($bucket);
    return 1;
}

#md_### replace_bucket()
#md_
sub replace_bucket {
    my ($self, $bucket) = @_;
    $self->_buckets->replace_one({_id => $bucket->id}, $bucket->unbless);
}

#md_### clean()
#md_
sub clean {
    my ($self) = @_;
    # Suppression des buckets qui ce sont terminés avec succès
    $self->_buckets->delete_many(
        {'$or' => [{'workflow.status' => 'SUCCEEDED'}, {workflow => undef, 'job.status' => 'SUCCEEDED'}]}
    );
}

1;
__END__
