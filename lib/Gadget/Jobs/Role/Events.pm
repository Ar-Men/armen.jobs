#######
##                                    _      __
##   ___ _______ _  ___ ___          (_)__  / /  ___
##  / _ `/ __/  ' \/ -_) _ \  _     / / _ \/ _ \(_-<
##  \_,_/_/ /_/_/_/\__/_//_/ (_) __/ /\___/_.__/___/
##                              |___/
##
####### Ecosystème basé sur les microservices ##################### (c) 2018 losyme ####### @(°_°)@

package Gadget::Jobs::Role::Events;

#md_# Gadget::Jobs::Role::Events
#md_

use Exclus::Exclus;
use Moo::Role;
use Types::Standard -types;
use Exclus::Exceptions;
use Exclus::Util qw($_call_if_can);

#md_## Les attributs
#md_

#md_### _events
#md_
has '_events' => (
    is => 'ro', isa => HashRef[CodeRef], default => sub { {} }, init_arg => undef
);

#md_## Les méthodes
#md_

requires qw(pending);

#md_### _get_next_event()
#md_
sub _get_next_event { return $_[0]->private->{__next_event} }

#md_### set_next_event()
#md_
sub set_next_event { $_[0]->private->{__next_event} = $_[1] }

#md_### on()
#md_
sub on {
    my ($self, $event, $cb) = @_;
    my $events = $self->_events;
    if (exists $events->{$event}) {
        EX->throw({ ##//////////////////////////////////////////////////////////////////////////////////////////////////
            message => "Cet évènement existe déjà pour ce job",
            params  => [event => $event, job => $self->id]
        });
    }
    $events->{$event} = $cb;
}

#md_### _emit()
#md_
sub _emit {
    my ($self, $event) = @_;
    my $events = $self->_events;
    $self->logger->info('Job event', [name => $event]);
    unless (exists $events->{$event}) {
        EX->throw({ ##//////////////////////////////////////////////////////////////////////////////////////////////////
            message => "Cet évènement n'existe pas pour ce job",
            params  => [event => $event, job => $self->id]
        });
    }
    return $events->{$event}();
}

#md_### try_run()
#md_
sub try_run {
    my ($self) = @_;
    $self->$_call_if_can('on_events');
    my $event = $self->_get_next_event // '__run';
    my $end;
    while ($event && !$end) {
        ($event, $end) = $self->_emit($event);
        $self->set_next_event($event);
    };
}

#md_### do_it_again()
#md_
sub do_it_again {
    my ($self) = @_;
    $self->pending(0);
    return '__run';
}

#md_### come_back_later()
#md_
sub come_back_later {
    my ($self, $event, $delay) = @_;
    $self->pending($delay);
    return ($event, 1);
}

1;
__END__
