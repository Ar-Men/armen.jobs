#######
##                                    _      __
##   ___ _______ _  ___ ___          (_)__  / /  ___
##  / _ `/ __/  ' \/ -_) _ \  _     / / _ \/ _ \(_-<
##  \_,_/_/ /_/_/_/\__/_//_/ (_) __/ /\___/_.__/___/
##                              |___/
##
####### Ecosystème basé sur les microservices ##################### (c) 2018 losyme ####### @(°_°)@

package Gadget::Jobs::Attributes;

#md_# Gadget::Jobs::Attributes
#md_

use Exclus::Exclus;
use Carp;
use Exclus::Exceptions;
use Exclus::Util qw(monkey_patch);

#md_## Les méthodes
#md_

#md_### import()
#md_
sub import {
    my ($class) = @_;
    my $target = caller;

    my $has = do { no strict 'refs'; \&{"${target}::has"} }
        or croak "$target is not a Moo class or role";

    my $create_attribute = sub {
        my ($attr, $name, $is, $isa, $default_value) = @_;
        my $default = @_ > 4;
        my $key = substr($name, 0, 1) eq '_' ? substr($name, 1) : $name;
        monkey_patch(
            $target,
            $name,
            sub {
                my $self = shift;
                my $cb_set_value = sub {
                    my $value = shift;
                    unless ($isa->check($value)) {
                        EX->throw({ ##//////////////////////////////////////////////////////////////////////////////////
                            message => "Cette valeur pour cet attribut de job n'est pas valide",
                            params  => [job => $target, attr => $attr, name => $name, value => $value]
                        });
                    }
                    $self->$attr->{$key} = $value;
                };
                if (@_) {
                    $cb_set_value->($_[0]);
                } else {
                    if (!exists $self->$attr->{$key}) {
                        if ($default) {
                            $cb_set_value->($default_value);
                        }
                        else {
                            EX->throw({ ##//////////////////////////////////////////////////////////////////////////////
                                message => "Cet attribut de job n'est pas initialisé",
                                params  => [job => $target, attr => $attr, name => $name]
                            });
                        }
                    }
                    return $self->$attr->{$key};
                }
            }
        );
        my @properties = (
            is       => $is,
            isa      => $isa,
            lazy     => 0,
            reader   => "${target}::_access1_$name",
            required => 0,
            init_arg => undef
        );
        push @properties, writer => "${target}::_access2_$name"
            if $is eq 'rw';
        $has->($name => @properties);
    };
    monkey_patch($target, 'private', sub { $create_attribute->('private', @_) });
    monkey_patch($target, 'public',  sub { $create_attribute->('public',  @_) });
}

1;
__END__
