#######
##                                    _      __
##   ___ _______ _  ___ ___          (_)__  / /  ___
##  / _ `/ __/  ' \/ -_) _ \  _     / / _ \/ _ \(_-<
##  \_,_/_/ /_/_/_/\__/_//_/ (_) __/ /\___/_.__/___/
##                              |___/
##
####### Ecosystème basé sur les microservices ##################### (c) 2018 losyme ####### @(°_°)@

package Gadget::Types;

#md_# Gadget::Types
#md_

use Exclus::Exclus;
use Type::Library -base;
use Type::Utils -all;

BEGIN { extends 'Types::Standard' };

#md_## Les types
#md_

#md_### JobExclusivity
#md_
declare 'JobExclusivity', as enum[qw(NO ITSELF APP ALL)];

#md_### JobStatus
#md_
declare 'JobStatus', as enum[qw(TODO RUNNING PENDING SUCCEEDED FAILED ABORTED)];

#md_### WorkflowStatus
#md_
declare 'WorkflowStatus', as enum[qw(RUNNING SUCCEEDED FAILED ABORTED)];

1;
__END__
