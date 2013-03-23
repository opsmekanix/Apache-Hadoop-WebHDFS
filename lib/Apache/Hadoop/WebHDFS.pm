#!/usr/bin/perl
package Apache::Hadoop::WebHDFS;
our $VERSION = "0.01";      
use warnings;
use strict;
use lib '.';
use parent 'WWW::Mechanize';
require Carp;

# ###################
# calls we care about
# $m -> get('http://url.com')  Does a get on that url
# $m -> put('http://blah.com', content=$content)
#
# $m -> success()  boolean if last request was success
# $m -> content()  content of request, which can be formated
# $m -> ct()       content type returned, ie: 'application/json'
# $m -> status()   HTTP status code of response

sub redirect_ok {
    # need to allow 'put' to follow redirect on 307 requests, per RFC 2616 section 10.3.8
    # redirect_ok is part of LWP::UserAgent which is subclassed 
    # by WWW:Mech and finally Apache::Hadoop::WebHDFS. 
    return 1;    # always return true.
}


sub new {
	# Create new WebHDFS object
    my $class = shift;
	my $namenode =  'localhost';
	my $namenodeport= 50070;
	if ($_[0]->{'namenode'}) { $namenode =  $_[0]->{'namenode'}; }
	if ($_[0]->{'namenodeport'}) { $namenodeport =  $_[0]->{'namenodeport'}; }
    my $self = $class-> SUPER::new();
	$self->{'namenode'} = $namenode;
	$self->{'namenodeport'} = $namenodeport;
	return $self;
}

# TODO add other supported authentication methods
#      proxy user and non-gssapi unsecure grids

# TODO add renew delegation token method

sub getdelegationtoken {
    # Fetch delegation token and store in object
    my ( $self, $user) = @_; 
    my $token = '';
	my $url = 'http://' . $self->{'namenode'} . ':' . $self->{'namenodeport'} . '/webhdfs/v1/?op=GETDELEGATIONTOKEN&renewer=' . $user;
    $self->get( $url );
    if ( $self->success() ) { 
       $token = substr ($self->content(), 23 , -3);
    }
    $self->{'webhdfstoken'}=$token;
    return $self;    
}

sub canceldelegationtoken {
    # Tell namenode to cancel existing delegation token and remove token from object
    my ( $self ) = @_; 
    if ( $self->{'webhdfstoken'} )  {
	   my $url = 'http://' . $self->{'namenode'} . ':' . $self->{'namenodeport'} . '/webhdfs/v1/?op=CANCELDELEGATION&token=' . $self->{'webhdfstoken'};
       $self->get( $url );
       delete $self->{'webhdfstoken'} 
    } 
    return $self;    
}

sub Open {
    my ( $self, $file ) = @_;
	my $url = 'http://' . $self->{'namenode'} . ':' . $self->{'namenodeport'} .  '/webhdfs/v1' . $file . '?op=OPEN';
    if ( $self->{'webhdfstoken'} ) {
        $url = $url . "&delegation=" . $self->{'webhdfstoken'};
    }
    $self->get( $url );
    return $self;
}

sub getfilestatus {
    my ( $self, $file ) = @_;
	my $url = 'http://' . $self->{'namenode'} . ':' . $self->{'namenodeport'} .  '/webhdfs/v1' . $file . '?op=GETFILESTATUS';
    if ( $self->{'webhdfstoken'} ) {
        $url = $url . "&delegation=" . $self->{'webhdfstoken'};
    }
    $self->get( $url );
    return $self;
}

# TODO need delete method and specify if it's recursive or not

sub create {
	# TODO need to add overwrite, blocksize, replication, and buffersize options
    my ( $self, $file_src, $file_dest, $perms ) = @_;
    if ( !$perms ) { $perms = '000'; }
	my $url = 'http://' . $self->{'namenode'} . ':' . $self->{'namenodeport'} .  ':50070/webhdfs/v1' . $file_dest . '?op=CREATE&permission=' . $perms . '&overwrite=false';
    if ( $self->{'webhdfstoken'} ) {
        $url = $url . "&delegation=" . $self->{'webhdfstoken'};
    }
    # TODO: implement File::Map and means to handle binmode.  For now we slurp ... :(
    my $content;
    {
        local $/;    # unset record seperator so we can put everything in a single string
        open my $fh, '<', $file_src or die "can't open $file_src: $!";
        $content = <$fh>;
        close $fh;
    }
    $self->put( $url, content => $content );
    return $self;
}

# TODO need to add 'append' method  - for people wanting to corrupt hdfs. :)

sub mkdirs {
    my ( $self, $file, $perms ) = @_;
    if ( !$perms ) { $perms = '000'; }
	my $url = 'http://' . $self->{'namenode'} . ':' . $self->{'namenodeport'} .  '/webhdfs/v1' . $file . '?op=MKDIRS&permission=' . $perms;
    if ( $self->{'webhdfstoken'} ) {
        $url = $url . "&delegation=" . $self->{'webhdfstoken'};
    }
    $self->put( $url );
    return $self;
}

# TODO add a content summary method
# TODO add a get file checksum method
# TODO add a get home directory method
# TODO add a set permission  method for existing files
# TODO add a set owner  method for existing files
# TODO add a set replication  method for existing files
# TODO add a set atime or mtime  method for existing files
# TODO add hashmap for errors and method of returning them - maybe?

sub liststatus {
    # list contents of directory
    my ( $self, $file ) = @_;
	my $url = 'http://' . $self->{'namenode'} . ':' . $self->{'namenodeport'} . '/webhdfs/v1' . $file . '?op=LISTSTATUS&recrusive=true';
    if ( $self->{'webhdfstoken'} ) {
        $url = $url . "&delegation=" . $self->{'webhdfstoken'};
    }
    $self->get( $url );
    return $self;
}

sub rename {
    my ( $self, $src, $dst ) = @_;
	my $url = 'http://' . $self->{'namenode'} . ':' . $self->{'namenodeport'} . '/webhdfs/v1' . $src . '?op=RENAME&destination=' . $dst;
    if ( $self->{'webhdfstoken'} ) {
        $url = $url . "&delegation=" . $self->{'webhdfstoken'};
    }
    $self->put( $url );
}


=pod

=head1 NAME

Apache::Hadoop::WebHDFS - interface to Hadoop's WebHDS API that supports GSSAPI (secure) access.

=head1 VERSION

Version 0.01

=head1 SYNOPSIS

Hadoop's WebHDFS API, is a rest interface to HDFS.  This module provides 
a perl interface to the API, allowing one to both read and write files to 
HDFS.  Because Apache::Hadoop::WebHDFS supports GSSAPI, it can be used to 
interface with unsecure and secure Hadoop Clusters.

Apache::Hadoop::WebHDFS is a subclass of WWW:Mechanize, so one could 
reference WWW::Mechanize methods if needed.  One will note that 
WWW::Mechanize is a subclass of LWP, meaning it's possible to also reference 
LWP methods from Apache::Hadoop::WebHDFS.

=head1 METHODS

=head2 new()                   - creates a new WebHDFS object.  Takes an anonomous hash with namenode and namenode port as keys. If not specified defaults to localhost and 50070.

=head2 getdelegationtoken()    - gets a delegation token from the namenode. 

=head2 canceldelegationtoken() - informs the namenode to invalidate the delegation token as it's no longer needed.

=head2 Open()                  - opens and reads a file on HDFS

=head2 create()                - creates and writes to a file on HDFS

=head2 rename()                - renames a file on HDFS.

=head2 getfilestatus()         - returns a json structure containing status of file or directory

=head2 liststatus()            - returns a json structure of contents inside a directory 

=head2 mkdirs()                - creates a directory on HDFS


=head1 GSSAPI Debugging

To see GSSAPI calls during the request, enable LWP::Debug by adding 
'use LWP::Debug qw(+);' to your script.

=head1 REQUIREMENTS

Carp                   is used for various warnings and errors.
WWW::Mechanize         is needed as this is a subclass.
LWP::Debug             is required for debugging GSSAPI connections
LWP::Authen::Negotiate is the magic sauce for working with secure hadoop clusters 
parent                 included with Perl 5.10.1 and newer or found on CPAN 
                       for older versions of perl

=head1 EXAMPLES

=head2 list a HDFS directory on a secure hadop cluster

  #!/bin/perl
  use Data::Dumper;
  use Authen::Krb5::Effortless;  # <-- to get TGT from kerberos
  use Apache::Hadoop::WebHDFS;
  my $username=getlogin();
  my $krb5=Authen::Krb5::Effortless->new();
  $krb5->fetch_TGT_PW('s3kr3+', $username);
  my $hdfsclient = Apache::Hadoop::WebHDFS->new( {namenode       =>"mynamenode.example.com",
                                                  mynamenodeport =>50070}
											   );
  $hdfsclient->liststatus("/tmp");        
  print Dumper $content  if ( $hdfsclient->success() ) ;     

	  
=head1 AUTHOR

Adam Faris, C<< <apache-hadoop-webhdfs at mekanix.org> >>

=head1 BUGS

  Please use github to report bugs and feature requests 
  https://github.com/opsmekanix/Apache-Hadoop-WebHDFS/issues

=head1 SUPPORT

You can find documentation for this module with the perldoc command.

    perldoc Apache::Hadoop::WebHDFS


You can also look for information at:

=over 4

=item * AnnoCPAN: Annotated CPAN documentation

L<http://annocpan.org/dist/Apache-Hadoop-WebHDFS>

=item * CPAN Ratings

L<http://cpanratings.perl.org/d/Apache-Hadoop-WebHDFS>

=item * Search CPAN

L<http://search.cpan.org/dist/Apache-Hadoop-WebHDFS/>

=back


=head1 ACKNOWLEDGEMENTS

I would like to acknowledge Andy Lester plus the numerous people who have 
worked on WWW::Mechanize, Anchim Grolms and team for providing 
LWP::Authen::Negotiate, and the contributors to LWP.  Thanks for providing 
awesome modules.

=head1 LICENSE AND COPYRIGHT

Copyright 2013 Adam Faris.

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

    L<http://www.apache.org/licenses/LICENSE-2.0>

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.


=cut


return 1;
