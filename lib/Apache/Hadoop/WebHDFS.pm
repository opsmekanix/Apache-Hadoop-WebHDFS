#!/usr/bin/perl
package Apache::Hadoop::WebHDFS;
our $VERSION = "0.02";      
use warnings;
use strict;
use lib '.';
use parent 'WWW::Mechanize';
use Carp;

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


# TODO - need to add check in new for authmethod = 'pseudo, gssapi, or doas'

sub new {
	# Create new WebHDFS object
    my $class = shift;
	my $namenode =  'localhost';
	my $namenodeport= 50070;
	my $authmethod = 'gssapi';                # 3 values: gssapi, pseudo, doas
	my ($url, $urlpre, $urlauth, $user, $doas_user)  = undef;
    
	if ($_[0]->{'doas_user'}) { $doas_user =  $_[0]->{'doas_user'}; }
	if ($_[0]->{'namenode'}) { $namenode =  $_[0]->{'namenode'}; }
	if ($_[0]->{'namenodeport'}) { $namenodeport =  $_[0]->{'namenodeport'}; }
    if ($_[0]->{'authmethod'}) { $authmethod =  $_[0]->{'authmethod'}; }
    if ($_[0]->{'user'}) { $user =  $_[0]->{'user'}; }

    my $self = $class-> SUPER::new();

	$self->{'namenode'} = $namenode;
	$self->{'namenodeport'} = $namenodeport;
	$self->{'authmethod'} = $authmethod;
	$self->{'user'} = $user;
	$self->{'doas_user'} = $doas_user;
	$self->{'webhdfsurl'} = $url;
					        
	return $self;
}

# TODO add other supported authentication methods
#      proxy user and non-gssapi unsecure grids 
#      note: 'pseudo' non-gssapi method added.   still need to tst proxy user

# curl -i "http://<HOST>:<PORT>/webhdfs/v1/<PATH>?[user.name=<USER>&]op=..." or 
# curl -i "http://<HOST>:<PORT>/webhdfs/v1/<PATH>?[user.name=<USER>&]doas=<USER>&op=..."

sub getdelegationtoken {
    # Fetch delegation token and store in object
    my ( $self ) = @_; 
    my $token = '';
	my $url = 'http://' . $self->{'namenode'} . ':' . $self->{'namenodeport'} . '/webhdfs/v1/?op=GETDELEGATIONTOKEN&renewer=' . $self->{'user'};
	if ($self->{'authmethod'} eq 'gssapi') {
      $self->get( $url );
      if ( $self->success() ) { 
        $token = substr ($self->content(), 23 , -3);
      }
      $self->{'webhdfstoken'}=$token;
	} else {
		carp "getdelgation token only valid when using GSSAPI" ;
	}
    return $self;    
}

sub canceldelegationtoken {
    # Tell namenode to cancel existing delegation token and remove token from object
    my ( $self ) = @_; 
	if ($self->{'authmethod'} eq 'gssapi') { if ( $self->{'webhdfstoken'} )  {
   	   my $url = 'http://' . $self->{'namenode'} . ':' . $self->{'namenodeport'} . '/webhdfs/v1/?op=CANCELDELEGATION&token=' . $self->{'webhdfstoken'};
          $self->get( $url );
          delete $self->{'webhdfstoken'} 
       } 
	} else {
		carp "canceldelgation token only valid when using GSSAPI";
	}
    return $self;    
}

sub renewdelegationtoken {
    # Tell namenode to cancel existing delegation token and remove token from object
    my ( $self ) = @_; 
	if ($self->{'authmethod'} eq 'gssapi') {
       if ( $self->{'webhdfstoken'} )  {
   	   my $url = 'http://' . $self->{'namenode'} . ':' . $self->{'namenodeport'} . '/webhdfs/v1/?op=RENEWDELEGATION&token=' . $self->{'webhdfstoken'};
          $self->get( $url );
          delete $self->{'webhdfstoken'} 
       } 
	} else {
		carp "canceldelgation token only valid when using GSSAPI";
	}
    return $self;    
}

sub Open {
	# TODO need to add overwrite, blocksize, replication, and buffersize options
    my ( $self, $file ) = @_;

	#curl -i -L "http://<HOST>:<PORT>/webhdfs/v1/<PATH>?op=OPEN
	#                    [&offset=<LONG>][&length=<LONG>][&buffersize=<INT>]"
	# TODO implement offset, length, and buffersize 

    my $url;
	if ($self->{'authmethod'} eq 'gssapi') { 
       $url = 'http://' . $self->{'namenode'} . ':' . $self->{'namenodeport'} . '/webhdfs/v1' . $file . '?op=OPEN';	
	} elsif ( $self->{'authmethod'} eq 'pseudo' ) {
       croak ("I need a 'user' value if authmethod is 'none'") if ( !$self->{'user'} ) ;
       $url = 'http://' . $self->{'namenode'} . ':' . $self->{'namenodeport'} . '/webhdfs/v1' . $file . '?op=OPEN' . '&user.name=' . $self->{'user'};
	} elsif ( $self->{'authmethod'} eq 'doas' ) {
       croak ("I need a 'user' value if authmethod is 'doas'") if ( !$self->{'user'} ) ;
       croak ("I need a 'doas_user' value if authmethod is 'doas'") if ( !$self->{'doas_user'} ) ;
       $url = 'http://' . $self->{'namenode'} . ':' . $self->{'namenodeport'} . '/webhdfs/v1' . $file . '?op=OPEN' . '&user.name=' . $self->{'user'} . '&doas=' . $self->{'doas_user'};
	}
    if ( $self->{'webhdfstoken'} ) {
        $url = $url . "&delegation=" . $self->{'webhdfstoken'};
    }

    $self->get( $url );
    return $self;
}

sub getfilestatus {
    my ( $self, $file ) = @_;
	# curl -i  "http://<HOST>:<PORT>/webhdfs/v1/<PATH>?op=GETFILESTATUS"
    my $url;
	if ($self->{'authmethod'} eq 'gssapi') { 
       $url = 'http://' . $self->{'namenode'} . ':' . $self->{'namenodeport'} . '/webhdfs/v1' . $file . '?op=GETFILESTATUS';	
	} elsif ( $self->{'authmethod'} eq 'pseudo' ) {
       croak ("I need a 'user' value if authmethod is 'none'") if ( !$self->{'user'} ) ;
       $url = 'http://' . $self->{'namenode'} . ':' . $self->{'namenodeport'} . '/webhdfs/v1' . $file . '?op=GETFILESTATUS' . '&user.name=' . $self->{'user'};
	} elsif ( $self->{'authmethod'} eq 'doas' ) {
       croak ("I need a 'user' value if authmethod is 'doas'") if ( !$self->{'user'} ) ;
       croak ("I need a 'doas_user' value if authmethod is 'doas'") if ( !$self->{'doas_user'} ) ;
       $url = 'http://' . $self->{'namenode'} . ':' . $self->{'namenodeport'} . '/webhdfs/v1' . $file . '?op=GETFILESTATUS' . '&user.name=' . $self->{'user'} . '&doas=' . $self->{'doas_user'};
	}
    if ( $self->{'webhdfstoken'} ) {
        $url = $url . "&delegation=" . $self->{'webhdfstoken'};
    }
    $self->get( $url );
    return $self;
}

sub Delete {
	# TODO need to add overwrite, blocksize, replication, and buffersize options
    my ( $self, $file ) = @_;

	# curl -i -X DELETE "http://<host>:<port>/webhdfs/v1/<path>?op=DELETE
	#                                            [&recursive=<true|false>]"
	# TODO implement recursive

    my $url;
	if ($self->{'authmethod'} eq 'gssapi') { 
       $url = 'http://' . $self->{'namenode'} . ':' . $self->{'namenodeport'} . '/webhdfs/v1' . $file . '?&op=DELETE';	
	} elsif ( $self->{'authmethod'} eq 'pseudo' ) {
       croak ("I need a 'user' value if authmethod is 'none'") if ( !$self->{'user'} ) ;
       $url = 'http://' . $self->{'namenode'} . ':' . $self->{'namenodeport'} . '/webhdfs/v1' . $file . '?&op=DELETE' . '&user.name=' . $self->{'user'};
	} elsif ( $self->{'authmethod'} eq 'doas' ) {
       croak ("I need a 'user' value if authmethod is 'doas'") if ( !$self->{'user'} ) ;
       croak ("I need a 'doas_user' value if authmethod is 'doas'") if ( !$self->{'doas_user'} ) ;
       $url = 'http://' . $self->{'namenode'} . ':' . $self->{'namenodeport'} . '/webhdfs/v1' . $file . '?op=DELETE' . '&user.name=' . $self->{'user'} . '&doas=' . $self->{'doas_user'};
	}
    if ( $self->{'webhdfstoken'} ) {
        $url = $url . "&delegation=" . $self->{'webhdfstoken'};
    }

    $self->get( $url );
    return $self;
}

# TODO need delete method and specify if it's recursive or not

sub create {
	# TODO need to add overwrite, blocksize, replication, and buffersize options
	# curl -i -X PUT "http://<HOST>:<PORT>/webhdfs/v1/<PATH>?op=CREATE
	#                                         [&overwrite=<true|false>][&blocksize=<LONG>][&replication=<SHORT>]
	#                                         [&permission=<OCTAL>][&buffersize=<INT>]"
    my ( $self, $file_src, $file_dest, $perms ) = @_;
    if ( !$perms ) { $perms = '000'; }
    my $url;
	if ($self->{'authmethod'} eq 'gssapi') { 
       $url = 'http://' . $self->{'namenode'} . ':' . $self->{'namenodeport'} . '/webhdfs/v1' . $file_dest . '?&op=CREATE&permission=' . $perms . '&overwrite=false';	
	} elsif ( $self->{'authmethod'} eq 'pseudo' ) {
       croak ("I need a 'user' value if authmethod is 'none'") if ( !$self->{'user'} ) ;
       $url = 'http://' . $self->{'namenode'} . ':' . $self->{'namenodeport'} . '/webhdfs/v1' . $file_dest . '?&op=CREATE&permission=' . $perms . '&overwrite=false' . '&user.name=' . $self->{'user'};	
	} elsif ( $self->{'authmethod'} eq 'doas' ) {
       croak ("I need a 'user' value if authmethod is 'doas'") if ( !$self->{'user'} ) ;
       croak ("I need a 'doas_user' value if authmethod is 'doas'") if ( !$self->{'doas_user'} ) ;
       $url = 'http://' . $self->{'namenode'} . ':' . $self->{'namenodeport'} . '/webhdfs/v1' . $file_dest . '?&op=CREATE&permission=' . $perms . '&overwrite=false' . '&user.name=' . $self->{'user'} . '&doas=' . $self->{'doas_user'};
	}

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
	# curl -i -X PUT "http://<HOST>:<PORT>/<PATH>?op=MKDIRS[&permission=<OCTAL>]"
    my ( $self, $file, $perms ) = @_;
    if ( !$perms ) { $perms = '000'; }
    my $url;
	if ($self->{'authmethod'} eq 'gssapi') { 
       $url = 'http://' . $self->{'namenode'} . ':' . $self->{'namenodeport'} . '/webhdfs/v1' . $file . '?&op=MKDIRS&permission=' . $perms ;
	} elsif ( $self->{'authmethod'} eq 'pseudo' ) {
       croak ("I need a 'user' value if authmethod is 'none'") if ( !$self->{'user'} ) ;
       $url = 'http://' . $self->{'namenode'} . ':' . $self->{'namenodeport'} . '/webhdfs/v1' . $file . '?&op=MKDIRS&permission=' . $perms . '&user.name=' . $self->{'user'};	
	} elsif ( $self->{'authmethod'} eq 'doas' ) {
       croak ("I need a 'user' value if authmethod is 'doas'") if ( !$self->{'user'} ) ;
       croak ("I need a 'doas_user' value if authmethod is 'doas'") if ( !$self->{'doas_user'} ) ;
       $url = 'http://' . $self->{'namenode'} . ':' . $self->{'namenodeport'} . '/webhdfs/v1' . $file . '?&op=MKDIRS&permission=' . $perms . '&user.name=' . $self->{'user'} . '&doas=' . $self->{'doas_user'};
	}

    if ( $self->{'webhdfstoken'} ) {
        $url = $url . "&delegation=" . $self->{'webhdfstoken'};
    }
    $self->put( $url );
    return $self;
}

# TODO add a content summary method
sub getfilechecksum {
    # get and return checksum for a file
	# curl -i  "http://<HOST>:<PORT>/webhdfs/v1/<PATH>?op=GETFILECHECKSUM"
    my ( $self, $file ) = @_;
    my $url;
	if ($self->{'authmethod'} eq 'gssapi') { 
       $url = 'http://' . $self->{'namenode'} . ':' . $self->{'namenodeport'} . '/webhdfs/v1' . $file . '?op=GETFILECHECKSUM';
	} elsif ( $self->{'authmethod'} eq 'pseudo' ) {
       croak ("I need a 'user' value if authmethod is 'none'") if ( !$self->{'user'} ) ;
       $url = 'http://' . $self->{'namenode'} . ':' . $self->{'namenodeport'} . '/webhdfs/v1' . $file . '?op=GETFILECHECKSUM' .  '&user.name=' . $self->{'user'}; ;
	} elsif ( $self->{'authmethod'} eq 'doas' ) {
       croak ("I need a 'user' value if authmethod is 'doas'") if ( !$self->{'user'} ) ;
       croak ("I need a 'doas_user' value if authmethod is 'doas'") if ( !$self->{'doas_user'} ) ;
       $url = 'http://' . $self->{'namenode'} . ':' . $self->{'namenodeport'} . '/webhdfs/v1' . $file . '?op=GETFILECHECKSUM' . '&user.name=' . $self->{'user'} . '&doas=' . $self->{'doas_user'};
	}

    if ( $self->{'webhdfstoken'} ) {
        $url = $url . "&delegation=" . $self->{'webhdfstoken'};
    }
     
    $self->get( $url );
    return $self;
}
# TODO add a get home directory method
# TODO add a set permission  method for existing files
# TODO add a set owner  method for existing files
# TODO add a set replication  method for existing files
# TODO add a set atime or mtime  method for existing files
# TODO add hashmap for errors and method of returning them - maybe?

sub liststatus {
    # list contents of directory
    my ( $self, $file ) = @_;
	# curl -i  "http://<HOST>:<PORT>/webhdfs/v1/<PATH>?op=LISTSTATUS"

    my $url;
	if ($self->{'authmethod'} eq 'gssapi') { 
       $url = 'http://' . $self->{'namenode'} . ':' . $self->{'namenodeport'} . '/webhdfs/v1' . $file . '?op=LISTSTATUS';
	} elsif ( $self->{'authmethod'} eq 'pseudo' ) {
       croak ("I need a 'user' value if authmethod is 'none'") if ( !$self->{'user'} ) ;
       $url = 'http://' . $self->{'namenode'} . ':' . $self->{'namenodeport'} . '/webhdfs/v1' . $file . '?op=LISTSTATUS' .  '&user.name=' . $self->{'user'}; ;
	} elsif ( $self->{'authmethod'} eq 'doas' ) {
       croak ("I need a 'user' value if authmethod is 'doas'") if ( !$self->{'user'} ) ;
       croak ("I need a 'doas_user' value if authmethod is 'doas'") if ( !$self->{'doas_user'} ) ;
       $url = 'http://' . $self->{'namenode'} . ':' . $self->{'namenodeport'} . '/webhdfs/v1' . $file . '?op=LISTSTATUS' . '&user.name=' . $self->{'user'} . '&doas=' . $self->{'doas_user'};
	}

    if ( $self->{'webhdfstoken'} ) {
        $url = $url . "&delegation=" . $self->{'webhdfstoken'};
    }
    $self->get( $url );
    return $self;
}

sub rename {
	# curl -i -X PUT "<HOST>:<PORT>/webhdfs/v1/<PATH>?op=RENAME&destination=<PATH>"
    my ( $self, $src, $dst ) = @_;
	#my $url = 'http://' . $self->{'namenode'} . ':' . $self->{'namenodeport'} . '/webhdfs/v1' . $src . '?op=RENAME&destination=' . $dst;
	
    my $url;
	if ($self->{'authmethod'} eq 'gssapi') { 
       $url = 'http://' . $self->{'namenode'} . ':' . $self->{'namenodeport'} . '/webhdfs/v1/?' . $src . '&op=RENAME&destination=' . $dst . '&overwrite=false';	
	} elsif ( $self->{'authmethod'} eq 'pseudo' ) {
       croak ("I need a 'user' value if authmethod is 'none'") if ( !$self->{'user'} ) ;
       $url = 'http://' . $self->{'namenode'} . ':' . $self->{'namenodeport'} . '/webhdfs/v1/?' . $src . '&op=RENAME&destination=' . $dst . '&overwrite=false' . '&user.name=' . $self->{'user'};	
	} elsif ( $self->{'authmethod'} eq 'doas' ) {
       croak ("I need a 'user' value if authmethod is 'doas'") if ( !$self->{'user'} ) ;
       croak ("I need a 'doas_user' value if authmethod is 'doas'") if ( !$self->{'doas_user'} ) ;
       $url = 'http://' . $self->{'namenode'} . ':' . $self->{'namenodeport'} . '/webhdfs/v1/?' . $src . '&op=RENAME&destination=' . $dst . '&overwrite=false' . '&user.name=' . $self->{'user'} . '&doas=' . $self->{'doas_user'};
	}

    if ( $self->{'webhdfstoken'} ) {
        $url = $url . "&delegation=" . $self->{'webhdfstoken'};
    }
    $self->put( $url );
}


=pod

=head1 NAME

Apache::Hadoop::WebHDFS - interface to Hadoop's WebHDS API that supports GSSAPI (secure) access.

=head1 VERSION

Version 0.02

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

=head2 renewdelegationtoken()  - renews a delegation token from the namenode. 

=head2 canceldelegationtoken() - informs the namenode to invalidate the delegation token as it's no longer needed.

=head2 Open()                  - opens and reads a file on HDFS

=head2 create()                - creates and writes to a file on HDFS

=head2 rename()                - renames a file on HDFS.

=head2 getfilestatus()         - returns a json structure containing status of file or directory

=head2 liststatus()            - returns a json structure of contents inside a directory 

=head2 mkdirs()                - creates a directory on HDFS

=head2 getfilechecksum()       - gets HDFS checksum on file

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
                                                  namenodeport    =>"50070"});
  $hdfsclient->liststatus("/tmp");        
  print Dumper $hdfsclient->content()  if ( $hdfsclient->success() ) ;     

	  
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
