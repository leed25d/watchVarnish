#!/usr/bin/perl
#
# This program called by [x]inetd with root permissions.
#

use strict;

sub dispatch;

my $VERSION = "2.3";
my $verbose = 1;
my $VARNISHSTAT='/usr/bin/varnishstat';

# TODO: add help cmd
my $cmd = {
    # Get information
    'version'           => sub { print "OK $VERSION\n" },

    # varnish control
    'varn_stats'        => \&varn_stats,
    'varn_stats_full'   => \&varn_stats_full,
};


$| = 1;
while (my $req = <STDIN>) {
    dispatch($req);
}
exit 0;


########################################################################
##                      S U B R O U T I N E S                         ##
########################################################################

# Input and dispatch commands.
sub dispatch {
    my $req = shift;
    $req =~ s/\r?\n$//;
    my $args;
    if ($req =~ s/\s+(.+)//) {
        $args = $1;
    }
    if (my $csub = $cmd->{$req}) {
        $csub->($args);
        return;
    }
    print "ERROR: unknown command $req\n" if $req;
}



# Commands processing
sub varnStatsGet{
    ##  make sure that we are on a varnish machine
    my $retString='';
    if (! -e $VARNISHSTAT) {
        $retString= "ERROR:  This is not a Varnish machine";
        $retString .="\n\$VARNISHSTAT: '$VARNISHSTAT' could not be found";

    } else {
        my $stats= qx/$VARNISHSTAT -1/;
        my $exitCode= $?;
        if ($exitCode) {
            $retString= "ERROR: $VARNISHSTAT failed with exit code: $exitCode";

        } else {
            $retString= $stats;
        }
    }
    return  $retString;
}
sub varn_stats {
    my $vs= &varnStatsGet();
    if ($vs !~ /^ERROR:/) {
        $vs =~ s/\S+\s+(\S+)\s+\S+\s+(.*\n)/ \1 \2/mg;
    }
    print "$vs\n";
    return;
}
sub varn_stats_full {
    my $vs= &varnStatsGet();
    print "$vs\n";
    return;
}
