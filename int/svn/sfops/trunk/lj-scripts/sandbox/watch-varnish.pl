#!/usr/bin/perl
#
# This script is intended to be run by ops to monitor the status of
# the site's varnish servers.
#

use strict;
use warnings;
use Getopt::Long;
use FileHandle;
use Net::Telnet;
use Data::Dumper;
use FileHandle;
$|=1;

my $usage = <<USAGE;

$0 - Monitor Varnish Stats.
This script is intended to be run by ops to monitor the status of
the site's varnish servers.

Usage:
    $0 [options]
    Options:
        --help|h          Print this help and exit
        --fields|f        Gather stats for the specified fields only
        --servers|s       Gather stats for the specified servers only
        --iterations|i    Iterations
        --pool-file|p     Pool file to use
        --List-Fields|l   Print the list of legal field names on STDOUT

NOTES:

    --fields (default: client_conn,client_req,cache_hit) is a comma
             separated list of field names to display, the option may
             be specified multiple times.  Unknown field names are
             silently ignored.

    --servers is a comma separated list of server names to contact,
             the option may be specified multiple times.

    --iterations <forever|[nnnn[sS]] (string)> specify how long to run
             the program.  If this is a number, run that number of
             iterations of the main loop. If it is a number followed
             by [sS] run unitl that many seconds have elapsed.  The
             (case-insesnitive) string 'forever' without the quotes
             will put the program into an infinie loop.  In this case,
             terminate with C-c.  Default is 300s.

    --pool-file pool file containing server anmes one pr line.
             Default pool file is \$LJHOME/etc/pool_varnish.txt The
             pool file will not be reaqd if --servers option was used.
USAGE

##  get a list of valid field names and descriptions from some random
##  varnishserver
my $allFieldsCmd= "ssh bil1-varn20 varnishstat -l 2>&1|egrep -v '^Field|^----|^Varnishstat'";
my $allfieldsFH= FileHandle->new("$allFieldsCmd|");
die "Can't open connection for fields list: $!" unless defined($allfieldsFH);

my @allFields= map {
    my ($s, $d)= /^\s*(\S*)\s+(.*)/;
    {'symbol' => $s, 'desc' => $d}
} split(/\n/, do {local $/; my $txt= <$allfieldsFH>});
$allfieldsFH->close();

##  map field symbols to the matching descriptions
my %patHash= map {($_->{'symbol'}, $_->{'desc'})} @allFields;

########################################################################
##                          O P T I O N S                             ##
########################################################################
my %clOptions;
GetOptions(
    'help|h'               => \($clOptions{'help'}= 0),
    'fields|f=s@'          => $clOptions{'fields'}= [],
    'servers|s=s@'         => $clOptions{'servers'}= [],
    'iterations|i=s'       => \($clOptions{'iterations'}= '300s'),
    'pool-file|p=s'        => \($clOptions{'pool_file'}= ''),
    'list-fields|l'        => \($clOptions{'list_fields'}= 0),
) or die $usage;
##print "Options Hash==>>@{[Dumper(\%clOptions)]}\n";
##exit;

##  -h option?  print a mesage and exit
die $usage if ($clOptions{'help'});

##  -l option?  just print a list of fields and descriptions then exit
if ($clOptions{'list_fields'}) {
    for my $f (@allFields) {printf "%-30.30s %s\n", $f->{'symbol'}, $f->{'desc'}};
    exit;
}

##  -i option? limit program run time.
my %runTime= ('stop'=>0);
if (lc($clOptions{'iterations'}) eq 'forever') {
    $runTime{'limit'}= 'forever';

} else {
    my ($digits, $spec) = $clOptions{'iterations'} =~ /^([0-9]+)([sS])?$/;
    $spec= $spec ? $spec : '';
    die "invalid iterations" unless $digits > 0;

    $runTime{'limit'}= $spec ? 'seconds' : 'iterations';
    $runTime{'count'}= $digits;
}
##  print "Run Time hash==>>@{[Dumper(\%runTime)]}\n";
##  exit;

##  get a hash of the field symbols which need to be displayed
my @defaultFieldSymbols= ('client_conn,client_req,cache_hit',);
$clOptions{'fields'}= \@defaultFieldSymbols unless @{$clOptions{'fields'}};
my %optFields=();
for my $optionString (@{$clOptions{'fields'}}) {
    for my $optionSymbol (split(',', $optionString)) {
        next unless exists($patHash{$optionSymbol});
        $optFields{$optionSymbol}= 1;
    }
}
$optFields{'uptime'}= 1;  ##  always include uptime
##print "Fields Hash==>>@{[Dumper(\%optFields)]}\n";
##exit;
die "fields array is empty.  nothing to do\n" if ((keys(%optFields)) <= 1);

##  establish an ordering relation.  this is the order that fields
##  will appear on the output line
for my $symb (keys(%optFields)) {$optFields{$symb}= optFieldOrder($symb)};
sub optFieldOrder {
    my ($s)= @_;
    for (my $i=0; $i<@allFields; $i++) {
        return $i if ($allFields[$i]->{'symbol'} eq $s);
    }
}

##  get servers
my %varnishServers;
if (@{$clOptions{'servers'}}) {
    for my $serverString (@{$clOptions{'servers'}}) {
        for my $serverName (split(',', $serverString)) {
            $varnishServers{$serverName}= 1;
        }
    }
}
##  print "Varnish Servers (from options)==>>@{[Dumper(\%varnishServers)]}\n";
##  exit;
########################################################################
##                      E N D    O P T I O N S                        ##
########################################################################
##
##  if no servers were specified on the command line, then read the
##  server pool file
unless (keys(%varnishServers)){
    my $poolFile = $clOptions{'pool_file'} || "$ENV{LJHOME}/etc/pool_varnish.txt";
    die "Can't determine pool file" unless defined($poolFile);

    my $poolFH= FileHandle->new("< $poolFile");
    die "Can't open varnish pool file: $poolFile, $!" unless defined($poolFH);

    ##  read the file as one long string; split on newlines; grep out
    ##  comments; build a hash (in case of duplicates).  all
    ##  whitespace is removed in the map{}
    %varnishServers= map {s/\s+//g; ($_,1)} grep {!/^#/} split(/\n/, do {local $/; my $txt= <$poolFH>});
    close $poolFH;
}
die "servers array is empty.  nothing to do\n" unless keys(%varnishServers);
my @varnishServers= sort keys(%varnishServers);
##print "Servers array ==>>@{[Dumper(\@varnishServers)]}\n";
##exit;

##  construct a matching pattern for field descriptions.
my $descPattern='';
for my $symbol (keys(%optFields)) {
    next unless exists($patHash{$symbol});
    $descPattern .= (length($descPattern) ? '|' : '') . $patHash{$symbol}
}
$descPattern= "($descPattern)\$";
##print "\$descPattern= $descPattern\n";

$SIG{INT} = \&programOff;
sub programOff {
    $runTime{'stop'}= 1;
    $runTime{'reason'}= "Interrupted by signal";
}
my %varnishServerStats;

#########################################################################
##                      S U B R O U T I N E S                          ##
#########################################################################
##  Loop Control.  There are a nmber of ways to terminate the program.
##        --  stop after a given time period
##        --  stop after a given number of iterations
##        --  stop on receipt of SIGINT
##  See the Options code or look at sage or --help
my %checkIterate= ('iterations' => \&countIterations, 'seconds' => \&countSeconds);
sub loopControl {
    my ($rh)= @_;

    return 0 if $rh->{'stop'};
    return 1 unless defined ($checkIterate{$rh->{'limit'}});
    return 1 unless (ref($checkIterate{$rh->{'limit'}}) eq 'CODE');
    return $checkIterate{$rh->{'limit'}}->($rh);
}

##  down counter for loop iterations
sub  countIterations{
    my ($rh)= @_;
    $rh->{'maxIterations'}= $rh->{'count'} unless defined($rh->{'maxIterations'});
    if ((--$rh->{'maxIterations'}) < 0) {
        $rh->{'reason'}= "Max (${\($rh->{'count'})}) iterations reached";
        $rh->{'stop'}= 1;
    }
    ##print "countIterations() runTime hash==>>   @{[Dumper(\%runTime)]}\n";
    return(!$rh->{'stop'});
}

##  down counter for time period
sub  countSeconds{
    my ($rh)= @_;
    $rh->{'startTime'}= time unless defined($rh->{'startTime'});
    my $checktime= time;
    if ((time() - $rh->{'startTime'}) > $rh->{'count'}) {
        $rh->{'reason'}= "Time limit exceeded";
        $rh->{'stop'}= 1;
    }
    return (!$rh->{'stop'});
}


##  Given A host name, initialize a telnet objct for that host
sub initializeTelnet {
    my ($s)= @_;
    my $telnet = new Net::Telnet();
    $telnet->open(Host => $s,Port => 6082, Timeout => 10);
    my $errmsg='OK';

    ##  skip lines to 'quit'
    while (1) {
        my $line= $telnet->getline(Errmode => "return", Timeout => 5);
        if (!$line) {
            $errmsg= $telnet->errmsg();
            last;
        }
        last if $line =~ /'quit'/;
    }
    return undef unless $errmsg eq 'OK';

    ##  skip until empty line
    while (1) {
        my $line= $telnet->getline(Errmode => "return", Timeout => 1);
        if (!$line) {
            $errmsg= $telnet->errmsg();
            last;
        }
        chomp $line;
        last unless length($line);
    }
    return undef unless $errmsg eq 'OK';
    return($telnet);
}

sub getServerStats {
    my ($time)= @_;

    for my $server (@varnishServers) {
        my $p= $varnishServerStats{$server};  ##  for convenience.  a pointer.
        $p->{'sampleTime'}= $time;

        my $ap= getCurrentStats($server);

        if (arraysDifferent($p->{'current'}, $ap)) {
            if (scalar(@{$p->{'current'}}) && arraysDifferent($p->{'current'}, $p->{'last'})){
                $p->{'last'}= $p->{'current'};
            }
            $p->{'current'}= $ap;
        }
        $p->{'last'}= $p->{'current'} unless  scalar(@{$p->{'last'}});
    }
}
sub arraysDifferent {
    my ($p1, $p2)= @_;
    return 1 if (scalar(@$p1) != scalar(@$p2));
    my %count;

    foreach my $e (@$p1, @$p2) {$count{$e}++};
    foreach my $e (keys(%count)) {return 1 if ($count{$e} != 2)};
    return 0;
}
sub dumpServerStats {
   print "\n\n${\(fmtHeaderLine())}\n";

    for my $server (@varnishServers) {
        printf("%-15.15s %s\n", $server, serverStats($server));
    }
}
sub fmtHeaderLine {
    my $ret= '';
    $ret .= sprintf("%-15.15s", 'server');
    for my $field (sort {$optFields{$a} <=> $optFields{$b}} keys(%optFields)) {
        next if ($field eq 'uptime');
        $ret .= sprintf("%20.20s", $field)
    }
    return "$ret";
}
sub serverStats {
    my ($s)= @_;
    my $ret='';
    my $deltaSeconds=1;

    ##  no telnet obj:  Couldn't connect to varnish server
    return "No Connection to server" unless defined($varnishServerStats{$s}->{'telnetObj'});

    ##  empty array:  timed out reading data.
    my @current= @{$varnishServerStats{$s}->{'current'}};
    return "No Data this Cycle" unless scalar(@current);

    my @last= @{$varnishServerStats{$s}->{'last'}};

    for (my $i=0; $i < scalar(@current); $i++) {
        my @cur= $current[$i] =~ /\s*(\S*)\s*(.*)$/;
        my @lst= $last[$i] =~ /\s*(\S*)\s*(.*)$/;
        next unless $cur[1] eq 'Client uptime';
        last unless $cur[0] > $lst[0];
        $deltaSeconds= $cur[0] - $lst[0];
    }

    for (my $i=0; $i < scalar(@current); $i++) {
        my @cur= $current[$i] =~ /\s*(\S*)\s*(.*)$/;
        my @lst= $last[$i] =~ /\s*(\S*)\s*(.*)$/;
        next if $cur[1] eq 'Client uptime';
        $ret .= sprintf("%20.20s ", sprintf("%7d %+7d",
                                            $cur[0], (($cur[0] - $lst[0]) / $deltaSeconds)));
    }
    return $ret;
}

sub getCurrentStats {
    my ($s)= @_;
    my $telnet= $varnishServerStats{$s}->{'telnetObj'};
    return [] unless $telnet;
    my $errmsg= 'OK';

    my @lines=();
    $telnet->print('stats');
    while (1) {
        my $line= $telnet->getline(Errmode => "return", Timeout => 1);
        if (!$line) {
            $errmsg= $telnet->errmsg();
            @lines=();
            last;
        }
        chomp $line;
        last unless length($line);
        push @lines, $line;
    }

    $varnishServerStats{$s}->{'getCurrentStatsMessage'}= $errmsg;
    $varnishServerStats{$s}->{'getCurrentStatsRetcode'}= shift @lines if @lines;
    my @greppedLines= grep {/$descPattern/} (@lines);
    return(\@greppedLines);
}
#########################################################################
##                             M A I N                                 ##
#########################################################################
for my $server (@varnishServers) {
    $varnishServerStats{$server}->{'telnetObj'}= initializeTelnet($server);
    $varnishServerStats{$server}->{'current'}= [];
    $varnishServerStats{$server}->{'last'}= [];
    $varnishServerStats{$server}->{'name'}= $server;
}

while (loopControl(\%runTime)) {
    my $loopTime= time;

    getServerStats($loopTime);
    dumpServerStats();

    ##  if there are unconnected servers then try to reconnect
    my @noConnects;
    for my $server (@varnishServers) {
        push(@noConnects, $server) unless defined($varnishServerStats{$server}->{'telnetObj'});
    }
    ##print "noConnects==>>@{[Dumper(\@noConnects)]}\n";
    if (scalar(@noConnects)) {
        my $server= $noConnects[int(rand(scalar(@noConnects)))];
        print "trying to initialize server $server\n";
        $varnishServerStats{$server}->{'telnetObj'}= initializeTelnet($server);
    }

    sleep 1;
}

for my $server (@varnishServers) {
    next unless $varnishServerStats{$server}->{'telnetObj'};
    $varnishServerStats{$server}->{'telnetObj'}->close();
    $varnishServerStats{$server}->{'telnetObj'}= undef;
}
print "Quitting.  ${\($runTime{'reason'})}\n";
exit;
