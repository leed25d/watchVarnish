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

my $connectTimeoutSecs= 8;
my $readTimeoutSecs=1;
my $slewTimeoutSecs=1;

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
my %symbHash= map {($_->{'symbol'}, $_->{'desc'})} @allFields;
my %descHash= map {($_->{'desc'}, $_->{'symbol'})} @allFields;

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
        next unless exists($symbHash{$optionSymbol});
        $optFields{$optionSymbol}= 1;
    }
}
$optFields{'uptime'}= 1;  ##  always include uptime

##print "Fields Hash==>>@{[Dumper(\%optFields)]}\n";
##exit;
die "fields array is empty.  nothing to do\n" if ((keys(%optFields)) <= 1);

#  establish an ordering relation.  this is the order that fields
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
my @descAry= ();
for my $symbol (keys(%optFields)) {
    next unless exists($symbHash{$symbol});
    $descPattern .= (length($descPattern) ? '|' : '') . $symbHash{$symbol};
    push(@descAry, $symbHash{$symbol});
}

##the descriptions array needs to be sorted into display order
@descAry= sort {of($a) <=> of($b)} (@descAry);
sub of {
    my ($p)= @_;
    return($optFields{$descHash{$p}});
}

$descPattern= "($descPattern)\$";
##  print "\$descPattern= $descPattern\n";
##  print "\@descAry= @{[Dumper(\@descAry)]}\n";

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


##  Given A host name, initialize a telnet object for that host
sub initializeTelnet {
    my ($s)= @_;
    my $telnet = new Net::Telnet();
    $telnet->open(Host => $s,Port => 6082, Timeout => $connectTimeoutSecs);
    my $errmsg='OK';

    ##  skip lines to 'quit'
    while (1) {
        my $line= $telnet->getline(Errmode => "return", Timeout => $slewTimeoutSecs);
        if (!$line) {
            $errmsg= $telnet->errmsg();
            last;
        }
        last if $line =~ /'quit'/;
    }
    return undef unless $errmsg eq 'OK';

    ##  skip until empty line
    while (1) {
        my $line= $telnet->getline(Errmode => "return", Timeout => $readTimeoutSecs);
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

##  two arrays are the same if
##    --they have the same cardinality AND
##    --their values are pairwise equivalent
sub arraysDifferent {
    my ($p1, $p2)= @_;
    return 1 if (scalar(@$p1) != scalar(@$p2));
    my %count;

    foreach my $e (@$p1, @$p2) {$count{$e}++};
    foreach my $e (keys(%count)) {return 1 if ($count{$e} != 2)};
    return 0;
}

my $ret= '';
sub fmtHeaderLine {
    ##  only build it the first time.
    return $ret if $ret;

    $ret .= sprintf("%-15.15s ", 'server');
    for my $field (sort {$optFields{$a} <=> $optFields{$b}} keys(%optFields)) {
        next if ($field eq 'uptime');
        $ret .= sprintf("%20.20s ", $field)
    }
    $ret .= sprintf("%20.20s", 'hit_ratio');

    return($ret);
}

sub  hitFields{
    my ($ap)= @_;
    ##  The @$ap array has a list of strings that look like this:
    ##      '        2501  Client connections accepted',
    ##      '          20  Client requests received',
    ##      '          18  Cache hits',
    ##      '           2  Cache misses',
    my %hitFields=();

    my @ary= grep {/$symbHash{'client_req'}/} (@$ap);
    ($hitFields{'client_req'}= (grep {/$symbHash{'client_req'}/} (@$ap))[0]) =~ s/^\s+(\S+).*$/$1/;

    @ary= grep {/$symbHash{'cache_hit'}/} (@$ap);
    ($hitFields{'cache_hit'}= (grep {/$symbHash{'cache_hit'}/} (@$ap))[0]) =~ s/^\s+(\S+).*$/$1/;

    return(\%hitFields);
}

sub  deltaSymb{
    my ($cp, $lp, $symb)= @_;
    die "deltaSymb() undefined symbol error" unless (exists($cp->{$symb}) && exists($lp->{$symb}));

    my $delta= $cp->{$symb} - $lp->{$symb};
    if ($delta < 0) {
        print "cp Hash==>>@{[Dumper($cp)]}\n";
        print "lp Hash==>>@{[Dumper($lp)]}\n";
        print "symb= $symb, delta= $delta\n";
        die "deltaSymb() delta calculation error";
    }
    return 1 if ($delta == 0);
    return $delta;
}

sub calcHitRatio {
    my ($s)= @_;
    my $retVal='';

    my $cur= hitFields($varnishServerStats{$s}->{'current'});
    my $lst= hitFields($varnishServerStats{$s}->{'last'});

    my $perCent= (deltaSymb($cur, $lst, 'cache_hit') / deltaSymb($cur, $lst, 'client_req')) * 100;
    $retVal= sprintf("%20.20s", sprintf(" %6.2f", $perCent));

    return($retVal);
}

sub serverStats {
    my ($s)= @_;
    my $ret='';
    my $deltaSeconds=1;

    ##  no telnet obj:  Couldn't connect to varnish server
    return "No Connection to server" unless defined($varnishServerStats{$s}->{'telnetObj'});
    ##  empty array:  timed out reading data.
    return "No Data this cycle" unless scalar(@{$varnishServerStats{$s}->{'current'}});

    my %curDescLines= map {descValue($_)} (@{$varnishServerStats{$s}->{'current'}});
    my %lastDescLines= map {descValue($_)} (@{$varnishServerStats{$s}->{'last'}});
    sub descValue {
        my($sLine)= @_;
        my $temp;
        my @ary= (($temp= $sLine) =~ /\s+(\S*)\s+(.*)$/);
        return () unless (scalar(@ary) == 2);
        return($ary[1], $sLine);
    }
    my @current= map {(exists($curDescLines{$_})) ? $curDescLines{$_} : '***'} @descAry;
    my @last= map {(exists($lastDescLines{$_})) ? $lastDescLines{$_} : '***'} @descAry;

    for (my $i=0; $i < scalar(@current); $i++) {
        my @cur= $current[$i] =~ /\s*(\S*)\s*(.*)$/;
        my @lst= $last[$i] =~ /\s*(\S*)\s*(.*)$/;
        next unless $cur[1] eq 'Client uptime';
        last unless $cur[0] > $lst[0];
        $deltaSeconds= $cur[0] - $lst[0];
    }

    for (my $i=0; $i < scalar(@current); $i++) {
        my $str='';
        if ($current[$i] eq '***' || $last[$i] eq '***') {
            $str= '***';
        } else {
            my @cur= $current[$i] =~ /\s*(\S*)\s*(.*)$/;
            my @lst= $last[$i] =~ /\s*(\S*)\s*(.*)$/;
            next if $cur[1] eq 'Client uptime';
            $str= sprintf("%7d %+7d", $cur[0], (($cur[0] - $lst[0]) / $deltaSeconds));
        }
        $ret .= sprintf("%20.20s ", $str);
    }
    $ret .= calcHitRatio($s);

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
        my $line= $telnet->getline(Errmode => "return", Timeout => $readTimeoutSecs);
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
    return(\@lines);
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

    ##  dump server stats on STDOUT
    system('clear');
    print "\n\n${\(fmtHeaderLine())}\n";
    for my $server (@varnishServers) {
        printf("%-15.15s %s\n", $server, serverStats($server));
    }

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
