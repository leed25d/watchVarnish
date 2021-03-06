#!/usr/bin/perl
#
# This script is intended to be run by ops to monitor the status of
# the site's varnish servers.
#
use strict;
use warnings;
use Getopt::Long;
use Net::Telnet;
use Data::Dumper;
use FileHandle;
use threads;
use threads::shared;
use Thread::Queue;
$|=1;

my $connectTimeoutSecs= 120;
my $readTimeoutSecs=10;
my $slewTimeoutSecs=5;
my %summaryStats;

########################################################################
##  TODO: this counter named $varnishServerStats{$s}->{'stale'} is    ##
##  maintained as a count of the number of data collection cycles     ##
##  (ie of the main loop) which have passed since new data was        ##
##  actually acquired from $server.  This count is no longer used,    ##
##  but was left in place just so that it could be used if it ever    ##
##  became important again.  Consider taking it out.                  ##
########################################################################

my $usage = <<USAGE;
$0 - Monitor Varnish Stats.
This script is intended to be run by ops to monitor the status of
the site's varnish servers.

Usage:
    $0 [options]
    Options:
        --help|h          Print this help and exit
        --[no]clear       Clear the screen [or not] on each loop iteration
        --[no]ratio       Display a hit ratio [or not]
        --[no]summary     Display a summary block [or not]
        --field-set       Specify a starting field set
        --fields|f        Gather stats for the specified fields only
        --servers|s       Gather stats for the specified servers only
        --iterations|i    Iterations
        --pool-file|p     Pool file to use
        --list-fields|l   Print the list of legal field names on STDOUT

NOTES:

    --[no]clear   the screen is cleared by default [or not] on each loop
                  iteraion.  default --clear

    --[no]ratio   display the hit ratio [or not] default --ratio.

    --[no]summary display a summay block [or not] default --summary.

    --field-set   this is one of {empty, default, bans, queues}

    --fields      is a comma separated list of field names to display,
                  these are added to the fields specified by the
                  --field-set option.  This option may be specified
                  multiple times.

    --servers     is a comma separated list of server names to contact,
                  the option may be specified multiple times.

    --iterations  <forever|[nnnn[sS]] (string)> Default 300s. Specify
                  how long to run the program.  If this is a number,
                  run that number of iterations of the main loop. If it
                  is a number followed by [sS] run until that many
                  seconds have elapsed.  The (case-insesnitive) string
                  'forever' without the quotes will put the program
                  into an infinie loop.  In this case, terminate with
                  C-c.

    --pool-file  pool file containing server names one pr line.
                 Default pool file is \$LJHOME/etc/pool_varnish.txt
                 The pool file will not be read if --servers option
                 was used.

USAGE

########################################################################
##                          O P T I O N S                             ##
########################################################################
##  field sets can be specified by name by calling out the --fieldSet
##  option
my $fieldSets={};
$fieldSets->{'empty'}=   [qw //];
$fieldSets->{'default'}= [qw /client_conn client_req cache_hit cache_miss uptime/];
$fieldSets->{'bans'}=    [qw /n_ban n_ban_add n_ban_retire n_ban_obj_test
                              n_ban_re_test n_ban_dups client_req LCK.ban.locks/];
$fieldSets->{'queues'}=  [qw /n_wrk n_wrk_create n_wrk_failed n_wrk_max n_wrk_queue
                              n_wrk_overflow n_wrk_drop/];

my %clOptions;
GetOptions(
    'help|h'               => \($clOptions{'help'}= 0),
    'clear!'               => \($clOptions{'clear'}= 1),
    'ratio!'               => \($clOptions{'ratio'}= 1),
    'summary!'             => \($clOptions{'summary'}= 1),
    'field-set|=s'         => \($clOptions{'field_set'}= 'default'),
    'fields|f=s@'          => $clOptions{'fields'}= [],
    'servers|s=s@'         => $clOptions{'servers'}= [],
    'iterations|i=s'       => \($clOptions{'iterations'}= '300s'),
    'pool-file|p=s'        => \($clOptions{'pool_file'}= ''),
    'list-fields|l'        => \($clOptions{'list_fields'}= 0),
) or die $usage;
##print "Options Hash==>>@{[Dumper(\%clOptions)]}\n";
##exit;

##  -h --help option?  print a message and exit
die $usage if ($clOptions{'help'});

##  -s --servers option? get server list
my %varnishServers;
if (@{$clOptions{'servers'}}) {
    for my $serverString (@{$clOptions{'servers'}}) {
        for my $serverName (split(',', $serverString)) {
            $varnishServers{$serverName}= 1;
        }
    }
}

##  -p --pool-file option? get pool file.  (-s  has higher precedence -p)
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


##  get a list of valid field names and descriptions from some random
##  varnishserver.  First, find live server:
sub findALiveOne{
    my ($aryRef)= @_;
    my $telnet = new Net::Telnet();
    for my $s (@$aryRef) {
        eval '$telnet->open(Host => $s,Port => 7600, Timeout => $connectTimeoutSecs)';
        return $s unless $@;
    }
    return undef;
}
my $liveServer= findALiveOne([(keys(%varnishServers))]);
die "No living server could be found\n" unless $liveServer;

my $allFieldsCmd= "ssh $liveServer varnishstat -l 2>&1|egrep -v '^Field|^----|^Varnishstat'";
my $allfieldsFH= FileHandle->new("$allFieldsCmd|");
die "Can't open connection for fields list: $!" unless defined($allfieldsFH);

##  the @allFields array is an array of hashes like this:
##      my @allFields= ({'symbol' => 'client_conn',      'desc'  => 'Client connections accepted'},
##                      {'symbol' => 'client_drop',      'desc'  => 'Connection dropped, no sess/wrk'},
##                      {'symbol' => 'client_req',       'desc'  => 'Client requests received'})
my @allFields= map {
    my ($s, $d)= /^\s*(\S*)\s+(.*)/;
    {'symbol' => $s, 'desc' => $d}
} split(/\n/, do {local $/; my $txt= <$allfieldsFH>});
$allfieldsFH->close();

##  map field names to their index position in the list of status lines
my %validFieldNames= do {my $i=1; map {($_->{'symbol'}, $i++)} (@allFields) };

##  -l --list-fields option?  just print a list of fields and descriptions then exit
if ($clOptions{'list_fields'}) {
    for my $f (@allFields) {printf "%-30.30s %s\n", $f->{'symbol'}, $f->{'desc'}};
    exit;
}

##  -i --iterations option? limit program run time.
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

##  -f --fields option? list the field symbols which need
##  to be displayed
my %optFields=();
my $fsID= exists($fieldSets->{$clOptions{'field_set'}}) ? $clOptions{'field_set'} : 'default';
my @fieldSet= @{$fieldSets->{$fsID}};

for my $optionString (@fieldSet, @{$clOptions{'fields'}}) {
    for my $optionSymbol (split(',', $optionString)) {
        next unless exists($validFieldNames{$optionSymbol});
        $optFields{$optionSymbol}= $validFieldNames{$optionSymbol};
    }
}
die "fields array is empty.  nothing to do\n" unless scalar(keys(%optFields));

########################################################################
##                      E N D    O P T I O N S                        ##
########################################################################
##
##  construct an array of displayable field names.  The order of elements
##  in array is the order that they will appear on the output line.
my @nameAry= sort {$optFields{$a} <=> $optFields{$b}} (keys(%optFields));

$SIG{INT} = \&programOff;
sub programOff {
    $runTime{'stop'}= 1;
    $runTime{'reason'}= "Interrupted by signal";
}
my %varnishServerStats;

#########################################################################
##               T H R E A D    S U B R O U T I N E S                  ##
#########################################################################
my $q = Thread::Queue->new();
my @threadAry;

sub thr_initializeTelnet {
    my ($s)= @_;
    my $telnet = new Net::Telnet();
    eval '$telnet->open(Host => $s,Port => 7600, Timeout => $connectTimeoutSecs)';
    return undef if $@;
    return($telnet);
}

sub thr_getCurrentStats {
    my ($sap)= @_;
    $sap->{'lines'}= '';
    return unless my $telnet= $sap->{'tn'};

    my $errmsg= 'OK';
    my @lines=();

    $telnet->errmode("return");
    $errmsg= $telnet->errmsg() unless ($telnet->print('varn_stats_full'));
    if ($errmsg eq 'OK') {
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
    } else {
        ##  lost our telnet connection
        $sap->{'tn'}= undef;
    }
    $sap->{'getCurrentStatsMessage'}= $errmsg;
    $sap->{'lines'}= join("\n", @lines);
    return;
}

##  Varnish server thread
sub  thr_doVarnishServer {
    my ($sName)= @_;

    ##  server attributes pointer.
    my $sap= {};
    $sap->{'server'}= $sName;
    $sap->{'tn'}= undef;

    while (1) {
        ##  initialize a telnet object maybe
        if (!defined($sap->{'tn'})) {
            do {$sap->{'tn'}= thr_initializeTelnet($sName); sleep 2 unless $sap->{'tn'}} until $sap->{'tn'};
        }

        thr_getCurrentStats($sap);
        next unless (defined($sap->{'tn'})); ##  in case the telnet connection died

        my $hp= {}; share($hp);
        $hp->{'server'}= $sap->{'server'};

        for my $a ('getCurrentStatsMessage', 'lines') {$hp->{$a}= $sap->{$a}};
        $q->enqueue($hp);

        sleep 1;
    }
}

#########################################################################
##        M A I N    P R O G R A M    S U B R O U T I N E S            ##
#########################################################################
##  Loop Control.  There are a nmber of ways to terminate the program.
##        --  stop after a given time period
##        --  stop after a given number of iterations
##        --  stop on receipt of SIGINT
##  See the Options code or look at usage or --help
my %checkIterate= ('iterations' => \&countIterations, 'seconds' => \&countSeconds);
sub loopControl {
    my ($rh)= @_;

    return 0 if $rh->{'stop'};
    return 1 unless (exists($rh->{'limit'}));
    return 1 unless (exists($checkIterate{$rh->{'limit'}}));
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

##  server stats are queued by threads and there is one thread for
##  each varnish server.  This routine pulls all available entries off
##  of the queue and updates the %varnishServerStats hash
##  appropriately.
sub getServerStats {
    my ($time)= @_;

    for (@varnishServers) {$varnishServerStats{$_}->{'stale'}++}
    return unless (my $count= $q->pending());

    ##  get all the work out of the queue, then process each element
    my @ary= map {$q->dequeue_nb()} (1..$count);
    for my $element (@ary) {
        my $server= $element->{'server'};
        my $p= $varnishServerStats{$server};  ##  for convenience.  a pointer.

        ##  $newAryp is a pointer to a new array of data from $server
        my $newAryp= [ split("\n", $element->{'lines'}) ];
        next unless scalar(@$newAryp);

        ##  the %varnishServerStats has two arrays of statistics for
        ##  each server: the most recently acquired (current) array
        ##  and the last one
        if (arraysDifferent($p->{'current'}, $newAryp)) {
            if (scalar(@{$p->{'current'}}) && arraysDifferent($p->{'current'}, $p->{'last'})){
                $p->{'last'}= $p->{'current'};
            }
            $p->{'current'}= $newAryp;
            $p->{'stale'}= 0;
        }
    }
    for my $server (@varnishServers) {
        my $p= $varnishServerStats{$server};  ##  for convenience.  a pointer.
        $p->{'last'}= $p->{'current'} unless  scalar(@{$p->{'last'}});
    }
}

##  two arrays are the same if
##    --they have the same cardinality AND
##    --their values are pairwise equivalent
##  there is an assumption here, and it is a valid one for this
##  dataset, that an array will never contain multiple elements with
##  the same value.
sub arraysDifferent {
    my ($aryRef1, $aryRef2)= @_;
    return 1 if (scalar(@$aryRef1) != scalar(@$aryRef2));

    my %count;
    foreach my $e (@$aryRef1, @$aryRef2) {$count{$e}++};
    foreach my $e (keys(%count)) {return 1 if ($count{$e} != 2)};
    return 0;
}

my $headerLine= '';
sub fmtHeaderLine {
    ##  only build it the first time.
    return $headerLine if $headerLine;

    $headerLine .= sprintf("%-12.12s", 'server');
    for my $field (sort {$optFields{$a} <=> $optFields{$b}} keys(%optFields)) {
        $headerLine .= sprintf("%20.20s ", $field)
    }
    $headerLine .= sprintf("%20.20s", 'hit_ratio') if $clOptions{'ratio'};

    return($headerLine);
}

sub  hitFields{
    my ($ap)= @_;
    ##  The @$ap array has a list of strings that look like this:
    ##    'cache_hit              5736681        72.97 Cache hits',
    ##    'cache_hitpass             6045         0.08 Cache hits for pass',
    ##    'cache_miss             3034994        38.61 Cache misses',
    ##    'backend_conn           3254235        41.39 Backend conn. success',
    ##
    ##  return values for the 'cache_miss' and 'cache_hit' lines in a hash

    my %hitFields= map {getSymbolic($_, $ap)} ('cache_miss', 'cache_hit');
    sub getSymbolic {
        my @ary= grep {/^$_[0]/} (@{$_[1]});
        return ($_, 1) unless scalar(@ary);
        $ary[0] =~ s/^\S+\s+(\d+).*$/$1/;
        return ($_, $ary[0]);
    }
    return(\%hitFields);
}

sub updateSummaryRatio {
    my ($which, $p)= @_;
    for (qw/cache_hit cache_miss/) {$summaryStats{$which}->{$_} += $p->{$_}};
}

sub calcHitRatio {
    my ($s)= @_;
    my $retVal='';
    my $p= $varnishServerStats{$s}; ##  for convenience.  a pointer.
    my $cur= hitFields($p->{'current'});
    my $calc= $cur;
    updateSummaryRatio('curRatio', $cur);

    if (scalar(@{$p->{'last'}})) {
        my $lst= hitFields($p->{'last'});
        my $delta= { map {($_, ($cur->{$_} - $lst->{$_}))} ('cache_hit', 'cache_miss') };
        if ($delta->{'cache_hit'} > 1 && $delta->{'cache_miss'} > 1) {
            $calc= $delta;
        }
    }

    my $calcPerCent= ($calc->{'cache_hit'} / (($calc->{'cache_hit'} + $calc->{'cache_miss'}) || 1.0)) * 100;
    my $calcDisp= sprintf("%6.2f", $calcPerCent);
    updateSummaryRatio('deltaRatio', $calc) unless ($cur == $calc);
    $calcDisp= '***   ' if ($cur == $calc);

    my $curPerCent= ($cur->{'cache_hit'} / (($cur->{'cache_hit'} + $cur->{'cache_miss'}) || 1.0)) * 100;
    my $curDisp= sprintf("%6.2f", $curPerCent);

    $retVal= sprintf("%20.20s", sprintf('%2.2s%s%2.2s%s', ' ', $curDisp, ' ', $calcDisp));
    return($retVal);
}

sub serverStats {
    my ($s)= @_;
    my $deltaSeconds=1;
    my $ret= '';

    my $p= $varnishServerStats{$s};  ##  for convenience.  a pointer.
    return "No Data from Server" unless scalar(@{$p->{'current'}});


    ##  these next two hashes  %curNameLines and  %lstNameLines are of the form
    ##     'name'  => 'line'
    ##
    ##  where 'line' is a string like one of these:
    ##    'cache_hit              5736681        72.97 Cache hits',
    ##    'cache_hitpass             6045         0.08 Cache hits for pass',
    ##    'cache_miss             3034994        38.61 Cache misses',
    ##    'backend_conn           3254235        41.39 Backend conn. success',
    ##
    ##  and 'name' is the first attribute of 'line', thus:
    ##  {
    ##     'cache_hit'     => 'cache_hit              5736681        72.97 Cache hits',
    ##     'cache_hitpass' => 'cache_hitpass             6045         0.08 Cache hits for pass',
    ##     'cache_miss'    => 'cache_miss             3034994        38.61 Cache misses',
    ##     'backend_conn'  => 'backend_conn           3254235        41.39 Backend conn. success',
    ##
    ##         . . .  and so forth . . .
    ##  }
    my %curNameLines= map {(/(\S+)/, $_)} (@{$p->{'current'}});
    my %lstNameLines= map {(/(\S+)/, $_)} (@{$p->{'last'}});

    ##  determine the numer of seconds which have elapsed between the
    ##  most recent two measurements
    my $curUptime= (split('\s+', $curNameLines{'uptime'}))[1] || 1;
    my $lstUptime= (split('\s+', $lstNameLines{'uptime'}))[1] || 1;
    $deltaSeconds= ($curUptime>$lstUptime) ? $curUptime-$lstUptime : 1;

    my $fakeEntry='XXX XXX';
    my @current= map {(exists($curNameLines{$_})) ? $curNameLines{$_} : $fakeEntry} @nameAry;
    my @last= map {(exists($lstNameLines{$_})) ? $lstNameLines{$_} : $fakeEntry} @nameAry;

    for (my $i=0; $i < scalar(@current); $i++) {
        my $str='';
        my $name= $nameAry[$i];

        if ($current[$i] eq $fakeEntry || $last[$i] eq $fakeEntry) {
            $str= '***';

        } else {
            my @cur= $current[$i] =~ /^(\S+)\s*(\S+).*$/;
            my @lst= $last[$i] =~ /^(\S+)\s*(\S+).*$/;
            my $t= (($cur[1] - $lst[1]) / $deltaSeconds);

            $summaryStats{$name}->{'cur'} += $cur[1];
            $summaryStats{$name}->{'delta'}  += sprintf('%d', $t);
            $str= sprintf("%10d %+6d", $cur[1], $t);
        }
        $ret .= sprintf("%20.20s ", $str);
    }

    $ret .= calcHitRatio($s) if $clOptions{'ratio'};
    return $ret;
}

sub initsummaryStats {
    $summaryStats{$_}= {'cur', 0, 'delta', 0} for (@nameAry);
    $summaryStats{'curRatio'}= {'cache_hit', 0, 'cache_miss', 0};
    $summaryStats{'deltaRatio'}= {'cache_hit', 0, 'cache_miss', 0};
}

sub caclAggregateHitRatio {
    my ($p)= (@_);
    my $ret= '  ***  ';
    my $denom= $p->{'cache_hit'} + $p->{'cache_miss'};
    if ($denom > 0) {
        $ret = sprintf("%6.2f", (($p->{'cache_hit'} / ($denom || 1.0)) * 100));
    }
    return $ret;
}

sub serverStatsSummary {
    my $ret= sprintf('%-17.17s',"Summary Stats") . "\n";

    ##  hit ratio
    if ($clOptions{'ratio'}) {
        $ret .= sprintf("%20.20s:    %8.8s      %8.8s\n",
                        'hit_ratio',
                        caclAggregateHitRatio($summaryStats{'curRatio'}),
                        caclAggregateHitRatio($summaryStats{'deltaRatio'}));
    }

    ##  status fields
    for (@nameAry) {$ret .= appendSummaryValueField($_)};
    sub appendSummaryValueField {
        return(sprintf("%20.20s: %15d %+8d\n",
                       $_[0],
                       $summaryStats{$_[0]}->{'cur'},
                       $summaryStats{$_[0]}->{'delta'}));
    }
    return "\n$ret\n";
}

#########################################################################
##                             M A I N                                 ##
#########################################################################

##  start a thread for each varnish server.  This thread will telnet
##  to port 7600, acquire stats data and queue that data for the main
##  thread.  The main program will read from that queue in the
##  getServerStats() subroutine.
for (@varnishServers) {kickOff($_)};
sub kickOff {
    my ($s)= @_;

    $varnishServerStats{$s}->{'current'}= [];
    $varnishServerStats{$s}->{'last'}= [];
    $varnishServerStats{$s}->{'stale'}= 0;
    $varnishServerStats{$s}->{'name'}= $s;

    my $thr= threads->create(sub {thr_doVarnishServer($_[0])}, ($s));
    push(@threadAry, $thr) if $thr;
}

##  This is the main loop.  Get queued data from each server, format a
##  stats line for each server and, optionally, display a summary
##  block. keep it up until the loop control (set by the --iterations
##  command line option) has expired.
while (loopControl(\%runTime)) {
    my $loopTime= time;

    getServerStats($loopTime);

    ##  dump server stats on STDOUT
    initsummaryStats() if $clOptions{'summary'};
    my $outStr= "\n\n${\(fmtHeaderLine())}\n";

    for my $server (@varnishServers) {
        $outStr .= sprintf("%-12.12s%s\n", $server, serverStats($server));
    }

    $outStr .= serverStatsSummary() if $clOptions{'summary'};
    system('clear') if $clOptions{'clear'};
    print $outStr;
    sleep 1;
}
print "Quitting.  ${\($runTime{'reason'})}\n";
##for my $t (@threadAry) {$t->join};

exit;
