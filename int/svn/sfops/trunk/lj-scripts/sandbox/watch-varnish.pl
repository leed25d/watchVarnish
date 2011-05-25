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
        --field-set       Specify a starting field set
        --fields|f        Gather stats for the specified fields only
        --servers|s       Gather stats for the specified servers only
        --iterations|i    Iterations
        --pool-file|p     Pool file to use
        --List-Fields|l   Print the list of legal field names on STDOUT

NOTES:

    --[no]clear  clear the screen is cleared by default [or not] on
                 each loop iteraion.  default --clear

    --[no]ratio  display the hit ratio [or not] default --ratio.

    --field-set  this is one of {empty, default, purges}

    --fields     is a comma separated list of field names to display,
                 these are added to the fields specified by the
                 --field-set option

    --servers    is a comma separated list of server names to contact,
                 the option may be specified multiple times.

    --iterations <forever|[nnnn[sS]] (string)> Default 300s. Specify
                 how long to run the program.  If this is a number,
                 run that number of iterations of the main loop. If it
                 is a number followed by [sS] run until that many
                 seconds have elapsed.  The (case-insesnitive) string
                 'forever' without the quotes will put the program
                 into an infinie loop.  In this case, terminate with
                 C-c.

    --pool-file pool file containing server anmes one pr line.
                 Default pool file is \$LJHOME/etc/pool_varnish.txt
                 The pool file will not be read if --servers option
                 was used.

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
##  field sets can be specified by name by calling out the --fieldSet
##  option
my $fieldSets={};
$fieldSets->{'empty'}=   [qw //];
$fieldSets->{'default'}= [qw /client_conn client_req cache_hit cache_miss/];
$fieldSets->{'purges'}=  [qw /n_purge n_purge_add n_purge_retire n_purge_obj_test
                              n_purge_re_test n_purge_dups client_req/];
my %clOptions;
GetOptions(
    'help|h'               => \($clOptions{'help'}= 0),
    'clear!'               => \($clOptions{'clear'}= 1),
    'ratio!'               => \($clOptions{'ratio'}= 1),
    'field-set|=s'         => \($clOptions{'field_set'}= 'default'),
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

##  -f option? --field-set option? list the field symbols which need
##  to be displayed
my %optFields=();
my $fsID= exists($fieldSets->{$clOptions{'field_set'}}) ? $clOptions{'field_set'} : 'default';
my @fieldSet= @{$fieldSets->{$fsID}};

for my $optionString (@fieldSet, @{$clOptions{'fields'}}) {
    for my $optionSymbol (split(',', $optionString)) {
        next unless exists($symbHash{$optionSymbol});
        $optFields{$optionSymbol}= 1;
    }
}
die "fields array is empty.  nothing to do\n" unless scalar(keys(%optFields));

#  establish an ordering relation.  this is the order that fields
##  will appear on the output line
for my $symb (keys(%optFields)) {$optFields{$symb}= optFieldOrder($symb)};
sub optFieldOrder {
    my ($s)= @_;
    for (my $i=0; $i<@allFields; $i++) {
        return $i if ($allFields[$i]->{'symbol'} eq $s);
    }
}

##  -s option? get server list
my %varnishServers;
if (@{$clOptions{'servers'}}) {
    for my $serverString (@{$clOptions{'servers'}}) {
        for my $serverName (split(',', $serverString)) {
            $varnishServers{$serverName}= 1;
        }
    }
}

##  -p option? get pool file.  (-s  binds more tightly than -p)
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

########################################################################
##                      E N D    O P T I O N S                        ##
########################################################################
##
##  construct an array of displayable fields
my @descAry= ();
for my $symbol (keys(%optFields)) {
    next unless exists($symbHash{$symbol});
    push(@descAry, $symbHash{$symbol});
}

##  the descriptions array needs to be sorted into display order
@descAry= sort {of($a) <=> of($b)} (@descAry);
sub of {
    my ($p)= @_;
    return($optFields{$descHash{$p}});
}

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

sub initializeTelnet {
    my ($s)= @_;
    my $telnet = new Net::Telnet();
    eval '$telnet->open(Host => $s,Port => 6082, Timeout => $connectTimeoutSecs)';
    return undef if $@;

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

sub getCurrentStats {
    my ($sap)= @_;
    $sap->{'lines'}= '';
    return unless my $telnet= $sap->{'tn'};

    my $errmsg= 'OK';
    my @lines=();

    $telnet->errmode("return");
    $errmsg= $telnet->errmsg() unless ($telnet->print('stats'));
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
    $sap->{'getCurrentStatsRetcode'}= shift @lines if @lines;
    $sap->{'lines'}= join("\n", @lines);
    return;
}

##  Varnish server thread
sub  doVarnishServer {
    my ($sName)= @_;

    ##  server attributes pointer.
    my $sap= {};
    $sap->{'server'}= $sName;
    $sap->{'tn'}= undef;

    while (loopControl(\%runTime)) {
        ##  initialize a telnet object maybe
        if (!defined($sap->{'tn'})) {
            do {$sap->{'tn'}= initializeTelnet($sName); sleep 2 unless $sap->{'tn'}} until $sap->{'tn'};
        }

        getCurrentStats($sap);
        next unless (defined($sap->{'tn'})); ##  in case the telnet connection died

        my $hp= {}; share($hp);
        $hp->{'server'}= $sap->{'server'};

        for my $a ('getCurrentStatsMessage', 'getCurrentStatsRetcode', 'lines') {$hp->{$a}= $sap->{$a}};
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
##  See the Options code or look at sage or --help
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
    for my $server (@varnishServers) {
        $varnishServerStats{$server}->{'stale'}++
    }
    return unless (my $count= $q->pending());

    my @ary= map {$q->dequeue_nb()} (1..$count);

    for my $hp (@ary) {
        my $server= $hp->{'server'};
        my $p= $varnishServerStats{$server};  ##  for convenience.  a pointer.

        my $ap= [ split("\n", $hp->{'lines'}) ];
        next unless scalar(@$ap);

        ##  the %varnishServerStats has two arrays of statistics for
        ##  each server: the most recently acquired array and the
        ##  previous one
        if (arraysDifferent($p->{'current'}, $ap)) {
            if (scalar(@{$p->{'current'}}) && arraysDifferent($p->{'current'}, $p->{'last'})){
                $p->{'last'}= $p->{'current'};
            }
            $p->{'current'}= $ap;
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
sub arraysDifferent {
    my ($p1, $p2)= @_;
    return 1 if (scalar(@$p1) != scalar(@$p2));
    my %count;

    foreach my $e (@$p1, @$p2) {$count{$e}++};
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
    ##      '        2501  Client connections accepted',
    ##      '          20  Client requests received',
    ##      '          18  Cache hits',
    ##      '           2  Cache misses',
    my %hitFields= map {getSymbolic($_, $ap)} ('cache_miss', 'cache_hit');
    sub getSymbolic {
        my @ary= grep {/$symbHash{$_[0]}/} (@{$_[1]});
        return ($_, 1) unless scalar(@ary);
        $ary[0] =~ s/^\s+(\S+).*$/$1/;
        return ($_, $ary[0]);
    }
    return(\%hitFields);
}

sub calcHitRatio {
    my ($s)= @_;
    my $retVal='';
    my $p= $varnishServerStats{$s}; ##  for convenience.  a pointer.
    my $cur= hitFields($p->{'current'});
    my $calc= $cur;
    my $ind= '*';

    if (scalar(@{$p->{'last'}})) {
        my $lst= hitFields($p->{'last'});
        my $delta= { map {($_, ($cur->{$_} - $lst->{$_}))} ('cache_hit', 'cache_miss') };
        if ($delta->{'cache_hit'} > 1 && $delta->{'cache_miss'} > 1) {
            $calc= $delta;
            $ind= ' ';

        }
    }

    my $perCent= ($calc->{'cache_hit'} / ($calc->{'cache_hit'} + $calc->{'cache_miss'})) * 100;
    $retVal= sprintf("%20.20s", sprintf("%6.2f %s", $perCent, $ind || ' '));
    $summaryStats{'ratio'}->{'cache_hit'} += $calc->{'cache_hit'};
    $summaryStats{'ratio'}->{'cache_miss'} += $calc->{'cache_miss'};
    return($retVal);
}

sub serverStats {
    my ($s)= @_;
    my $deltaSeconds=1;
    my $ret= '';

    my $p= $varnishServerStats{$s};  ##  for convenience.  a pointer.
    return "No Data from Server" unless scalar(@{$p->{'current'}});

    my %curDescLines= map {descValue($_)} (@{$p->{'current'}});
    my %lastDescLines= map {descValue($_)} (@{$p->{'last'}});
    sub descValue {
        my($sLine)= @_;
        my $temp;
        my @ary= (($temp= $sLine) =~ /\s+(\S*)\s+(.*)$/);
        return () unless (scalar(@ary) == 2);
        return($ary[1], $sLine);
    }

    ##  determine the numer of seconds which have elapsed between the
    ##  most recent two measurements
    my @curUptime= map {/\s*(\S*)\s*(.*)$/} grep {/client uptime/i} (@{$p->{'current'}});
    my @lstUptime= map {/\s*(\S*)\s*(.*)$/} grep {/client uptime/i} (@{$p->{'last'}});
    $deltaSeconds= ($curUptime[0]>$lstUptime[0]) ? $curUptime[0]-$lstUptime[0]: 1;

    my $fakeEntry='XXX XXX';
    my @current= map {(exists($curDescLines{$_})) ? $curDescLines{$_} : $fakeEntry} @descAry;
    my @last= map {(exists($lastDescLines{$_})) ? $lastDescLines{$_} : $fakeEntry} @descAry;

    for (my $i=0; $i < scalar(@current); $i++) {
        my $str='';
        my $descr= $descAry[$i];

        if ($current[$i] eq $fakeEntry|| $last[$i] eq $fakeEntry) {
            $str= '***';

        } else {
            my @cur= $current[$i] =~ /\s*(\S*)\s*(.*)$/;
            my @lst= $last[$i] =~ /\s*(\S*)\s*(.*)$/;
            my $t= (($cur[0] - $lst[0]) / $deltaSeconds);

            $summaryStats{$descr}->{'cur'} += $cur[0];
            $summaryStats{$descr}->{'delta'}  += sprintf('%d', $t);
            $str= sprintf("%10d %+6d", $cur[0], $t);
        }
        $ret .= sprintf("%20.20s ", $str);
    }

    $ret .= calcHitRatio($s) if $clOptions{'ratio'};
    return $ret;
}

sub initSummaryStats {
    for (@descAry) {initSummaryStatsAttr($_)};
    sub initSummaryStatsAttr {
        $summaryStats{$_[0]}= {'cur', 0, 'delta', 0};
    }
    $summaryStats{'ratio'}= {'cache_hit', 0, 'cache_miss', 0};
}

sub serverStatsSummary {
    my $ret= sprintf('%-17.17s',"Summary Stats") . "\n";

    ##  aggregate hit ratio
    my $p= $summaryStats{'ratio'};  ##  for convenience.   a pointer.
    my $denom= $p->{'cache_hit'} + $p->{'cache_miss'};
    if ($denom > 0) {
        my $perCent= ($p->{'cache_hit'} / ($p->{'cache_hit'} + $p->{'cache_miss'})) * 100;
        $ret .= sprintf("%20.20s: %15.2f\n", 'hit_ratio', $perCent);
    }

    ##  fields
    for (@descAry) {$ret .= appendSummaryValueField($_)};
    sub appendSummaryValueField {
        return(sprintf("%20.20s: %15d %+8d\n",
                       $descHash{$_[0]},
                       $summaryStats{$_[0]}->{'cur'},
                       $summaryStats{$_[0]}->{'delta'}));
    }
    return "\n$ret\n";
}
#########################################################################
##                             M A I N                                 ##
#########################################################################
##  (main) create threads
for my $server (@varnishServers) {kickOff($server)};
sub kickOff {
    my ($s)= @_;

    $varnishServerStats{$s}->{'current'}= [];
    $varnishServerStats{$s}->{'last'}= [];
    $varnishServerStats{$s}->{'stale'}= 0;
    $varnishServerStats{$s}->{'name'}= $s;

    my $thr= threads->create(sub {doVarnishServer($_[0])}, ($s));
    push(@threadAry, $thr) if $thr;
}

while (loopControl(\%runTime)) {
    my $loopTime= time;

    getServerStats($loopTime);

    ##  dump server stats on STDOUT
    my $outStr= "\n\n${\(fmtHeaderLine())}\n";
    initSummaryStats();

    for my $server (@varnishServers) {
        my $p= $varnishServerStats{$server}; ##  for convenience.  a pointer.
        $outStr .= sprintf("%-12.12s%s\n",
                           $server,
                           serverStats($server));
    }

    $outStr .= serverStatsSummary();
    system('clear') if $clOptions{'clear'};
    print $outStr;
    sleep 1;
}
print "Quitting.  ${\($runTime{'reason'})}\n";
##for my $t (@threadAry) {$t->join};

exit;
