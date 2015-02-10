#!/usr/bin/perl -w

use strict;
use warnings;
use Weather::Underground;
use LWP::UserAgent;
use DBI;

sub get_page
{
    my ($uri) = @_;

    # $uri is like http://www.wunderground.com/weather-forecast/01545
    my $ua = 'Mozilla/5.0 (compatible; MSIE 9.0; Windows NT 6.1; Trident/5.0)';
    my $timeout = '20';
    my $lwp = LWP::UserAgent->new(agent=>$ua, timeout=>$timeout);

    my $file = "/home/eii/weather/tmp";
    if ($uri =~ /\/([^\/]*?)$/) {
	$file = $1;
    }
    print $file, "\n";
    my $res = $lwp->get($uri, ":content_file" => $file);
}

sub get_data_via_uw_module
{
    my ($loc) = @_;
    my $weather = Weather::Underground->new (
	place => $loc,
	debug => 0,
	);

    my $ary_r = $weather->get_weather();
    if ($ary_r) {
	my @ary = @$ary_r;
	return %{$ary[0]};
    }
    else {
	my %hh;
	return %hh;
    }
}

sub db_write
{
    my ($table_name, $data_r) = @_;
    my @data = @$data_r;

    my $driver = "SQLite";
    my $db = "weather.db";
    my $dsn = "DBI:$driver:dbname=$db";
    my $userid = "";
    my $password = "";
    my $dbh = DBI->connect($dsn, $userid, $password, {RaiseError => 1});

    my $stmt = "select count(*) from sqlite_master where name = \"" . $table_name . "\";";
    my $sth = $dbh->prepare($stmt);
    $sth->execute();
    my $is_exist = 0;
    while (my @r = $sth->fetchrow_array) {
	if ($r[0] == 1) {
	    $is_exist = 1;
	    last;
	}
    }

    my $ret;
    if (!$is_exist) {
	my @row0 = @{$data[0]};

	$stmt = "create table " . $table_name . " (";
	for (my $i = 0; $i < @row0; $i++) {
	    $stmt = $stmt . $row0[$i] . " text, ";
	}
	$stmt = $stmt . " primary key (place, updated));";

	$ret = $dbh->do($stmt);
	if ($ret < 0) {
	    print $DBI::errstr;
	}
    }

    $stmt = "begin;";
    $sth = $dbh->prepare($stmt);
    $ret = $sth->execute();
    if ($ret < 0) {
	print DBI::errstr;
    }

    for (my $i = 1; $i < scalar(@data); $i++) {
	my @row = @{$data[$i]};

	$stmt = "insert or replace into " . $table_name . " values (" ;	
	for (my $j = 0; $j < scalar(@row); $j++) {
	    $stmt = $stmt . "\"" . $row[$j] . "\"";
	    if ($j == $#row) {
		$stmt = $stmt . ");";
	    }
	    else {
		$stmt = $stmt . ",";
	    }
	}
	$sth = $dbh->prepare($stmt);
	$ret = $sth->execute();
	if ($ret < 0) {
	    print $DBI::errstr;
	}
    }

    $stmt = "commit;";
    $sth = $dbh->prepare($stmt);
    $ret = $sth->execute();
    if ($ret < 0) {
	print DBI::errstr;
    }

    $dbh->disconnect();
}

# get_page("http://www.wunderground.com/weather-forecast/01545");
my %hh = get_data_via_uw_module("01545");

my @ary;
my @row0 = sort(keys(%hh));
if (scalar(@row0)) {
    push(@ary, \@row0);

    my @row;
    foreach my $k (sort(keys(%hh))) {
	push(@row, $hh{$k});
    }
    push(@ary, \@row);
    db_write("weather", \@ary);
}



