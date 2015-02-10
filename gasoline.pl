#!/usr/bin/perl -w

use strict;
use warnings;
use Web::Scraper;
use URI;
use DBI;


sub get_todays_gasoline_price
{
    my $uri = "http://fuelgaugereport.aaa.com/import/display.php?lt=state&ls=";
    my $scraper = scraper {
	process '//select[@id="select_metro"]/option', 'option[]' => '@value';
	process '//select[@id="select_metro"]/option', 'city[]' => 'TEXT';
    };

    my $res;
    eval {
	$res = $scraper->scrape(URI->new($uri));
    };
    if($@) {
	print "failed\n";
    }

    my %state_list;
    foreach my $p (@{$res->{city}}) {
	if ($p =~ / - (\w\w)/) {
	    my $state_name = $1;
	    $state_list{$state_name}++;
	}
    }

    my $cur_time = localtime();
    my @ary;
    my @row0 = qw/city state date time data_type regular mid premium diesel cur_time/;
    push(@ary, \@row0);
    foreach my $state (keys(%state_list)) {
	my $uri_state = "http://fuelgaugereport.aaa.com/import/display.php?lt=metro&ls=" . $state;
	my $scraper_state = scraper {
	    process '//a[@name]', 'name[]' => '@name';
	    process '//table[@class="metro"]/tbody/tr/td', 'td[]' => 'TEXT';
	    process '//em[2]', 'em' => 'TEXT';
	};
	my $res_state;
	eval {
	    $res_state = $scraper_state->scrape(URI->new($uri_state));
	};
	if ($@) {
	    print "$uri_state: failed\n";
	}
	my @td = @{$res_state->{td}};
	my @name = @{$res_state->{name}};
	my $em = $res_state->{em};
	my $d = "";
	my $t = "";
	if ($em =~ /(\d+)\/(\d+)\/(\d\d\d\d) (\d+):(\d+)(\w\w)/) {
	    my $year = $3;
	    my $mon = $1;  $mon = sprintf("%02d", $mon);
	    my $day = $2;  $day = sprintf("%02d", $day);
	    my $hour = $4;
	    my $min = $5;  $min = sprintf("%02d", $min);
	    my $ampm = $6;
	    if ($ampm eq "pm") {
		$hour += 12;
	    }
	    $hour = sprintf("%02d", $hour);
	    $d = "$year-$mon-$day";
	    $t = "$hour:$min";
	}

	my $n_col = 5;
	my $cnt = 0;
	for (my $i = 0; $i < @td / $n_col; $i++) {
	    my @row;
	    push(@row, $name[$cnt]);
	    push(@row, $state);
	    push(@row, $d);
	    push(@row, $t);
	    for (my $j = 0; $j < $n_col; $j++) {
		push(@row, $td[$i*$n_col + $j]);
	    }
	    push(@row, $cur_time);

	    if ($td[$i*$n_col] =~ /Year Ago/) {
		$cnt++;
	    }
	    push(@ary, \@row);
	}
    }
    return @ary;
}

sub db_write
{
    my ($table_name, $data_r) = @_;
    my @data = @$data_r;

    my $driver = "SQLite";
    my $db = "gasoline.db";
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
	$stmt = $stmt . " primary key (";
	my $primary_cols = 5;
	for (my $i = 0; $i < $primary_cols; $i++) {
	    if ($i == $primary_cols - 1) {
		$stmt = $stmt . $row0[$i];
	    }
	    else {
		$stmt = $stmt . $row0[$i] . ",";
	    }
	}
	$stmt = $stmt . "));";

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

my @ary_today = get_todays_gasoline_price;
db_write("gasoline_price", \@ary_today);

