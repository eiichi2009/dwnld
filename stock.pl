#!/usr/bin/perl -w

use strict;
use warnings;
use Encode;
use Spreadsheet::ParseExcel;
use Web::Scraper;
use URI;
use LWP::UserAgent;
use File::Temp qw/tempfile/; 
use POSIX;
use DBI;
use Date::Holidays::KR;
use Date::Simple;
use DateTime;
use DateTime::Format::Strptime;
use Selenium::Remote::Driver;
use Selenium::Remote::Driver::Firefox::Profile;

$|=1;

my $work_dir = "/home/eii/korean_stock/";
my $company_list_table = "kr_company_list";
my $stock_table = "kr_stock";
my $database_name = "stock.db";
my $quand_token = "";

sub date_diff
{
    my ($from, $to) = @_;
    my $strp = DateTime::Format::Strptime->new(pattern=>"%Y-%m-%d");

    my $dt_from = $strp->parse_datetime($from);
    my $dt_to   = $strp->parse_datetime($to);

    my $diff = $dt_to->delta_days($dt_from);
    my $days = $diff->in_units("days") + 1;

    return $days;
}

sub get_kr_yahoo
{
    my ($comp_code, $market, $from, $to) = @_;

    my $market_type = "KS";
    if ($market eq "KOSDAQ") {
	$market_type = "KQ";
    }
    my $year_from = substr($from, 0, 4);
    my $mon_from = qw/XX 00 01 02 03 04 05 06 07 08 09 10 11/[scalar(substr($from, 5, 2))];
    my $day_from = substr($from, 8, 2) * 1;
    my $year_to = substr($to, 0, 4);
    my $mon_to = qw/XX 00 01 02 03 04 05 06 07 08 09 10 11/[scalar(substr($to, 5, 2))];
    my $day_to = substr($to, 8, 2) * 1;

    my $uri = "http://real-chart.finance.yahoo.com/table.csv?s=" . $comp_code . "." . $market_type . "&a="
	. $mon_from . "&b=" . $day_from . "&c=" . $year_from . "&d="
	. $mon_to   . "&e=" . $day_to   . "&f=" . $year_to   . "&g=d&ignore=.csv";
    my $ua = 'Mozilla/5.0 (compatible; MSIE 9.0; Windows NT 6.1; Trident/5.0)';
    my $timeout = '20';

    my ($fh, $file) = File::Temp::tempfile(DIR => '/tmp', UNLINK => 1);
    my $lwp = LWP::UserAgent->new(agent=>$ua, timeout=>$timeout);
    my $res = $lwp->get($uri, ':content_file'=>$file);

    my @ary;
    if ($res->is_success) {
	while (<$fh>) {
	    my $line = $_;
	    chomp($line);
	    my @row_tmp = split(/,/, $line);
	    my @row;
	    push(@row, $comp_code); # code
	    push(@row, @row_tmp); # date open high low close volume adjclose
	    push(@row, "yahoo"); # website
	    if ($row[1] =~ /\d\d\d\d-\d\d-\d\d/) {
		push(@ary, \@row);
	    }
	}
    }
    close($fh);
    unlink($file);

    return @ary;
}

sub get_kr_google
{
    my ($comp_code, $market, $from, $to) = @_;

    # Format
    # Date       | Open    | High    | Low     | Close   | Volume
    # Jan 6, 2003| 1,770.00| 1,960.00| 1,680.00| 1,900.00| 809,189

    my $days = date_diff($from, $to);

    my $scraper = scraper {
	process '//table[@class="gf-table historical_price"]//td', 'td[]' => 'TEXT';
    };

    my $year_f = substr($from, 0, 4);
    my $mon_f = qw/XXX Jan Feb Mar Apr May Jun Jul Aug Sep Oct Nov Dec/[scalar(substr($from, 5, 2))];
    my $day_f = substr($from, 8, 2) * 1;

    my $year_t = substr($to, 0, 4);
    my $mon_t = qw/XXX Jan Feb Mar Apr May Jun Jul Aug Sep Oct Nov Dec/[scalar(substr($to, 5, 2))];
    my $day_t = substr($to, 8, 2) * 1;

    my $startdate = $mon_f . "+" . $day_f . "%2C+" . $year_f;
    my $enddate = $mon_t . "+" . $day_t . "%2C+" . $year_t;

    my $d_mkt;
    if ($market eq "KRX" || $market eq "KOSDAQ") {
	# no change
	$d_mkt = $market;
    }
    else {
	$d_mkt = "KRX";
    }

    my $start = 0;
    my @ary;
    while ($start <= $days) {
	my $uri = "http://www.google.com/finance/historical?q=" . $d_mkt . "%3A" . $comp_code
	    . "&startdate=" . $startdate . "&enddate=" . $enddate . "&num=200&start=" . $start;
	my $res = $scraper->scrape(URI->new($uri));
	if (exists($res->{td})) {
	    push(@ary, @{$res->{td}});
	}
	else {
	    if ($start == 0 && $market eq "unknown" && $d_mkt eq "KRX") {
		$d_mkt = "KOSDAQ";
		next;
	    }
	    last;
	}
	$start += 200;
    }

    if (scalar(@ary) == 0) {
	return @ary;
    }

    my @ary_col;
    my $n_col = 6; # date / open / high / low / close / volume
    my %mon2num = ("Jan"=>"01", "Feb"=>"02", "Mar"=>"03", "Apr"=>"04", "May"=>"05", "Jun"=>"06",
		   "Jul"=>"07", "Aug"=>"08", "Sep"=>"09", "Oct"=>"10", "Nov"=>"11", "Dec"=>"12");
    for (my $i = 0; $i < scalar(@ary) / $n_col; $i++) {
	my @row;
	if ($ary[$i*$n_col] =~ /(\w\w\w)\s+(\d+),\s+(\d\d\d\d)/) {
	    my $col_year = $3;
	    my $col_mon = $mon2num{$1};
	    my $col_day = sprintf("%02d", $2);
	    $ary[$i*$n_col] = $col_year . "-" . $col_mon . "-" . $col_day;
	}
	push(@row, $comp_code);
	for (my $j = 0; $j < $n_col; $j++) {
	    my $col = $ary[$i*$n_col + $j];
	    $col =~ s/[ ,]//g;
	    push(@row, $col);
	}
	push(@row, ""); # Adj Close
	push(@row, "google");
	if ($row[1] =~ /\d\d\d\d-\d\d-\d\d/) {
	    push(@ary_col, \@row);
	}
    }
    return @ary_col;
}

sub get_kr_quandl
{
    my ($comp_code, $market, $from, $to) = @_;
    my $market_type = "KS";
    if ($market eq "KOSDAQ") {
        $market_type = "KQ";
    }
    my $uri = "http://www.quandl.com/api/v1/datasets/YAHOO/" . $market_type . "_" . $comp_code . ".csv" . "?auth_token=" . $quand_token;
    my ($fh, $file) = File::Temp::tempfile(DIR => '/tmp', UNLINK => 1);

    my $ua = 'Mozilla/5.0 (compatible; MSIE 9.0; Windows NT 6.1; Trident/5.0)';
    my $timeout = '20';
    my $lwp = LWP::UserAgent->new(agent=>$ua, timeout=>$timeout);
    my $res = $lwp->get($uri, ':content_file'=>$file);

    my @ary;
    if ($res->is_success) {
        while (<$fh>) {
            my $line = $_;
            chomp($line);

	    my @row;
	    push(@row, $comp_code);
            my @row_tmp = split(/,/, $line);
	    push(@row, @row_tmp);
	    for (my $ii = scalar(@row_tmp); $ii < 7; $ii++) {
		push(@row, "");
	    }
	    push(@row, "quandl");
	    if ($row[1] =~ /\d\d\d\d-\d\d-\d\d/) {
		push(@ary, \@row);
	    }
        }
    }
    close($fh);
    unlink($file);

    return @ary;
}

sub get_kr_hankyung
{
    my ($comp_code, $from, $to) = @_;

    my $days = date_diff($from, $to);

    my $scraper = scraper {
	process '//div[@class="day_price"]//iframe[@id="dailyPrice"]', 'frame_link' => '@src';
    };
    my $uri = "http://stock.hankyung.com/apps/analysis.current?itemcode=" . $comp_code;

    my $date_curr = "";
    my $date_prev = "";

    my $close_lastday;
    my $open;
    my $high;
    my $low;
    my $close = "";
    my $volume;
    my $adjclose = "";
    my $website = "hankyung";
    my $capital;

    my @ary;
    my $res = $scraper->scrape(URI->new($uri));
    if (exists($res->{frame_link})) {
	my $page_num = 1;
	my $year = 2015;
	while (scalar(@ary) <= $days) {
	    my $scraper2 = scraper {
		process '//table[@class="indx_list_ty1"]//td', 'td2[]' => 'TEXT';
	    };
	    my $uri2 = $res->{frame_link} . "&page=" . $page_num;
	    my $res2 = $scraper2->scrape(URI->new($uri2));
	    my @td2;
	    if (exists($res2->{td2})) {
		@td2 = @{$res2->{td2}};
		my $n_col = 8;
		for (my $n = 0; $n < @td2/$n_col; $n++) {
		    $date_prev = $date_curr;
                    $date_curr = $td2[$n*$n_col + 0];
		    $close  = $td2[$n*$n_col + 1];
		    $open   = $td2[$n*$n_col + 4];
		    $high   = $td2[$n*$n_col + 5];
		    $low    = $td2[$n*$n_col + 6];
		    $volume = $td2[$n*$n_col + 7];

		    my $date = $date_curr;
		    $date =~ s/\//\-/g;
                    if ($date_prev =~ /^01/ && $date_curr =~ /^12/) {
                        $year--;
                    }
                    $date = $year . "-" . $date;

		    $open =~ s/,//g;
		    $high =~ s/,//g;
		    $low =~ s/,//g;
		    $close =~ s/,//g;

		    $adjclose = "";
		    $website = "hankyung";
		    my @row = ($comp_code, $date, $open, $high, $low, $close, $volume, $adjclose, $website);
		    push(@ary, \@row);
		}
	    }
	    else {
		last;
	    }
	    $page_num++;
	}
    }

    return @ary;
}

sub get_market_db
{
    my ($comp_code) = @_;

    my $driver = "SQLite";
    my $db = $database_name;
    my $dsn = "DBI:$driver:dbname=$db";
    my $table_name = $company_list_table;
    my $userid = "";
    my $password = "";
    my $dbh = DBI->connect($dsn, $userid, $password, {RaiseError => 1});

    my $stmt = "select count(*) from sqlite_master where name = \"$table_name\";";
    my $sth = $dbh->prepare($stmt);
    $sth->execute();
    if ($sth->fetchrow_array == 0) {
	return "";
    }

    $stmt = "select market from $table_name where code = \"$comp_code\";";
    $sth = $dbh->prepare($stmt);
    $sth->execute();

    my $market = $sth->fetchrow_array();
    $sth->finish();
    $dbh->disconnect();

    if ($market) {
	return $market;
    }
    else {
	return "";
    }
}

sub get_kr_market
{
    my ($comp_code) = @_;

    my $market = get_market_db($comp_code);
    if ($market) {
	return $market;
    }

    my $scraper_bmg = scraper {
	process '//title', 'mkt' => 'TEXT';
	process '//div[@class="snapshot"]/table[@class="snapshot_table"]/tr/td[@class="last"]', 'primary_exchange' => 'TEXT';
    };
    my $bmg = "http://www.bloomberg.com/quote/" . $comp_code . ":KS";
    my $uri = new URI($bmg);

    my $res;
    eval {
	$res = $scraper_bmg->scrape($uri);
    };
    if ($@) {
	print $@, "\n";
    }
    $market = "Unknown";
    if (defined($res) && exists($res->{mkt})) {
	if ($res->{mkt} =~ /Korea SE/) {
	    $market = "KRX";
	}
	elsif ($res->{mkt} =~ /KOSDAQ/) {
	    $market = "KOSDAQ";
	}
	elsif ($res->{mkt} =~ /Konex/) {
	    $market = "Konex";
	}
	elsif (exists($res->{primary_exchange})) {
	    if ($res->{primary_exchange} =~ /Korea SE/) {
		$market = "KRX";
	    }
	    elsif ($res->{primary_exchange} =~ /KOSDAQ/) {
		$market = "KOSDAQ";
	    }
	    elsif ($res->{primary_exchange} =~ /Konex/) {
		$market = "Konex";
	    }
	}
    }
    return $market;
}

sub parse_excel
{
    my ($filename) = @_;

    my $parser = Spreadsheet::ParseExcel->new();
    my $workbook = $parser->parse($filename);
    die $parser->error() if !defined $workbook;

    my @ary;
    for my $worksheet ($workbook->worksheets()) {
	my ($row_min, $row_max) = $worksheet->row_range();
	my ($col_min, $col_max) = $worksheet->col_range();

	for my $row ($row_min .. $row_max) {
	    my @each_row;
	    for my $col ($col_min .. $col_max) {
		my $cell = $worksheet->get_cell($row,$col);
		if ($cell) {
		    my $s = Encode::decode_utf8($cell->value());
		    $s =~ s/(^\s+|\s+$)//g;
		    $s = Encode::encode_utf8($s);
		    if ($s) {
			push(@each_row, $s);
		    }
		    else {
			push(@each_row, "");
		    }
		}
		else {
		    push(@each_row, "");
		}
	    }
	    if (@each_row) {
		push(@ary, \@each_row);
	    }
	}
    }
    return @ary;
}

sub create_kr_company_list
{
    my ($in_xls_en, $in_xls_kr, $in_xls_del, $date) = @_;

    my @ary_en = parse_excel($in_xls_en);
    my @ary_kr = parse_excel($in_xls_kr);
    @ary_en = grep($_->[0] =~ /\d\d\d\d\d\d/, @ary_en);
    @ary_kr = grep($_->[1] =~ /\d\d\d\d\d\d/, @ary_kr);

    my %hh_comp_list;
    for (my $i = 0; $i < @ary_en; $i++) {
	my $comp_code = $ary_en[$i][0];
	my %hh_comp = (
	    "code"             => $ary_en[$i][0],
	    "name_en"          => $ary_en[$i][1],
	    "industry_code_en" => $ary_en[$i][2],
	    "industry_en"      => $ary_en[$i][3],
	    "listed_shares"    => $ary_en[$i][4],
	    "capital_stock_en" => $ary_en[$i][5],
	    "pervalue"         => $ary_en[$i][6],
	    "currency"         => $ary_en[$i][7],
	    "unknown"          => $ary_en[$i][8],
	    "last_active_date" => $date,
	);
	
	for (my $j = 0; $j < @ary_kr; $j++) {
	    if ($comp_code == $ary_kr[$j][1]) {
		$hh_comp{"name_kr"}     = $ary_kr[$j][2];
		$hh_comp{"industry_kr"} = $ary_kr[$j][4];
		$hh_comp{"phone"}       = $ary_kr[$j][9];
		$hh_comp{"address"}     = $ary_kr[$j][10];
		last;
	    }
	}

	$hh_comp_list{$comp_code} = \%hh_comp;
    }

    my $delisted_old = $work_dir . "KRX_delisted_20150217.xls";
    my @ary_del_old = parse_excel($delisted_old);
    my @ary_del = parse_excel($in_xls_del);
    push(@ary_del, @ary_del_old);

    my $max_delisted_num = 0;
    for (my $i = 0; $i < @ary_del; $i++) {
	my $comp_code = $ary_del[$i][0];
	$comp_code =~ s/A//g;
	my $name_kr = $ary_del[$i][1];
	my $date_del = $ary_del[$i][2];
	$date_del =~ s/\//\-/g;
	my $reason_del = $ary_del[$i][3];
	my %delisted = (
	    "date" => $date_del,
	    "reason" => $reason_del
	    );
	
	if (exists($hh_comp_list{$comp_code})) {
	    my $hh_r = $hh_comp_list{$comp_code};
	    my $hh_r2 = $hh_r->{"delisted"};
	    $hh_r2->{$date_del} = \%delisted;
	    if ($max_delisted_num < scalar(keys(%{$hh_r2}))) {
		$max_delisted_num = scalar(keys(%{$hh_r2}));
	    }

	    my @a = sort {$b cmp $a} (keys(%{$hh_r2}));
	    $hh_r->{"last_active_date"} = $a[0];
	}
	else {
	    my %d = ($date_del => \%delisted);
	    my %hh_comp = (
		"code" => $comp_code,
		"name_kr" => $name_kr,
		"delisted" => \%d,
		"last_active_date" => $date_del,
		);
	    $hh_comp_list{$comp_code} = \%hh_comp;
	}
    }

    foreach my $comp_code (keys(%hh_comp_list)) {
	my $row_r = $hh_comp_list{$comp_code};
	$row_r->{"market"} = get_kr_market($comp_code);
	print "Getting market type for " . $comp_code . "/" . $row_r->{"name_kr"} . " => " . $row_r->{"market"} . "\n";
    }

    my @row0_0 = qw/code name_en industry_code_en industry_en listed_shares capital_stock_en pervalue currency unknown name_kr industry_kr phone address market last_active_date/;

    my @row0;
    push(@row0, @row0_0);
    for (my $i = 0; $i < $max_delisted_num; $i++) {
	push(@row0, "delisted${i}_date");
	push(@row0, "delisted${i}_reason");
    }

    my @ary_list;
    push(@ary_list, \@row0);

    foreach my $comp_code (keys(%hh_comp_list)) {
	my @row;
	for (my $i = 0; $i < @row0_0; $i++) {
	    my $hh_comp_r = $hh_comp_list{$comp_code};
	    if (exists($hh_comp_r->{$row0[$i]})) {
		push(@row, $hh_comp_r->{$row0[$i]});
	    }
	    else {
		push(@row,"");
	    }
	}
	my $hh_comp_r = $hh_comp_list{$comp_code};
	my $delisted_num = 0;
	if (exists($hh_comp_r->{"delisted"})) {
	    my %hh_delisted = %{$hh_comp_r->{"delisted"}};
	    foreach my $del_date (sort {$b cmp $a} (keys(%hh_delisted))) {
		push(@row, $hh_delisted{$del_date}->{"date"});
		push(@row, $hh_delisted{$del_date}->{"reason"});
		$delisted_num++;
	    }
	}
	for (my $i = $delisted_num; $i < $max_delisted_num; $i++) {
	    push(@row, ""); # date
	    push(@row, ""); # reason
	}
	push(@ary_list, \@row);
    }

    db_write("kr_company_list", \@ary_list, 1);

    return %hh_comp_list;
}

sub db_write
{
    my ($table_name, $data_r, $num_pkey) = @_;
    my @data = @$data_r;

    my $driver = "SQLite";
    my $db = $database_name;
    my $dsn = "DBI:$driver:dbname=$db";
    my $userid = "";
    my $password = "";
    my $dbh = DBI->connect($dsn, $userid, $password, {RaiseError => 1});
    my $is_exist = 1;
    my $start_row = 0;
    my $stmt;
    my $sth;

    if (defined($num_pkey)) {
	$start_row = 1;
	$stmt = "select count(*) from sqlite_master where name = \"" . $table_name . "\";";
	$sth = $dbh->prepare($stmt);
	$sth->execute();
	while (my @r = $sth->fetchrow_array) {
	    if ($r[0] == 0) {
		$is_exist = 0;
	    }
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
	for (my $i = 0; $i < $num_pkey; $i++) {
	    if ($i == $num_pkey - 1) {
		$stmt = $stmt . $row0[$i] . "));";
	    }
	    else {
		$stmt = $stmt . $row0[$i] . ",";
	    }
	}
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

    for (my $i = $start_row; $i < scalar(@data); $i++) {
	my @row = @{$data[$i]};

	my $sep = "\"";
	$stmt = "insert or replace into " . $table_name . " values (" ;	
	for (my $j = 0; $j < scalar(@row); $j++) {
	    $stmt = $stmt . $sep . $row[$j] . $sep;
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

sub get_current_data
{
    my ($comp_code) = @_;

    my $filename = $comp_code . ".csv";

    my @ary_data;
    if (-e $filename) {
	open(IN, "<", $filename) or die "$filename exists but is not readable\n";
	while (<IN>) {
	    my $line = $_;
	    chomp($line);
	    my @ary = split(/,/, $line);
	    push(@ary_data, \@ary);
	}
	close(IN);
    }
    else {
	# print "file does not exist\n";
    }
    my $start = $ary_data[$#ary_data]->[0];
    my $end = $ary_data[0]->[0];

    print $start, " ", $end, "\n";
}

sub marge_data
{
    my ($yh_r, $qu_r, $go_r, $hk_r) = @_;

    my %hh;
    my @yh = @$yh_r;
    my @qu = @$qu_r;
    my @go = @$go_r;
    my @hk = @$hk_r;

    for (my $i = 0; $i < @yh; $i++) {
	my @row = @{$yh[$i]};
	$hh{$row[1]} = \@row;
    }

    for (my $i = 0; $i < @qu; $i++) {
	my @row = @{$qu[$i]};
	if (!exists($hh{$row[1]})) {
	    $hh{$row[1]} = \@row;
	}
    }

    for (my $i = 0; $i < @go; $i++) {
	my @row = @{$go[$i]};
	if (!exists($hh{$row[1]})) {
	    $hh{$row[1]} = \@row;
	}
    }

    for (my $i = 0; $i < @hk; $i++) {
	my @row = @{$hk[$i]};
	if (!exists($hh{$row[1]})) {
	    $hh{$row[1]} = \@row;
	}
    }

    my @ary;
    foreach my $k (sort(keys(%hh))) {
	my $row_r = $hh{$k};
	push(@ary, $row_r);
    }

    return @ary;
}

sub get_kr_stock_all
{
    my ($from, $to) = @_;

    my $filename_en  = $work_dir . "KRX_list_en_" . $to . ".xls";
    my $filename_kr  = $work_dir . "KRX_list_kr_" . $to . ".xls";
    my $filename_del = $work_dir . "KRX_delisted_" . $to . ".xls";

    get_krx_stocklist($filename_kr, $filename_en, $filename_del);

    my %hh_company = create_kr_company_list($filename_en, $filename_kr, $filename_del, $to);

    my $n_data = 0;
    my $n_total_data = keys(%hh_company);

    foreach my $r (keys(%hh_company)) {
	$n_data++;
	my $start = time();

	my %comp = %{$hh_company{$r}};
	my $comp_code = $comp{"code"};
	my $market = $comp{"market"};
	my $name_kr = $comp{"name_kr"};

	print "Checking $comp_code/$name_kr\n";

	if ($from eq $to && $comp{"last_active_date"} ne $to) {
	    # this company is delisted
	    print "$comp_code/$name_kr is skipping from=$from to=$to lad=" . $comp{"last_active_date"} . "\n";
	    next;
	}

	my @data_yh;
	my @data_qu;
	my @data_go;
	my @data_hk;
	my $found = "";
	for (my $repeat = 1; $repeat <= 2; $repeat++) {
	    if ($market eq "Konex" || $market eq "unknown") {
		eval {
		    if (scalar(@data_hk) == 0) {
			@data_hk = get_kr_hankyung($comp_code, $from, $to);
		    }
		};
		if ($@) {
		    print $@, "\n";
		}

		if ($market eq "Konex") {
		    last;
		}
	    }

	    eval {
		if (scalar(@data_yh) == 0) {
		    @data_yh = get_kr_yahoo($comp_code, $market, $from, $to);
		}
	    };
	    if ($@) {
		print $@, "\n";
	    }

	    eval {
		if (scalar(@data_qu) == 0) {
		    @data_qu = get_kr_quandl($comp_code, $market, $from, $to);
		}
	    };
	    if ($@) {
		print $@, "\n";
	    }

	    eval {
		if (scalar(@data_go) == 0) {
		    @data_go = get_kr_google($comp_code, $market, $from, $to);
		}
	    };
	    if ($@) {
		print $@, "\n";
	    }
	}
	if (scalar(@data_yh)) {
	    $found = $found . "yahoo" . scalar(@data_yh);
	}
	if (scalar(@data_qu)) {
	    $found = $found . "quandl" . scalar(@data_qu);
	}
	if (scalar(@data_go)) {
	    $found = $found . "google" . scalar(@data_go);
	}
	if (scalar(@data_hk)) {
	    $found = $found . "hankyung" . scalar(@data_hk);
	}
	my @data = marge_data(\@data_yh, \@data_qu, \@data_go, \@data_hk);
	my @row0 = qw/code date open high low close volume adjclose website/;
	unshift(@data, \@row0);
	db_write($stock_table, \@data, 2);

	my $end = time();
	my $cur_time = localtime();
	print "$comp_code/$name_kr is found at $found ($n_data/$n_total_data) " . sprintf("%.2f", $n_data/$n_total_data*100.0) . " % data_num = " . scalar(@data) . " duration = " . ($end - $start) . " seconds time = $cur_time\n";
    }
}

sub db_drop_table
{
    my ($table_name) = @_;
    my $driver = "SQLite";
    my $db = "stock.db";
    my $dsn = "DBI:$driver:dbname=$db";
    my $userid = "";
    my $password = "";
    my $dbh = DBI->connect($dsn, $userid, $password, {RaiseError => 1});
    my $stmt = "drop table " . $table_name . ";";
    my $ret = $dbh->do($stmt);
    if ($ret < 0) {
	print $DBI::errstr;
    }
}

sub get_krx_stocklist
{
    my ($file_kr, $file_en, $file_delisted) = @_;
    my $file_download = "/home/eii/Downloads/Data.xls";

    my $url_kr = "http://www.krx.co.kr/m6/m6_1/m6_1_1/JHPKOR06001_01.jsp";
    my $click_kr = "//img[\@id=\"excelBtn\"]";
    my $url_en = "http://eng.krx.co.kr/m6/m6_1/m6_1_1/JHPENG06001_01.jsp";
    my $click_en = "//img[\@alt=\"Download\"]";
    my $url_delisted = "http://www.krx.co.kr/m6/m6_1/m6_1_5/JHPKOR06001_05.jsp";
    my $click_delisted = "//img[\@id=\"excelBtn\"]";
#    my $date_fr = "//input[\@id=\"fr_work_dt\"]";

    my $waitsec = 20;
    my $profile = Selenium::Remote::Driver::Firefox::Profile->new();

    $profile->set_preference(
	"browser.helperApps.neverAsk.saveToDisk" => "application/vnd.ms-excel,text/xls",
	"browser.download.defaultFolder" => "/tmp/",
	"browser.download.downloadDir" => "/tmp/",
	"browser.download.dir" => "/tmp/",
	"browser.downlaod.folderList" => "2",
	);
    $profile->set_boolean_preference(
	"browser.download.useDownloadDir" => 1,
	"browser.download.manager.showWhenStarting" => 0,
	"browser.helperApps.alwaysAsk.force" => 0,
	"browser.download.panel.shown" => 0,
	);

    my $driver = new Selenium::Remote::Driver(
	'browser_name' => 'firefox',
	'port' => '4444',
	'firefox_profile' => $profile,
	'extra_capabilities' => { 'name' => "eii" },
	);

    unlink($file_download);

    my $cnt = 0;
    eval {
	$driver->get($url_kr);
	sleep($waitsec);
	$driver->find_element($click_kr)->click();
	sleep($waitsec);
	rename($file_download, $file_kr);
	$cnt++;
    };
    if ($@) {
	print "Getting the  KRX Korean list failed: $@\n";
    }

    eval {
	$driver->get($url_en);
	sleep($waitsec);
	$driver->find_element($click_en)->click();
	sleep($waitsec);
	rename($file_download, $file_en);
	$cnt++;
    };
    if ($@) {
	print "Getting the KRX English list failed: $@\n";
    }

    eval {
	$driver->get($url_delisted);
	sleep($waitsec);
#	$driver->find_element($date_fr)->clear();
#        $driver->find_element($date_fr)->send_keys($date_from);
	$driver->find_element($click_delisted)->click();
	sleep($waitsec);
	rename($file_download, $file_delisted);
	$cnt++;
    };
    if ($@) {
	print "Getting the KRX Korean Delisted list failed: $@\n";
    }

    $driver->quit();

    return $cnt;
}



########## start

my $dt = DateTime->now();
my $to = $dt->ymd('-');
$dt->subtract(days => 30);
my $from = $dt->ymd('-');

# my $from = "1990-01-01";
get_kr_stock_all($from, $to);





my @aa_konex = qw/091270 185190 140660 180400 179280 192240 194860 179440 144630 136660 156170 208890 181980 092870 204690 189350 127160 189700 203400 165270 094360 122640 185280 142760 199800 082220 202960/;

my @aa_krx = qw/100250 016610 017810 004700 121550 083350 009420 036570 002210 101060 051310 163560 033270 023800 004410 004910 011700 010060 015230 010690 008000 003600 010050 079980 099350 014530/;

