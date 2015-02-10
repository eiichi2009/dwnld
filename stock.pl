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

my $work_dir = "/home/eii/korean_stock/";
my $company_list_table = "kr_company_list";
my $stock_table = "kr_stock";
my $database_name = "stock.db";
my $quandl_auth_token;

sub is_valid_business_day_kr
{
    my ($date) = @_;
    if ($date =~ /\d\d\d\d-\d\d-\d\d/) {
	return 1;
    }
    return 0;
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
	    if (is_valid_business_day_kr($row[1])) {
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

    my $scraper = scraper {
	process '//table[@class="gf-table historical_price"]//td', 'td[]' => 'TEXT';
    };

    # There are approximately 250 data in one year. In 14 years between 2000-01-01 and 2015-01-01, there are 250*14=3500 data.
    my @ary;

    my $mon_f = qw/XXX Jan Feb Mar Apr May Jun Jul Aug Sep Oct Nov Dec/[scalar(substr($from, 5, 2))];
    my $day_f = substr($from, 8, 2) * 1;
    my $year_f = substr($from, 0, 4);
    my $mon_t = qw/XXX Jan Feb Mar Apr May Jun Jul Aug Sep Oct Nov Dec/[scalar(substr($to, 5, 2))];
    my $day_t = substr($to, 8, 2) * 1;
    my $year_t = substr($to, 0, 4);

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
    while ($start < 4000) {
	my $uri = "http://www.google.com/finance/historical?q=" . $d_mkt . "%3A" . $comp_code . "&startdate=" . $startdate . "&enddate=" . $enddate . "&num=200&start=" . $start;
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

    my @ary_col;
    if (scalar(@ary) == 0) {
	return @ary_col;
    }

    my $n_col = 6; # date / open / high / low / close / volume
    my %mon2num = ("Jan"=>"01", "Feb"=>"02", "Mar"=>"03", "Apr"=>"04", "May"=>"05", "Jun"=>"06", "Jul"=>"07", "Aug"=>"08", "Sep"=>"09", "Oct"=>"10", "Nov"=>"11", "Dec"=>"12");
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
	if (is_valid_business_day_kr($row[1])) {
	    push(@ary_col, \@row);
	}
    }
    return @ary_col;
}

sub get_kr_konex
{
    my ($comp_code) = @_;

    # format
    # 날짜       | 가 격 | 전 일 비| 등 락 률 | 거 래 량
    # 2015/01/08 | 5,980 | ▼ 210   | -3.39%   | 12869
    my $scraper = scraper {
        process '//table[@summary]/tbody/tr[@bgcolor="#ffffff"]//td', 'td[]' => 'TEXT';
    };
    my $uri = "http://www.konex38.co.kr/forum/?code=" . $comp_code . "&o=sise";
    my $res = $scraper->scrape(URI->new($uri));

    my @ary;
    my @ary_col;
    if (exists($res->{td})) {
	@ary = @{$res->{td}};
    }
    else {
	return@ary_col;
    }

    my $n_col = 5;
    for (my $i = 0; $i < scalar(@ary) / $n_col; $i++) {
	my @row;
	push(@row,$comp_code); # code

	$ary[$i*$n_col + 0] =~ s/\//-/g;
	push(@row, $ary[$i*$n_col + 0]); # Date

	push(@row, ""); # Open
	push(@row, ""); # High
	push(@row, ""); # low

	push(@row, $ary[$i*$n_col + 1]); # Close
	push(@row, $ary[$i*$n_col + 4]); # Volume

	push(@row, ""); # AdjClose
	push(@row, "konex"); # website

	if (is_valid_business_day_kr($row[1])) {
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
    my $uri = "http://www.quandl.com/api/v1/datasets/YAHOO/" . $market_type . "_" . $comp_code . ".csv" . "?auth_token=$quandl_auth_token";
    my ($fh, $file) = File::Temp::tempfile(DIR => '/tmp', UNLINK => 1);

#    `wget $uri -O $file`;
#    if (-e $file) {

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
	    if (is_valid_business_day_kr($row[1])) {
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
    my ($comp_code) = @_;

    my $scraper = scraper {
	process '//div[@class="data_area"]//p//strong', 'close' => 'TEXT';
	process '//div[@class="data_area"]//dl[@class="indx_list kos_indx"]//dd', 'dd[]' => 'TEXT';
	process '//div[@class="time"]//span[@class="ymd"]', 'time' => 'TEXT';
    };
    my $uri = "http://stock.hankyung.com/apps/analysis.current?itemcode=" . $comp_code;

    my $date = "";

    my $close_lastday;
    my $open;
    my $high;
    my $low;
    my $close = "";
    my $volume;
    my $adjclose = "";
    my $website = "hankyung";
    my $capital;

    my $res = $scraper->scrape(URI->new($uri));
    if (exists($res->{close})) {
	$close = $res->{close};
    }
    if (exists($res->{dd})) {
	my @dd = @{$res->{dd}};
	$close_lastday = $dd[0];
	$open = $dd[1];
	$high = $dd[2];
	$low = $dd[3];
	$volume = $dd[4];
	$capital = $dd[5];

	$open =~ s/,//g;
	$high =~ s/,//g;
	$low =~ s/,//g;
	$close =~ s/,//g;
    }
    if (exists($res->{time})) {
	$date = $res->{time};
	$date =~ s/\//\-/g;
    }

    my @ary;
    my @row = ($comp_code, $date, $open, $high, $low, $close, $volume, $adjclose, $website);
    push(@ary, \@row);
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

sub get_jp_yahoo
{
    my ($comp_code, $from, $to) = @_;
    my $scraper = scraper {
#        process '//table[@class="boardFin yjSt marB6"]//tr//td[@*]', 'td[]' => 'TEXT';
        process '//table[@class="boardFin yjSt marB6"]//tr//td', 'td[]' => 'TEXT';
    };

    my $year_f = substr($from, 0, 4);
    my $mon_f  = substr($from, 5, 2); $mon_f =~ s/^0//g;
    my $day_f  = substr($from, 8, 2); $day_f =~ s/^0//g;
    my $year_t = substr($to,   0, 4);
    my $mon_t  = substr($to,   5, 2); $mon_t =~ s/^0//g;
    my $day_t  = substr($to,   8, 2); $day_t =~ s/^0//g;

    my $n_col = 7;
    my @ary;
    my @row0 = ("date", "open", "high", "low", "close", "volume", "adj_close");
    push(@ary, \@row0);
    for (my $idx = 1; $idx < 500; $idx++) {
        my $uri = "http://info.finance.yahoo.co.jp/history/?code=" . $comp_code . ".T&" .
            "sy=" . $year_f . "&sm=" . $mon_f . "&sd=" . $day_f . "&ey=" . $year_t . "&em=" . $mon_t . "&ed=" . $day_t . "&tm=d&p=" . $idx;

        my $res;
        my $retry = 3;
        while ($retry > 0) {
            eval {
                $res = $scraper->scrape(URI->new($uri));
            };
            if ($@) {
		# error
                print $@, "\n";
                print $uri, "\n";
            }
            else {
                last;
            }
            $retry--;
        }

        if (exists($res->{td})) {
            my @td = map {Encode::encode("utf8",$_)} @{$res->{td}};
            @td = map {$_ =~ s/(年|月)/-/g; $_ =~ s/日//g; $_} @td;
            @td = map {s/[, ]//g; $_} @td;

            my $row_start = 0;
            while ($row_start < @td) {
                my @row;
                my $row_len = 0;
                while ($row_len < $n_col && $row_start+$row_len < @td) {
                    if ($row_len > 0 && $td[$row_start + $row_len] =~ /\d+-\d+-\d+/) {
                        # this is not a regular data
                        for (my $i = $row_len; $i < $n_col; $i++) {
                            push(@row, 0);
                        }
                        last;
                    }
                    else {
                        push(@row, $td[$row_start + $row_len]);
                        $row_len++;
                    }
                }
                push(@ary, \@row);
                $row_start += $row_len;
            }
        }
        elsif ($retry == 0) {
            next;
        }
        else {
            last;
        }
    }
    return @ary;
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
		    my $s = Encode::encode("utf8", $cell->value());
		    $s =~ s/(^ +| +$)//g;
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
    my ($in_xls_en, $in_xls_kr, $in_csv_delisted_en, $in_csv_delisted_kr) = @_;

    my @ary_delisted_en;
    if (defined($in_csv_delisted_en) && -e $in_csv_delisted_en) {
	open(IN, "<", $in_csv_delisted_en);
	while (<IN>) {
	    my $line = $_;
	    chomp($line);
	    if ($line =~ /(\d\d\d\d\d\d)\s+([0-9a-zA-Z- &.',*]+)\s+(\d\d\d\d\/\d\d\/\d\d)\s+DELISTING/) {
		my $code = $1;
		my $name_en = $2;
		my $delisted_date1 = $3;
		$delisted_date1 =~ s/\//-/g;

		my $double_delisting = 0;
		for (my $i = 0; $i < @ary_delisted_en; $i++) {
		    my $r = $ary_delisted_en[$i];
		    if ($r->[0] == $code) {
			my $delisted_date_tmp = $r->[2];
			if ($delisted_date_tmp gt $delisted_date1) {
			    $r->[2] = $delisted_date1;
			}
			$r->[3] = $delisted_date_tmp;
			$double_delisting = 1;
			last;
		    }
		}

		my $delisted_date2 = "";
		if ($double_delisting == 0) {
		    my @row = ($code, $name_en, $delisted_date1, $delisted_date2);
		    push(@ary_delisted_en, \@row);
		}
	    }
	}
	close(IN);
    }

    my @ary_delisted_kr;
    if (defined($in_csv_delisted_kr) && -e $in_csv_delisted_kr) {
	open(IN, "<", $in_csv_delisted_kr);
	while (<IN>) {
	    my $line = $_;
	    chomp($line);
	    if ($line =~ /A(\d\d\d\d\d\d)\s+(.+)\s+(\d\d\d\d\/\d\d\/\d\d)\s+(.+)/) {
		my $code = $1;
		my $name_kr = $2;
		my $date = $3;
		my $reason = $4;

		$name_kr =~ s/^\s+|\s+$//g;
		$reason =~ s/^\s+|\s+$//g;
		my @row = ($code, Encode::decode_utf8($name_kr), $date, Encode::decode_utf8($reason));
		push(@ary_delisted_kr, \@row);
	    }
	}
	close(IN);
    }

    my @ary_en = parse_excel($in_xls_en);
    my @ary_kr = parse_excel($in_xls_kr);
    @ary_en = grep($_->[0] =~ /\d\d\d\d\d\d/, @ary_en);
    @ary_kr = grep($_->[1] =~ /\d\d\d\d\d\d/, @ary_kr);

    my %hh_comp_list;
    for (my $i = 0; $i < @ary_en; $i++) {
	my $comp_code = $ary_en[$i][0];
	my %hh_comp = (
	    "code" => $ary_en[$i][0],
	    "name_en" => $ary_en[$i][1],
	    "industry_code_en" => $ary_en[$i][2],
	    "industry_en" => $ary_en[$i][3],
	    "listed_shares" => $ary_en[$i][4],
	    "capital_stock_en" => $ary_en[$i][5],
	    "pervalue" => $ary_en[$i][6],
	    "currency" => $ary_en[$i][7],
	    "unknown" => $ary_en[$i][8],
	);
	
	for (my $j = 0; $j < @ary_kr; $j++) {
	    if ($comp_code == $ary_kr[$j][1]) {
		$hh_comp{"name_kr"} = $ary_kr[$j][2];
		$hh_comp{"industry_kr"} = $ary_kr[$j][4];
		$hh_comp{"phone"} = $ary_kr[$j][9];
		$hh_comp{"address"} = $ary_kr[$j][10];
		last;
	    }
	}

	$hh_comp_list{$comp_code} = \%hh_comp;
    }

    for (my $i = 0; $i < @ary_delisted_en; $i++) {
	my $comp_code = $ary_delisted_en[$i][0];
	my $name_en = $ary_delisted_en[$i][1];
	my $date_delisted1 = $ary_delisted_en[$i][2];
	my $date_delisted2 = $ary_delisted_en[$i][3];

	my $name_kr;
	my $date_delisted_kr;
	my $reason_delisted;
	for (my $j = 0; $j < @ary_delisted_kr; $j++) {
	    if ($ary_delisted_kr[$j][0] eq $comp_code) {
		$name_kr = Encode::encode("utf8", $ary_delisted_kr[$j][1]);
		$date_delisted_kr = $ary_delisted_kr[$j][2];
		$reason_delisted = Encode::encode("utf8", $ary_delisted_kr[$j][3]);
	    }
	}

	my %hh_comp = (
	    "code" => $comp_code,
	    "name_en" => $name_en,
	    "date_delisted1" => $date_delisted1,
	    "date_delisted2" => $date_delisted2,
	    "name_kr" => $name_kr,
	    "reason_delisted" => $reason_delisted,
	    );
	$hh_comp_list{$comp_code} = \%hh_comp;
    }

    foreach my $comp_code (keys(%hh_comp_list)) {
	my $row_r = $hh_comp_list{$comp_code};
	${$row_r}{"market"} = get_kr_market($comp_code);
	print "Getting market type for " . $comp_code . "/" . ${$row_r}{"name_en"} . " => " . ${$row_r}{"market"} . "\n";
    }

    my @row0 = qw/code name_en industry_code_en industry_en listed_shares capital_stock_en pervalue currency unknown name_kr industry_kr phone address market date_delisted1 date_delisted2 reason_delisted/;

    my @ary_list;
    push(@ary_list, \@row0);
    foreach my $comp_code (keys(%hh_comp_list)) {
	my @row;
	for (my $i = 0; $i < @row0; $i++) {
	    my $obj = $hh_comp_list{$comp_code};
	    if (exists($obj->{$row0[$i]})) {
		push(@row, $hh_comp_list{$comp_code}->{$row0[$i]});
	    }
	    else {
		push(@row,"");
	    }
	}
	push(@ary_list, \@row);
    }

    db_write("kr_company_list", \@ary_list, 1);

    return %hh_comp_list;
}

sub create_jp_company_list
{
    my $scraper = scraper {
        process '//table[@class="styleShiryo"]//tr//td[1]', 'td[]' => 'HTML';
        process '//table[@class="styleShiryo"]//tr//td[@class="center"]//a', 'href[]' => '@href';
    };
    my $uri = "http://www.tse.or.jp/market/data/listed_companies/";
    my $res;
    eval {
        $res = $scraper->scrape(URI->new($uri));
    };
    if ($@) {
        print $@, "\n";
    }

    my %file2market;
    if (exists($res->{td}) && exists($res->{href})) {
	my $data_num = scalar(@{$res->{td}});
	for (my $i = 0; $i < $data_num; $i++) {
	    my $market = Encode::encode("utf8", $res->{td}->[$i]);
	    my $filename = Encode::encode("utf8", $res->{href}->[$i]);
	    $file2market{$filename} = $market;
	}
    }

    my @company_list;
    # 日付,コード,銘柄名,33業種コード,33業種区分,17業種コード,17業種区分,規模コード,規模区分, market
    my @row0 = ("date", "code", "name", "type33_code", "type33_name", "type17_code", "type17_name", "size_code", "size_name", "market");
    push(@company_list, \@row0);

    if (exists($res->{href})) {
        my @ary_href = @{$res->{href}};
        for (my $i = 0; $i < @ary_href; $i++) {
            my $uri = $ary_href[$i];
            my $filename = "file" . $i;
            if ($uri =~ /([0-9a-z]*?.xls)$/) {
                $filename = $1;
            }
            my $file = $work_dir . $filename;
            my $ua = "Mozilla/5.0 (compatible; MSIE 9.0; Windows NT 6.1; Trident/5.0)";
            my $to = "20";
            my $lwp = LWP::UserAgent->new(agent=>$ua, timeout=>$to);
            my $res = $lwp->get($uri, ':content_file'=>$file);
            my @ary;
            if ($res->is_success) {
                @ary = parse_excel($file);
            }
            else {
                print "failed\n";
            }

            @ary = grep($_->[1] =~ /\d\d\d\d/, @ary);
	    my $row_num = scalar(@{$ary[0]});
	    if ($row_num == 3) {
		@ary = map { push(@{$_}, @{["", "", "", "", "", ""]}); $_ } @ary;
	    }
	    elsif ($row_num == 5) {
		@ary = map { push(@{$_}, @{["", "", "", ""]}); $_ } @ary;
	    }
	    elsif ($row_num == 9) {
	    }
	    else {
	    }

	    my $market_name = "";
	    if ($file2market{$uri} =~ /第一部/) {
		$market_name = "tosho1";
	    }
	    elsif ($file2market{$uri} =~ /第二部/) {
		$market_name = "tosho2;"
	    }
	    elsif ($file2market{$uri} =~ /マザーズ/) {
		$market_name = "mothers";
	    }
	    elsif ($file2market{$uri} =~ /REIT/) {
		$market_name = "reit";
	    }
	    elsif ($file2market{$uri} =~ /ETF/) {
		$market_name = "etf";
	    }
	    elsif ($file2market{$uri} =~ /PRO/) {
		$market_name = "pro";
	    }
	    elsif ($file2market{$uri} =~ /JASDAQ/) {
		$market_name = "jasdaq";
	    }
	    else {
		print "XXX\n";
	    }
	    @ary = map { push(@{$_},  $market_name); $_ } @ary;
            push(@company_list, @ary);
        }
    }

    db_write("jp_company_list", \@company_list);

    my %hh_comp_list;
    for (my $i = 0; $i < @company_list; $i++) {
	my @row = @{$company_list[$i]};
	my %hh_comp = (
	    date        => $row[0],
	    code        => $row[1],
	    name        => $row[2],
	    type33_code => $row[3],
	    type33_name => $row[4],
	    type17_code => $row[5],
	    type17_name => $row[6],
	    size_code   => $row[7],
	    size_name   => $row[8],
	    market      => $row[9]
	    );
	$hh_comp_list{$hh_comp{code}} = \%hh_comp;
    }
    return \%hh_comp_list;
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

    for (my $i = 1; $i < scalar(@data); $i++) {
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
    my ($yh_r, $qu_r, $go_r, $kn_r, $hk_r) = @_;

    my %hh;
    my @yh = @$yh_r;
    my @qu = @$qu_r;
    my @go = @$go_r;
    my @kn = @$kn_r;
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

    for (my $i = 0; $i < @kn; $i++) {
	my @row = @{$kn[$i]};
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
    my $date_of_list = "20150209";
    my $filename_en = $work_dir . "KRX_list_en_" . $date_of_list . ".xls";
    my $filename_kr = $work_dir . "KRX_list_kr_" . $date_of_list . ".xls";
    my $filename_delisted_en = $work_dir . "KRX_delisted_en_" . $date_of_list . ".csv";
    my $filename_delisted_kr = $work_dir . "KRX_delisted_kr_" . $date_of_list . ".csv";

    my %hh_company = create_kr_company_list($filename_en, $filename_kr, $filename_delisted_en, $filename_delisted_kr);

    my $n_data = 0;
    my $n_total_data = keys(%hh_company);

    foreach my $r (keys(%hh_company)) {
	$n_data++;

	my $start = time();

	my %comp = %{$hh_company{$r}};
	my $comp_code = $comp{"code"};
	my $market = $comp{"market"};

	my $from = "1990-01-01";
	my $to   = "2015-02-09";  # strftime("%Y-%m-%d", localtime());

	my @data_yh;
	my @data_qu;
	my @data_go;
	my @data_kn;
	my @data_hk;
	my $found = "";
	for (my $repeat = 1; $repeat <= 2; $repeat++) {
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

	    eval {
		if (scalar(@data_kn) == 0) {
		    @data_kn = get_kr_konex($comp_code);
		}
	    };
	    if ($@) {
		print $@, "\n";
	    }

	    if ($market eq "Konex") {
		eval {
		    if (scalar(@data_hk) == 0) {
			@data_hk = get_kr_hankyung($comp_code);
		    }
		};
		if ($@) {
		    print $@, "\n";
		}
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
	if (scalar(@data_kn)) {
	    $found = $found . "konex" . scalar(@data_kn);
	}
	if (scalar(@data_hk)) {
	    $found = $found . "hankyung" . scalar(@data_hk);
	}
	my @data = marge_data(\@data_yh, \@data_qu, \@data_go, \@data_kn, \@data_hk);
	my @row0 = qw/code date open high low close volume adjclose website/;
	unshift(@data, \@row0);
	db_write($stock_table, \@data, 2);

	my $end = time();
	my $cur_time = localtime();
	print $comp{"name_en"} . " is found at " . $found . " (" . $n_data . "/" . $n_total_data . ") " . sprintf("%.2f", $n_data/$n_total_data*100.0) . " % data_num = " . scalar(@data) . " duration = " . ($end - $start) . " seconds " . "time = " . $cur_time . "\n";
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

sub daily_update
{
    my $date_of_list = "20150209";
    my $filename_en = $work_dir . "KRX_list_en_" . $date_of_list . ".xls";
    my $filename_kr = $work_dir . "KRX_list_kr_" . $date_of_list . ".xls";

    my @ary_en = parse_excel($filename_en);
    my @ary_kr = parse_excel($filename_kr);

    my %hh_company = create_kr_company_list($filename_en, $filename_kr, "", "");

    foreach my $comp_code (keys(%hh_company)) {
	print $comp_code, "\n";
    }
}

########## start
my $r = get_kr_stock_all();


