#!/usr/bin/perl -w
######################### load_realtimefeed_2DB.pl ###############################
# A script to load one MBTA real time feed file to the database
#
# USAGE:  load_realtimefeed_2DB.pl line [DEBUG|FULL]
# REQUIRES: DBI packages for MySQL, LWP::Simple
# HISTORY: 06/11/2011 - Adam Travis - Original file created
# 		12/30/2011 - Adam Travis - Converted to process only one file and accept parameter
#				Knows how to lookup URL and find if file is in subway or commuter rail format
#		12/31/2011 - Adam Travis - Implement Debugging levels and subroutines for cleaning
#				whitespace and formatting announcement time
#
###################################################################################
use strict;
use DBI;
use LWP::Simple;

### Variables to set connection
my $dsn = 'DBI:mysql:mbta_data';
my $user = 'username';  #must have SELECT, INSERT, UPDATE, DELETE authority
my $password = 'password';

### Variables for Processing
my $filepath = '~/AppData/Perl';
my $errorfile = 'load_realtimefeed_2DB.log';
my $secRefresh = 60;  #Set Minimum Refresh interval here (in seconds)

### Local Variables
my $start_time = localtime;
my $end_time;
my $num_args;
my $line;
my $DEBUG = 0; #set greater than 0 for Debugging, greater than 200 for full logging
my $dbh;
my $sth;
my $sqlquery;
my $url;  #the url to get real time data from
my $secDiff;  #number of second since last refresh
my $dataset_type;  #subway or commuter rail
my $content; #contents of the real-time data file from web
my $fileRowCount = 0;
my $insertCount = 0;

# Subway ONLY Data Variables
my $subline;
my $trip;
my $PlatformKey;
my $information_type;
my $announcement_time;
my $remaining_time;
my $revenue;
my $route;

# CR ONLY Data Variables
my $RecTimestamp;
my $Trip;
my $Destination;
my $Stop;
my $Scheduled;
my $Flag;
my $Vehicle;
my $Latitude;
my $Longitude;
my $Direction;
my $Speed;
my $Lateness;


################################### Begin Processing ######################################
### Get the Data set/line from arguments passed to STDIN
# Based on examples from: http://www.devdaily.com/perl/perl-command-line-arguments-read-args
# Exit unless we have the correct number of command-line args
$num_args = $#ARGV + 1;
if (($num_args < 1) or ($num_args > 2)) {
	print "\nUsage: load_realtimefeed_2DB.pl line [DEBUG|FULL]\n";
	exit;
}
#Get the two command line args
$line=$ARGV[0];
if ($num_args == 2) {
	if ($ARGV[1] eq 'DEBUG') {
		$DEBUG = 1;
	} elsif ($ARGV[1] eq 'FULL') {
		$DEBUG = 201;
	}
}

### Open error log
if ($DEBUG) {
	open LOG, ">>$filepath/$errorfile" 
		or die "Could not open log file!\n";
	print LOG "\n\nBegin Processing with DEBUG\n";
	print LOG "start_time = $start_time \n";
	print LOG "arg0 = $ARGV[0] \n";
	print LOG "arg1 = $ARGV[1] \n";
}

#################### Query to find if data needs to be refeshed ###########################
### Open Database
$dbh = DBI->connect($dsn, $user, $password, { RaiseError => 1, AutoCommit => 0 })
	or die "Could not connect to database: " . DBI->errstr;

### Query for Last Update, DataSet Type, and URL
$sqlquery = q{SELECT TIMESTAMPDIFF(SECOND,last_refresh_time,NOW()) seconds, dataset_type, data_url FROM rt_refresh WHERE dataset=?};
$sth = $dbh->prepare($sqlquery)
	or die "Couldn't prepare statement: " . $dbh->errstr;
$sth->execute($line);

if ($sth->rows == 0) {
    die "The line submitted could not be found in the database";
}
($secDiff,$dataset_type,$url) = $sth->fetchrow_array;
undef $sth; #explicit Garbage Collection to prevent warnings
### If DEBUG, output variables
if ($DEBUG) {
	print LOG "secDiff = $secDiff \n";
	print LOG "dataset_type = $dataset_type \n";
	print LOG "url = $url \n";
}

###################### Refresh the Data Set #############################################
### check if it is time for a refresh
if ($secDiff>$secRefresh) {
	### Update Refresh Time and commit
	$dbh->do(qq{UPDATE rt_refresh SET last_refresh_time=NOW() WHERE dataset='$line'});
	$dbh->commit; #or die $dbh->errstr (I don't think I need the OR because the connection is set to raise exceptions)
	
	### Remove old data
	if ($dataset_type eq 'rt_subway') {
		$dbh->do(qq{DELETE FROM rt_subway WHERE line='$line'});
	} else {
		$dbh->do(qq{DELETE FROM rt_commrail WHERE line='$line'});
	}
	
	### Get Real-Time Data file from web
	$content = get($url);
	
	### Prepare the insert statement
	if ($dataset_type eq 'rt_subway') {
		$sqlquery = qq{INSERT INTO rt_subway SET line=?, trip=?, PlatformKey=?, information_type=?, announcement_time=?,remaining_time=?, revenue=?, route=?};
	} else {
		$sqlquery = qq{INSERT INTO rt_commrail SET RecTimestamp=?, Trip=?, Destination=?, Stop=?, Scheduled=?, Flag=?, Vehicle=?, Latitude=?, Longitude=?, Direction=?, Speed=?, Lateness=?, line=?;};
	}
	$sth = $dbh->prepare($sqlquery)
		or die "Couldn't prepare statement: " . $dbh->errstr;
	if ($DEBUG) {print LOG "Current Prepared Statement:  $sth->{Statement} \n";} ##DEBUGGING
	
	### Open data stream and begin processing
	open RTDF, '<', \$content || die "Content variable could not be opened!\n";
	while (<RTDF>) {
		chomp;
		$fileRowCount++;
		if ($dataset_type eq 'rt_subway') {
			### Put the subway data into local variables
			($subline,$trip,$PlatformKey,$information_type,$announcement_time,$remaining_time,$revenue,$route) = split(/,/,$_);
			#Remove Whitespace
			$subline = removeWhitespace($subline);
			$trip = removeWhitespace($trip);
			$PlatformKey = removeWhitespace($PlatformKey);
			$information_type = removeWhitespace($information_type);
			$revenue = removeWhitespace($revenue);
			$route = removeWhitespace($route);
			$remaining_time = removeWhitespace($remaining_time);
			#Format announcement_time for databse
			$announcement_time = formatAnnounceTime($announcement_time);
			###Insert to Database
			$sth->execute($subline, $trip, $PlatformKey, $information_type, $announcement_time, $remaining_time, $revenue, $route);
			$insertCount++;
			if ($DEBUG>100) {print LOG "Bind Variables:  $subline, $trip, $PlatformKey, $information_type, $announcement_time, $remaining_time, $revenue, $route \n";} ##DEBUGGING
		} else {
			### Put the commuter rail data into local variables
			# Skip the header line
			next if /TimeStamp,Trip,Destination,Stop,Scheduled,Flag,Vehicle,Latitude,Longitude,Heading,Speed,Lateness/;
			($RecTimestamp,$Trip,$Destination,$Stop,$Scheduled,$Flag,$Vehicle,$Latitude,$Longitude,$Direction,$Speed,$Lateness) = split(/,/,$_);
			###Insert to Database
			$sth->execute($RecTimestamp, $Trip, $Destination, $Stop, $Scheduled, $Flag, $Vehicle, $Latitude, $Longitude, $Direction, $Speed, $Lateness, $line);
			$insertCount++;
			if ($DEBUG>100) {print LOG "Bind Variables:  $RecTimestamp, $Trip, $Destination, $Stop, $Scheduled, $Flag, $Vehicle, $Latitude, $Longitude, $Direction, $Speed, $Lateness, $line \n";} ##DEBUGGING
		}
	}
	close RTDF;
	$dbh->commit;  #SAVE all changes
} else {
	#Do not refresh, we are within the minimum period
	if ($DEBUG) {print LOG "!!!!!Did not enter refresh section. \n";} ##DEBUGGING
}
# End of IF time to refresh

###Close DB connection
$dbh->disconnect();

### Finish error log and close
if ($DEBUG) {
	print LOG "File Rows Parsed = $fileRowCount \n";
	print LOG "Insert Count = $insertCount \n";
	print LOG "Start Time = $start_time \n";
	$end_time = localtime;
	print LOG "End Time = $end_time \n";
	print LOG "End of Processing! \n";
	print LOG "------------------------------------------------------------";
	close LOG;
}

#########################  SUBROUTINES  ##################################


##########################################
###Remove leading AND trailing whitespace from a string, but preserve inner spaces
sub removeWhitespace {

	#parameters
	my $var = $_[0];

	#remove whitespace
	$var =~ s/^\s+|\s+$//g;

	return $var;
}
### end of sub removeWhitespace


##########################################
### Format the Announcement time for MySQL
sub formatAnnounceTime {

	#parameters
	my $annTime = $_[0];

	# Time/Date Variables
	my $datestr;
	my $timestr;
	my $year;
	my $month;
	my $day;
	my $hour;
	my $minute;
	my $second;
	my $halfday;
	
	$annTime = removeWhitespace($annTime);
	if ($DEBUG>200) {print LOG "formatAnnounceTime - Announce Value: $annTime \n";} ##DEBUGGING 
	#Expects Announcement time to be in this format: 6/11/2011 12:33:24 PM
	($datestr, $timestr, $halfday) = split(/\s/,$annTime);
	if ($DEBUG>200) {print LOG "formatAnnounceTime - Parsed Date, Time, Half:  $datestr, $timestr, $halfday \n";}  ##DEBUGGING 
	($month, $day, $year) = split(/\//,$datestr);
	if ($DEBUG>200) {print LOG "formatAnnounceTime - Parsed Month, Day, Year:  $month, $day, $year \n";}  ##DEBUGGING 
	($hour, $minute, $second) = split(/:/,$timestr);
	if ($DEBUG>200) {print LOG "formatAnnounceTime - Parsed Hours, Minutes, Seconds:  $hour, $minute, $second \n";}  ##DEBUGGING 
	if ($halfday eq 'AM') {
		if ($hour == 12) { $hour = '00'; }
	} else {
		if ($hour < 12) { $hour = $hour+12; }
	}
	$annTime = sprintf("%04d-%02d-%02d %02d:%02d:%02d", $year, $month, $day, $hour, $minute, $second);
	if ($DEBUG>200) {print LOG "formatAnnounceTime - Announce reformatted: $annTime \n";} ##DEBUGGING 

	return $annTime;
}
### end of sub formatAnnounceTime


#########################  END OF PROGRAM  ################################
###########################################################################






















