package Run;

use strict;
use warnings;
use Dir::Self;
use lib __DIR__;

use Aoddb;
use Atlas;
use Barcode;
use Case;
use Storable 'dclone';
use Exporter;
use File::Basename;
use Encode qw(is_utf8 encode decode decode_utf8);
use List::Util qw(max);
use Data::Dumper;
use Variable::Temp 'temp';

our @ISA	= qw/ Exporter AODDB/;
our @EXPORT	= qw/ $verbose /;
our $verbose	= 0;

sub new {
	my $class = shift;
	my $self  = {};
	$self->{barcodes} = [];
	return (bless $self, $class);
	}

sub connect_barcode {
	my $class = shift;
	my $barcode_name = shift;
	my $dbh = $class->{DB}->{mysql};
	my $run_id = $class->get_id;


	my $sql_cmd = "UPDATE Barcode SET sequencingRunId = '$run_id' WHERE barcodeName = '$barcode_name';";
	#my $sql_cmd = "INSERT INTO `SequencingHistory` (barcodeName, sequencingrunid) VALUES ('$barcode_name', '$run_id');";
	my $sth;
	if ($sth = $dbh->prepare($sql_cmd)) {} else {
		die "Couldn't prepare statement: " . $dbh->errstr;
		}
	eval {$sth->execute};
	if ($@) {
		print STDERR "$@\n" if $verbose;
		print STDERR "Could not execute sql command:\n$sql_cmd\n" if $verbose;
		return 1;
		} else {
		$class->{DB}->mysql_log($sql_cmd);
		}
	my $bcode = $class->{DB}->Barcode($barcode_name);
	my $bam_file;
	if (defined($bcode->get_bam)) {
		$bam_file = $bcode->get_folder . "/raw/" . $bcode->get_bam;
		} else {
		die "Incorrect BAM File\n" if $verbose;
		}
	$class->put_bam($bam_file);
	return 0;
	
	}

sub connect {
	my $class = shift;
	my $db = shift;
	
	$class->{'DB'} = $db;
	}

sub get_field_dic {
	my $class = shift;
	my $db = $class->{DB};
	my $dbh = $class->{DB}->{mysql};
	my $sql_cmd = "SELECT * FROM SequencingRun WHERE 1=0";
	my $sth = $dbh->prepare($sql_cmd);
	eval {$sth->execute};
	if ($@) {
		undef $@;
		print STDERR "Could not execute sql command:\n$sql_cmd <- error\n" if $verbose;
		die;
		}
	my @cols = @{$sth->{NAME_lc}};
	$sth->finish;
	return @cols;
	}

sub load_info {
	my $class = shift;
	my $info  = shift;
	my $result;
	my @cols = $class->get_field_dic;
	OUTER_LOAD_INFO: foreach my $key (keys %{$info}) {
		foreach my $arg (@cols) {
			my $dic = $arg; $dic = lc($dic); $dic =~ s/_//g;
			my $cur = $key; $cur = lc($cur); $cur =~ s/_//g;
			next OUTER_LOAD_INFO if $cur eq 'sequencingrunid';
			if ($cur eq 'barcodes') {
				foreach my $bcode_info (@{$info->{barcodes}}) {
					my $bcode = Barcode->new;
					my $DB = $class->{DB};
					$bcode->connect($DB);
					$bcode->load_info($bcode_info);
					push (@{$class->{barcodes}}, $bcode);
					}
				next OUTER_LOAD_INFO;
				}
			if ($dic eq $cur) {
				my $field = lc($arg);
				$class->{info}->{$field} = $info->{$key};
				next OUTER_LOAD_INFO;
				}
			}
		print STDERR "WARNING: field $key was not found in mysql database\n" if $verbose;
		}
	}

sub info {
	my $class = shift;
	return $class->{info};
	}

sub config {
	my $class = shift;
	return $class->{DB}->{'global_config'};
	}

sub generate_id {
	my $class = shift;
	my $dbh = $class->{DB}->{mysql};
	
	my @runs = qw(1);
	my $id = $class->info->{sequencingrunid};
	if (defined $id) {
		print STDERR "Could not generate Id. Patient is already assigned with id : '$id'" if $verbose;
		return 1;
		}
	my $sql_cmd = "select sequencingrunid from SequencingRun;";
	my $sth;
	$sth = $dbh->prepare($sql_cmd) or return 1;
	eval {$sth->execute};
	if ($@) {
		print STDERR "$@" if $verbose;
		print STDERR "Could not execute sql command:\n$sql_cmd <- error\n" if $verbose;
		die;
		} else {
		while (my $row = $sth->fetchrow_arrayref) {
			if ($$row[0] =~ /^(\d+)$/) {
				push (@runs, $$row[0]);
				}
			}
		}
	return (max(@runs) + 1);	
	}

sub assign_id {
	my $class = shift;
	my $run_id = shift;
	$class->{info}->{sequencingrunid} = $run_id;
	}
	
sub get_info {
	my $class = shift;
	my $info = $class->info;
	my $config = $class->config;
	
	die "Undefined Database config\n" unless defined $info;
	die "Undefined run info\n" unless defined $config;
	
	return ($info, $config);
	}
	
sub is_folder_exist {
	my $class = shift;
	my ($info, $config) = $class->get_info;
	my $run_id = $class->get_id;
	
	my @dirs = (
		"$config->{data_path}->{runDumpPath}/$run_id"
		);
	
	foreach my $arg (@dirs) {
		if (opendir(my $dir, "$arg")) {
			closedir $dir;
			} else {
			return 0;
			}
		}
	
	return 1;
	}
	
sub check_folder_structure {
	my $class = shift;
	my ($info, $config) = $class->get_info;
	my $run_id = $class->get_id;
	
	return 1 if ($class->is_folder_exist eq 0);
	
	my @dirs = (
		"$config->{data_path}->{runDumpPath}/$run_id",
		"$config->{data_path}->{runDumpPath}/$run_id/BAM"
		);
	map {my $destiny = $_;
		if (opendir(my $dir, "$destiny")) {
			closedir $dir;
			} else {
			return 1;
			}
		} @dirs;
	
	return 0;
	}
	
sub get_id {
	my $class = shift;
	my $run_id = $class->info->{sequencingrunid};
	die "Sequencing Run Id is not defined\n" unless defined $run_id;
	return $run_id;
	}
	
sub create_data_dir {
	my $class = shift;
	my ($info, $config) = $class->get_info;
	my $run_id = $class->get_id;
	
	my %log;
	my @dirs = (
		"$config->{data_path}->{runDumpPath}/$run_id",
		"$config->{data_path}->{runDumpPath}/$run_id/BAM"
		);
	map {my $dir = $_;
		$log{$dir} = `mkdir -v $dir 2>&1`;
		chomp $log{$dir};
		print STDERR "$log{$dir}\n" if $verbose;
		} @dirs;
	
	my $f = 0;
	if ($class->check_folder_structure) {
		print STDERR "Could not correctly create run folder\n" if $verbose;
		for (my $i = scalar @dirs - 1; $i >= 0; $i--) {
			my $log_i = $dirs[$i];
			if ($log{$log_i} eq "mkdir: created directory '$log_i'") {
				print STDERR "Removing created fodler: $log_i\n" if $verbose;
				`rm -r $log_i`
				}
			};
		return 1;
		}
	
	return 0;
	}
	
sub folder_remove {
	my $class = shift;
	my ($info, $config) = $class->get_info;
	my $run_id = $class->get_id;
	
	`rm -r $config->{data_path}->{runDumpPath}/$run_id 2>&1`;
	if ($class->is_folder_exist eq 0) {
		print STDERR "Sequencing dump folder sucessfully removed (run_id : $run_id)\n" if $verbose eq 1;
		return 1;
		}
	return 0;	
	}
	
sub check_files {
	my $class = shift;
	my ($info, $config) = $class->get_info;
	
	
	foreach my $barcode ($class->barcodes) {
		my $file_name = "" . $barcode->meta->{librarypath} . "/" . $barcode->meta->{libraryname};
		if (open (my $fh, "<$file_name")) {
			close $fh;
			if (Atlas::check_bam_file($config, $file_name) eq 1) {
				print STDERR "Corrupted BAM file $file_name\n" if $verbose;
				return 1;
				}
			} else {
			print STDERR "File $file_name not found\n" if $verbose;
			return 1;
			}
		};
	
	return 0;
	}
	
sub dump_json {
	my $class = shift;
	my ($info, $config) = $class->get_info;
	my $run_id = $class->get_id;
	
	my @files = ("$config->{data_path}->{runDumpPath}/$run_id/run_info.json"
		);
	foreach my $arg (@files) {
		eval {Atlas::json_to_file($info, $arg)};
		if ($@) {print STDERR "$@\n";return 1;}
		}
	
	foreach my $arg ($class->barcodes) {
		my $barcode_name = $arg->get_id;
		eval {Atlas::json_to_file({"info" => $arg->info, "meta" => $arg->meta}, "$config->{data_path}->{runDumpPath}/$run_id/$barcode_name.json")}
		}
	return 0;
	}

sub put_bam {
	my $class = shift;
	my $bam_file =  shift;
	my ($info, $config) = $class->get_info;
	my $run_id = $class->get_id;
	
	my $destination = "$config->{data_path}->{runDumpPath}/$run_id/BAM/";
	my $log = `cp $bam_file $destination 2>&1`; chomp $log;
	}
	
sub upload_data {
	my $class = shift;
	my ($info, $config) = $class->get_info;
	my $run_id = $class->get_id;
	
	my @copied;
	if (scalar ($class->barcodes) eq 0) {
		print STDERR "Warn : No data found in run info\n" if $verbose;
		return 0;
		}
	
	foreach my $bcode ($class->barcodes) {
		my $file = $bcode->meta->{librarypath} . "/" . $bcode->meta->{libraryname};
		my $destination = "$config->{data_path}->{runDumpPath}/$run_id/BAM/";
		$destination = $destination . basename($file);
		my $log = `cp $file $destination 2>&1`; chomp $log;
		
		if (open(my $fh, "<", $destination)) {
			close $fh;
			push(@copied, $destination);
			$bcode->meta->{librarypath} = "$config->{data_path}->{runDumpPath}/$run_id/BAM/";
			} else {
			print STDERR "Could not copy file $file to $destination\n" if $verbose;
			foreach my $arg (@copied) {`rm $arg`;}
			return 1;
			}
		}
	
	return 0;
	}
	
sub barcodes {
	my $class = shift;
	return @{$class->{barcodes}};
	}
	
#Внести данные по запуску в базу данных.
sub insert {
	my $class	= shift;
	my $dbh	= $class->{DB}->{mysql};
	my ($info, $config) = $class->get_info;
	my $run_id = $class->get_id;
	
	$run_id = "'$run_id'";
	my $organizationId = Atlas::prepare_str_for_insert($info->{organizationid});;
	my $sequencingRunPI = Atlas::prepare_str_for_insert($info->{primaryinvestigator});
	my $sequencingRunDate = 'NULL';

	if (defined($info->{sequencingrundate})) {
		my $date = $info->{sequencingrundate};
		if (Atlas::check_date_format($date)) {
			print STDERR "Can not parse date '$date'. Should follow YYYY-MM-DD format\n" if $verbose;
			return 1;
			}
		$sequencingRunDate = "'$date'";
		}
	
	my $sql_cmd = "INSERT INTO `SequencingRun` (sequencingrunid, sequencingRunDate, organizationId, sequencingRunPI) VALUES ($run_id, $sequencingRunDate, $organizationId, $sequencingRunPI);";
	my $sth;
	$sth = $dbh->prepare($sql_cmd) or return 1;
	eval {$sth->execute};
	if ($@) {
		print STDERR "$@" if $verbose;
		print STDERR "Could not execute sql command:\n$sql_cmd <- error\n" if $verbose;
		die;
		} else {
		$class->{DB}->mysql_log($sql_cmd);
		}
	return 0;
	}
	
# Удалить запуск: удаляется папка из Дампа, данные из таблиц SequencingRun и SequencingHistory
sub delete {
	my $class = shift;
	my $dbh = $class->{DB}->{mysql};
	my $id = $class->get_id;
	my $error = 0;
	#my $sql_cmd = "delete from SequencingHistory where sequencingrunid = '$id';";
	#my $sth;
	#$sth = $dbh->prepare($sql_cmd);
	#eval {$sth->execute};
	#if (($@)) {
	#	undef $@;
	#	$error = 1;
	#	print STDERR "Could not remove sequencing history:\n$sql_cmd <- error\n" if $verbose;
	#	} else {
	#	$class->{DB}->mysql_log($sql_cmd);
	#	print STDERR "Sequencing data history for run $id removed from database\n" if $verbose;
	#	}
	my $sql_cmd = "delete from SequencingRun where sequencingrunid = '$id';";
	my $sth = $dbh->prepare($sql_cmd);
	eval {$sth->execute};
	if (($@)) {
		print STDERR "Could not delete sequencing run info from database:\n$sql_cmd <- error\n" if $verbose;
		print STDERR "$@\n" if $verbose;
		$error = 1;
		} else {
		print STDERR "Info on sequencing run $id removed from database\n" if $verbose;
		$class->{DB}->mysql_log($sql_cmd);
		}
	if ($class->folder_remove) {
		if ($class->is_folder_exist) {
			print STDERR "Could not remove sequencing data folder\n" if $verbose;
			$error = 1;
			}
		} else {
		print STDERR "Data folder successfully removed\n" if $verbose;
		}
	return $error;
	}
		
sub create {
	my $class = shift;
	my ($info, $config) = $class->get_info;
	my $run_id = $class->get_id;
	
	if ($class->check_files eq 1) {
		print STDERR "Can not create run: barcode files are corrupted or does not exists\n" if $verbose;
		return 1;
		}
	
	die "Run folder already exists" if ($class->is_folder_exist);
	{
		temp $verbose = 0;
		die "Run already exists in database" if (defined(my $run = Run->fetch($class->{DB}, $run_id)));
	}
	die "Can not create run dir" if ($class->create_data_dir);
	
	my $info_dump = dclone $info;
	if (($class->upload_data eq 0) and
		($class->check_files eq 0)) {
		print STDERR "run Directory succesfully created and data uploaded\n" if $verbose;
		} else {
		print STDERR "Failed to upload sequencing Data\n" if $verbose;
		$class->{'info'} = $info_dump;
		$class->folder_remove;
		return 1;
		}
	
	if ($class->insert eq 0) {
		print STDERR "Sequencing Run Data inserted into mySQL database\n" if $verbose;
		} else {
		print STDERR "Failed to insert into database\n" if $verbose;
		$class->{'info'} = $info_dump;$class->folder_remove;
		return 1;
		}
	my @copied;
	foreach my $arg ($class->barcodes) {
		print STDERR "Creating new barcode:\n" if $verbose;
		my $id = $arg->generate_id;
		$arg->assign_id($id);
		my $bcode_name = $arg->get_id;
		print STDERR "Associated barcode name : $bcode_name\n" if $verbose;
		if ($arg->upload_data) {
			print STDERR "Could not upload data from barcode $id\n";
			$class->{'info'} = $info_dump;$class->delete;
			foreach my $arg_g (@copied) {$arg_g->delete}
			return 1;
			};
		if ($arg->insert) {
			print STDERR "Could not insert info into database for barcode $id\n";
			$class->{'info'} = $info_dump;$class->delete;
			$arg->delete;
			foreach my $arg_g (@copied) {$arg_g->delete}
			return 1;
			}
		if ($class->connect_barcode($arg->get_id)) {
			print STDERR "Could not associate barcode $id with sequencing Run $run_id\n";
			$class->{'info'} = $info_dump;$class->delete;
			$arg->delete;
			foreach my $arg_g (@copied) {$arg_g->delete}
			return 1;
			}
		push (@copied, $arg);
		}
        if ($class->dump_json) {
		print STDERR "Could not dump run info into json file\n" if $verbose;
		$class->{'info'} = $info_dump;
		$class->delete;
		foreach my $arg_g (@copied) {$arg_g->delete}
		return 1;
		} else {
		print STDERR "Run info dumped into json file\n" if $verbose;
		}	
		
	return 0;
	}

sub fetch_barcodes {
	my $class = shift;
	my $id = $class->get_id;
	my $dbh = $class->{DB}->{mysql};
	my @barcodes;
	my $sql_cmd = "select barcodeName from Barcode where sequencingRunId = '$id'";
	my $sth;
	$sth = $dbh->prepare($sql_cmd) or return 1;
	return 1 unless defined $sth;
	my $counter = 0;
	eval {$sth->execute};
	if ($@) {
		print STDERR "$@" if $verbose;
		print STDERR "Could not execute sql command:\n$sql_cmd <- error\n" if $verbose;
		die;
		} else {
		while (my $row = $sth->fetchrow_arrayref) {
			++$counter;
			my $bcode = Barcode->fetch($class->{DB}, $$row[0]);
			unless(defined($bcode)) {
				print STDERR "Barcode $$row[0] was not found in database\n" if $verbose;
				return 1;
				}
			push @barcodes, $bcode;
			}
		}
	$class->{barcodes} = \@barcodes;
	}

sub fetch_info {
	my $class = shift;
	my $id = shift;
	my $dbh = $class->{DB}->{mysql};
	unless (defined($id)) {
		$id = $class->get_id;
		}
	my %info;
	my @fields = $class->get_field_dic;
	my $sql_cmd = "select " . join(", ", @fields) . " from SequencingRun where sequencingrunid = '$id'";
	my $sth;
	$sth = $dbh->prepare($sql_cmd) or return 1;
	my $counter = 0;
	eval {$sth->execute};
	if ($@) {
		print STDERR "$@" if $verbose;
		print STDERR "Could not execute sql command:\n$sql_cmd <- error\n" if $verbose;
		die;
		} else {
		while (my $row = $sth->fetchrow_arrayref) {
			++$counter;
			for (my $i = 0; $i < scalar @fields; $i++) {
				$info{$fields[$i]} = encode('utf8', $$row[$i])
				}
			}
		}
	unless ($counter eq 1) {
		print STDERR "Return $counter rows from database specified with the sequencing Run Id '$id'\n" if $verbose;
		return 1;
		}
	$class->{info} = \%info;
	
	}


sub fetch {
	my $class = shift;
	my $DB = shift;
	my $id = shift;

	my $self = $class->new;
	$self->connect($DB);
	$self->fetch_info($id);
	if (defined($self->{info})) {
		$self->fetch_barcodes;
		return $self;
		} else {
		return undef;
		}
	}












1;
