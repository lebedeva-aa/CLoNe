package CommandLine;
use Moose;

use strict;
#use warnings;
use Dir::Self;
use lib __DIR__;
use Fcntl qw(:flock SEEK_END);
use Command;
#use Mojo::Base -base;

use Aoddb;

#our @ISA = qw(Exporter);
our @EXPORT     = qw/ $verbose /;
our $verbose    = 0;

has 'err', is => 'rw', isa => 'Str', default => '';
has 'response', is => 'rw', isa => 'Str', default => '';


sub config {
	my $class = shift;
	return $class->{DB}->{'global_config'};
	}

sub connect {
	my $class = shift;
	my $DB = shift;
	
	$class->{DB} = $DB;
	}

sub str_to_command {
	my $class = shift;
	my $str = shift;
	
	my $command = Command->new();
	$command->connect($class);
	$command->{cmd} = $str;
	
	return $command;
	}

sub shout {
	my $class = shift;
	
	if (defined($class->response)) {
		print STDERR $class->response."\n";
		}
	if ($class->err eq 1) {
		$class->{DB}->Claudia_say_debug($class->response);
		}
	}

sub length {
	my $class = shift;
	
	my $file_name = $class->config->{data_path}->{command_stack};
	if (open my $fh, '+<', $file_name) {
		flock($fh, LOCK_EX) or die "Cannot lock mailbox - $!\n";
		my @lines = <$fh>;
		seek $fh, 0, 0;
		truncate $fh, 0;
		print $fh join("", @lines[0..(scalar @lines - 1)]);
		close $fh;
		return (scalar @lines);
		} else {die "Can not open command stack file"}
	}

sub push {
        my $class = shift;
        my $cmd = shift;
	chomp $cmd;
	
	my $file_name = $class->config->{data_path}->{command_stack};
	if (open my $fh, '+<', $file_name) {
		flock($fh, LOCK_EX) or die "Cannot lock mailbox - $!\n";
		my @lines = <$fh>;
		seek $fh, 0, 0;
		truncate $fh, 0;
		print $fh join("", @lines[0..(scalar @lines - 1)]);
		print $fh "$cmd\n";
		close $fh;
		} else {die "Can not open command stack file"}
	return $class->length;
	}

sub unshift {
	my $class = shift;
	my $cmd = shift;
	chomp $cmd;
	
	my $file_name = $class->config->{data_path}->{command_stack};
	if (open my $fh, '+<', $file_name) {
		flock($fh, LOCK_EX) or die "Cannot lock mailbox - $!\n";
		my @lines = <$fh>;
		seek $fh, 0, 0;
		truncate $fh, 0;
		print $fh "$cmd\n";
		print $fh join("", @lines[0..(scalar @lines - 1)]);
		close $fh;
		} else {die "Can not open command stack file"}
	return $class->length;
	}

sub pop {
	my $class = shift;
	
	my $file_name = $class->config->{data_path}->{command_stack};
	if (open my $fh, '+<', $file_name) {
		flock($fh, LOCK_EX) or die "Cannot lock mailbox - $!\n";
		my @lines = <$fh>;
		seek $fh, 0, 0;
		truncate $fh, 0;
		print $fh join("", @lines[0..(scalar @lines - 2)]);
		close $fh;
		return undef unless defined $lines[(scalar @lines - 1)];
		chomp $lines[(scalar @lines - 1)];
		return $class->str_to_command($lines[(scalar @lines - 1)]);
		} else {die "Can not open command stack file"}
	}

sub shift {
	my $class = shift;
	
	my $file_name = $class->config->{data_path}->{command_stack};
	if (open my $fh, '+<', $file_name) {
		flock($fh, LOCK_EX) or die "Cannot lock mailbox - $!\n";
		my @lines = <$fh>;
		seek $fh, 0, 0;
		truncate $fh, 0;
		print $fh join("", @lines[1..(scalar @lines - 1)]);
		close $fh;
		return undef unless defined $lines[0];
		chomp $lines[0];
		return $class->str_to_command($lines[0]);
		} else {die "Can not open command stack file"}
	}

















1;
