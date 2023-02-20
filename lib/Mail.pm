# /usr/bin/perl

package AtlasMail;

use strict;
use warnings;
use Dir::Self;
use lib __DIR__;

use Net::IMAP::Simple;
use Email::Simple;
use IO::Socket::SSL;
use HTML::Strip;
use Email::Send;
use Email::Send::Gmail;
use Email::Simple::Creator;
use Data::Dumper;
use Atlas;
use Aoddb;
use Encode qw(is_utf8 encode decode decode_utf8);

our @ISA	= qw/ Exporter /;
our @EXPORT	= qw/ $alert /;
our $alert	= 1;



sub new {
	my $class = shift;
	my $self = {};
	return (bless $self, $class);
	}

sub connect {
	my $class = shift;
	my $db = shift;

	$class->{'DB'} = $db;
	$class->{'DB'}->mail_connect;
	return 0;
	}

sub imap {
	my $class = shift;
	return $class->{DB}->{imap};
	}
	
sub pop_unread {
	my $class = shift;
	my $nm = $class->imap->select('INBOX');
	my $es;
	for ( my $i = 1000 ; $i <= $nm ; $i++ ) {
		print STDERR "$i\t$nm\t",$class->imap->seen($i),"\n";
		if ( $class->imap->seen($i) ) {
			next;
			}
		$es = Email::Simple->new( join '', @{ $class->imap->get($i) } );
		print STDERR "",$es->header('Subject'),"\n";
		print STDERR "",$class->{DB}->config->{mail}->{"subject"},"\n";
		if ($es->header('Subject') eq $class->{DB}->config->{mail}->{"subject"}) { 
			#Читаем тему сообщения, если она ненужная, то ищем дальше
			} else {
			$class->imap->unsee($i);
			next;
			}
		print STDERR "HEREimap\n";
		$class->imap->see($i);
		return $i;
		last;
		}
	return undef;
#	return $es;
	}

sub remove_endline { # from html file remove '\n' symbols and repplace them with <br>
	my $file = shift;
	my $seed = int(rand(10000000000000));
	open (READ, "<$file");
	open (WRITE, ">$file.$seed");
	
	my $line;
	while (<READ>) {
		print WRITE "$line<br>" if defined($line);
		chomp;
		$line = $_;
		}
	print WRITE "$line" if defined($line);
	close WRITE;
	close READ;
	`mv $file.$seed $file`;
	}

sub parse_line {
	my $line = shift;
	chomp $line;
	my $field;my $info;
	$line =~ s/\302\240/ /g;
	if ($line =~ /^[\s\t"',]*([\w\d_]+)[\s\t"',]*:[\s\t"',]*([^",']*)[\t\s"',]*$/) {
		$field = $1;
		$info  = $2;
		}
	$info = encode('utf8', lc(decode('utf8', $info))) if defined($info);
	$field = encode('utf8', lc(decode('utf8', $field))) if defined($field);
	return ($field, $info);
	}

sub parse_body {
	my $msg = shift;
	my @mas = split/\n/, $msg;
	my $data;
	$data->{info} = {};
	$data->{command} = '';
	OUTER: for (my $line = 0; $line < scalar @mas; $line++) {
		$mas[$line] =~ s/\302\240/ /g;
		if ($mas[$line] =~ /#aod(.*)$/) {
			$data->{command} = $1;
			$data->{command} =~ s/^\s+|\s+$//g;
			$data->{command} = lc($data->{command});
			INNER: for (my $line_i = $line + 1; $line_i < scalar @mas; $line_i++) {
				last OUTER if $mas[$line_i] =~ /#aod/;
#				my ($field, $info) = parse_line(encode('utf8', Atlas::decode_email($mas[$line_i])));
				my ($field, $info) = parse_line($mas[$line_i]);
#				print STDERR "$field\t$info\n";
				if ((defined($field))and(defined($info))) {
					if (uc($info) eq 'NULL') {
						undef $info;
						}
					$data->{info}->{$field} = $info;
					}
				}
			}
		}
	return $data;	
	}

sub from {
	my $class = shift;
	return $class->header('From');
	}

sub subject {
	my $class = shift;
	return $class->subject('Subject');
	}

sub send_warn {
	my $class = shift;
	my $info  = shift;
	my $email = Email::Simple->create(
		header => [
			From	=> $class->{DB}->config->{mail}->{user},
			To	=> $info->{target},
			Subject => $info->{subject},
			],
		body => $info->{message}
		);
	my $sender = Email::Send->new(
		{	mailer      => $class->{DB}->config->{mail}->{mailer},
			mailer_args => [
				username => $class->{DB}->config->{mail}->{user},
				password => $class->{DB}->config->{mail}->{pass},
			]
		});
	eval { $sender->send($email) };
	die "Error sending email: $@" if $@;
	
	}





1;
