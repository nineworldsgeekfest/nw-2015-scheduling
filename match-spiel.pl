#!/usr/bin/env perl

use DateTime;
use DateTime::Format::Strptime;

our $localTZ = DateTime::TimeZone->new(name => 'local');

my $spiels = read_spiels();
my $schedule = process_schedule();
do_incremental_matching($spiels, $schedule);

sub quoted_csv_split {
    my @lines = @_;

    my @splitLines = ();

    foreach my $thisLine (@lines) {
        my $lineLen = length($thisLine);
        my $state = 0; my $last = 0; my @buffer;
        for my $lineIndex (0..$lineLen) {
            my $thisChar = substr($thisLine, $lineIndex, 1);
            if ((($thisChar eq ',') || ($lineIndex == $lineLen)) && ($state == 0)) {
                my $elem = substr($thisLine, $last, $lineIndex - $last);
                $elem =~ s/"//g;
                chomp $elem;
                push @buffer, $elem;
                $last = $lineIndex + 1;
            } elsif (($thisChar eq '"') && ($state == 0)) {
                $state = 1;
            } elsif (($thisChar eq '"') && ($state == 1)) {
                $state = 0;
            }
        }
        push @splitLines, [@buffer];
    }

    return @splitLines;
}

sub read_spiels {
    open(IN, "<", "spiels.csv");
    my @lines = <IN>;
    chomp @lines;
    close(IN);
    
    my $spiels = {};

    my @splitLines = quoted_csv_split(@lines);

    foreach my $line (@splitLines) {
        my $key = lc($line->[0]);
        $key =~ s/[^a-z0-9]//g;
        if ($key eq '') {
            print "STOP - key for '$line->[0]' ($line->[1]) is blank";
            die;
        }
        $spiels->{$key} = {matched => 0, spiel => $line->[1]};
    }

    return $spiels;
}

sub process_schedule {
    open(IN, "<", "schedule.csv");
    my @lines = <IN>;
    close(IN);
    chomp @lines;

    my $headerLine = shift @lines;
    my @headers = split(/,/, $headerLine);

    my @records;

    my @splitLines = quoted_csv_split(@lines);

    foreach my $line (@splitLines) {
        my $record = {};
        $record->{'Tracks'} = ref_to_no_blanks(@{$line}[0..1]);
        @{$record}{@headers[2..11]} = @{$line}[2..11];
        $record->{'Room'} = $line->[12];
        $record->{'Rooms'} = handle_room($line->[12]);
        $record->{'Guests'} = ref_to_no_blanks(@{$line}[13..$#$line]);
        $record->{'StartObj'} = dt_to_obj($record->{'StartDT'});
        $record->{'EndObj'} = dt_to_obj($record->{'EndDT'});

        push @records, $record;
    }
    return \@records;
}

sub ref_to_no_blanks {
    return [grep {$_ ne ''} @_];
}

sub handle_room {
    my $room = shift;

    if ($room =~ /&/) {
        my ($main, $extra) = split(/&/, $room);
        my $otherRoom = substr($main, 0, length($main) - 1) . $extra;
        return [$main, $otherRoom];
    }

    return [$room];
}

sub dt_to_obj {
    my $dt = shift;
    my ($day, $time) = split(/ /, $dt);
    my $date;
    if ($day eq 'Thu') {
        $date = 7;
    } elsif ($day eq 'Fri') {
        $date = 8;
    } elsif ($day eq 'Sat') {
        $date = 9;
    } else {
        $date = 10;
    }
    my ($hr, $min) = split(/:/, $time);

    return DateTime->new(
        year => 2014,
        month => 8,
        day => $date,
        hour => $hr,
        minute => $min,
        second => 0,
        time_zone => $localTZ);
}

sub do_incremental_matching {
    my ($spiels, $schedule) = @_;
    my @matches; my @noMatchSched;
    my $sCount = 0;
SESSION:
    foreach my $s (@$schedule) {
        $sCount++;
        $s->{'ID'} = $sCount;
        my $matchEvent = lc($s->{'Event'});
        $matchEvent =~ s/[^a-z0-9]//g;
SPIEL:
        foreach my $key (keys %$spiels) {
            if (($matchEvent =~ m/^$key/) || ($key =~ m/^$matchEvent/)) {
                print "*** Matched $key with $matchEvent!\n";
                $s->{'Spiel'} = $spiels->{$key}{'spiel'};
                $spiels->{$key}{'matched'}++;
                next SESSION;
            }
        }
        print "--- No match for $matchEvent\n";
        push @noMatchSched, $s;
    }
    foreach my $key (keys %$spiels) {
        next if ($spiels->{$key}{'matched'} > 0);
        print "/// No match for spiel with key $key\n";
    }
}
