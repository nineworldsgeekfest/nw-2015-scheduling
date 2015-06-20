#!/usr/local/bin/perl

# skeleton-builder - build a schedule.csv with skeleton timing information

use strict;
use warnings;

my %tracks = ('Academia' => {rooms => [qw(County-B County-C&D)]},
              'All of the Books' => {rooms => [qw(Commonwealth-West 38 30 Royal-C&D)]},
              'Anime' => {rooms => [qw(Royal-B Commonwealth-East)]},
              'Apocalypse' => {rooms => [qw(County-A)]},
              'A Song of Ice and Fire' => {rooms => [qw(38)]},
              'Comics' => {rooms => [qw(11 Connaught-A)]},
              'Doctor Who' => {rooms => [qw(Royal-C&D 31 County-C&D)]},
              'Entertainment' => {rooms => [qw(Commonwealth-West County County-C&D)]},
              'Fanfic' => {rooms => [qw(12 31)]},
              'Film Festival' => {rooms => [qw(41)]},
              'Food Geekery' => {rooms => [qw(32)]},
              'Future Tech' => {rooms => [qw(11 31)]},
              'Geek Feminism' => {rooms => [qw(Connaught-A)]},
              'History' => {rooms => [qw(County-B County-C&D)]},
              'Kids' => {rooms => [qw(40)]},
              'LARP' => {rooms => [qw(40)]},
              'LGBTQAI+ Fandom' => {rooms => [qw(Connaught-B)]},
              'Podcasting' => {rooms => [qw(32 30)]},
              'Race & Culture' => {rooms => [qw(Connaught-B)]},
              'Religion' => {rooms => [qw(32)]},
              'Roleplay (Storygasm)' => {rooms => [qw(Commonwealth-East)]},
              'Skepticism' => {rooms => [qw(Royal-B)]},
              'Social Gaming' => {rooms => [qw(Royal-C&D County-C&D)]},
              'Star Trek' => {rooms => [qw(Royal-B)]},
              'Supernatural' => {rooms => [qw(County-A)]},
              'Tolkien' => {rooms => [qw(Connaught-B)]},
              'Vendors' => {rooms => [qw(Newbury-1)]},
              'Video Games Culture' => {rooms => [qw(11)]},
              'Water Dancing' => {rooms => [qw(Newbury-1 38)]},
              'Whedon' => {rooms => [qw(Royal-C&D 38)]},
              'Writing' => {rooms => [qw(Royal-A 31)]},
              'Yarn' => {rooms => [qw(40)]});

my @rooms = qw(Commonwealth-West
               Commonwealth-East
               County-A
               County-B
               County-C&D
               County
               11
               12
               30
               31
               32
               38
               Royal-A
               Royal-B
               Royal-C&D
               Connaught-A
               Connaught-B
               40
               41
               Newbury-1);

my %days = ('Fri' => '07/08/2015', 'Sat' => '08/08/2015', 'Sun' => '09/08/2015');
my %sessions = (0 => {start => '09:00', end => '09:45'},
                1 => {start => '10:00', end => '11:15'},
                2 => {start => '11:45', end => '13:00'},
                3 => {start => '13:30', end => '14:45'},
                4 => {start => '15:15', end => '16:30'},
                5 => {start => '17:00', end => '18:15'},
                6 => {start => '18:45', end => '20:00'},
                7 => {start => '20:30', end => '21:45'},
                8 => {start => '22:15', end => '23:30'});

foreach my $track (keys %tracks) {
    $tracks{$track}->{'counter'} = 1;
}

my @schedule;

my $globalCounter = 1;

foreach my $dayName (sort keys %days) {
    print "* This is $dayName\n";
    my $date = $days{$dayName};
    foreach my $room (@rooms) {
        print "** Scheduling for room $room\n";
        my @candidateTracks;
        foreach my $track (sort keys %tracks) {
            if (grep {$room eq $_} @{$tracks{$track}->{'rooms'}}) {
                push @candidateTracks, $track;
            }
        }
        foreach my $sessionKey (sort keys %sessions) {
            my $session = $sessions{$sessionKey};
REDO:
            print "*** Schedule session $dayName [$sessionKey] (" . $session->{'start'} . " - " . $session->{'end'} . ")? ";
            my $choice = <>;
            chomp $choice;
            if (lc($choice) eq 'y') {
                my $ctr = 0;
                foreach my $cTrack (@candidateTracks) {
                    print "  $ctr - $cTrack\n";
                    $ctr++;
                }
                print "Choose track: ";
                $choice = <>;
                chomp $choice;
                if (($choice !~ /^\d+$/) || ($choice >= $ctr)) {
                    print "Bad input.\n";
                    goto REDO;
                } else {
                    my $track = $candidateTracks[$choice];
                    my $eventName = '(' . $globalCounter . ') - ' . $track . ' - Session ' . $tracks{$track}->{'counter'};
                    $globalCounter++;
                    $tracks{$track}->{'counter'}++;
                    my $entry = [$track,'',$eventName,$eventName,'Unknown','',
                                 $date,$session->{'start'},$dayName . ' ' . $session->{'start'},
                                 $date,$session->{'end'},$dayName . ' ' . $session->{'end'},
                                 $room,'','','','','','','','',''];
                    push @schedule, $entry;
                    print "+++ Scheduled\n";
                }
            } else {
                print "--- Not scheduled\n";
            }
        }
    }
}

my $header = 'Track,Track,EventShort,Event,EventClass,Flags,StartDay,StartTime,StartDT,EndDay,EndTime,EndDT,Room,Guests,,,,,,,,';

open(OUT, ">", "skeleton.schedule.csv");
print OUT $header . "\n";

foreach my $scheduleItem (@schedule) {
    print OUT join(',', @$scheduleItem) . "\n";
}
close(OUT);
