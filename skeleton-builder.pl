#!/usr/local/bin/perl

# skeleton-builder - build a schedule.csv with skeleton timing information

use strict;
use warnings;

use Util::DB;

my %tracks = ('Academia' => {rooms => [qw(County-B County-C&D)]},
              'All of the Books' => {rooms => [qw(Commonwealth-West 38 30 Royal-C&D 16)]},
              'Anime' => {rooms => [qw(Royal-B Commonwealth-East)]},
              'Apocalypse' => {rooms => [qw(County-A)]},
              'A Song of Ice and Fire' => {rooms => [qw(38)]},
              'Comics' => {rooms => [qw(11 Connaught-A 31 County-C&D)]},
              'Doctor Who' => {rooms => [qw(Royal-C&D 31 County-C&D)]},
              'Entertainment' => {rooms => [qw(Commonwealth-West County County-C&D)]},
              'Fanfic' => {rooms => [qw(12 31i County-C&D)]},
              'Film Festival' => {rooms => [qw(41)]},
              'Food Geekery' => {rooms => [qw(32 Connaught)]},
              'Future Tech' => {rooms => [qw(11 31 County-C&D)]},
              'Geek Feminism' => {rooms => [qw(Connaught-A County-C&D)]},
              'History' => {rooms => [qw(County-B County-C&D)]},
              'Kids' => {rooms => [qw(40)]},
              'LARP' => {rooms => [qw(40)]},
              'LGBTQAI+ Fandom' => {rooms => [qw(Connaught-B Royal-B)]},
              'Podcasting' => {rooms => [qw(32 30)]},
              'Race and Culture' => {rooms => [qw(Connaught-B)]},
              'Religion' => {rooms => [qw(32)]},
              'Roleplay (Storygasm)' => {rooms => [qw(Commonwealth-East)]},
              'Skepticism' => {rooms => [qw(Royal-B)]},
              'Social Gaming' => {rooms => [qw(Royal-C&D County-C&D)]},
              'Star Trek' => {rooms => [qw(Royal-B)]},
              'Supernatural' => {rooms => [qw(County-A 40)]},
              'Tolkien' => {rooms => [qw(Connaught-B)]},
              'Video Games Culture' => {rooms => [qw(11)]},
              'Water Dancing' => {rooms => [qw(Newbury-1 38)]},
              'Whedon' => {rooms => [qw(Royal-C&D 38 31)]},
              'Creative Writing' => {rooms => [qw(Royal-A 31)]},
              'Yarn' => {rooms => [qw(40)]},
              'Young Adult' => {rooms => [qw(38 Connaught-B Royal-B)]});

my @rooms = qw(Commonwealth-West
               Commonwealth-East
               County-A
               County-B
               County-C&D
               County
               11
               12
               16
               30
               31
               32
               38
               Royal-A
               Royal-B
               Royal-C&D
               Connaught-A
               Connaught-B
               Connaught
               40
               41
               Newbury-1);

my %days = ('Fri' => '2015-08-07', 'Sat' => '2015-08-08', 'Sun' => '2015-08-09');
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

my $dbh = Util::DB::getDatabaseConnection();
my ($trackMapping, $error) = Util::DB::dbSelect($dbh, 'name', '*', ['track'], '1=1', []);

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
                    #my $entry = [$track,'',$eventName,$eventName,'Unknown','',
                    #             $date,$session->{'start'},$dayName . ' ' . $session->{'start'},
                    #             $date,$session->{'end'},$dayName . ' ' . $session->{'end'},
                    #             $room,'','','','','','','','',''];
                    my $entry = {start_date => $date, title => $eventName, type => 'Unknown', description => '', loc => $room, mins => 75, start_time => $session->{'start'}};
                    my ($iResult, $iError) = Util::DB::dbInsert($dbh, 'program', [qw(start_date start_time title type description loc mins)], $entry);
                    if (!$iResult) {
                        print "--- something wrong in program insert! $iError\n";
                    } else {
                        my $lastId = $dbh->selectrow_array("SELECT LAST_INSERT_ID()");
                        my $trackId = $trackMapping->{$track}{id};
                        my ($tResult, $tError) = Util::DB::dbInsert($dbh, 'program_track', [qw(program_id track_id)], {program_id => $lastId, track_id => $trackId});
                        if (!$tResult) {
                            print "--- something wrong in program_track insert! $tError\n";
                        }
                        push @schedule, $entry;
                        print "+++ Scheduled\n";
                    }
                }
            } else {
                print "--- Not scheduled\n";
            }
        }
    }
}

Util::DB::dropDatabaseConnection($dbh);

#my $header = 'Track,Track,EventShort,Event,EventClass,Flags,StartDay,StartTime,StartDT,EndDay,EndTime,EndDT,Room,Guests,,,,,,,,';

#open(OUT, ">", "skeleton.schedule.csv");
#print OUT $header . "\n";

#foreach my $scheduleItem (@schedule) {
#    print OUT join(',', @$scheduleItem) . "\n";
#}
#close(OUT);
