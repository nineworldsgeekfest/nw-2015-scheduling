#!/usr/local/bin/perl

use DateTime;
use DateTime::Format::Strptime;
use Mojo::JSON qw(encode_json);
use Util::DB;

our $localTZ = DateTime::TimeZone->new(name => 'local');
our $DEBUG = 0;
our $USESHORT = 0;

if ($ARGV[0] eq '--short') {
    $USESHORT = 1;
}

binmode(STDOUT, ":utf8"); 

my $schedule = process_db_schedule();
my $homeRooms = process_homerooms();
my ($roomSched, $guestSched, $trackSched) = validate_schedule($schedule);
produce_individual_schedules($roomSched, $guestSched, $trackSched);
produce_printable_schedules($roomSched, $homeRooms);
make_konopas_data($schedule);
make_fake_cache_manifest();

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

sub process_homerooms {
    open(IN, "<", "homerooms.csv");
    my @lines = <IN>;
    chomp @lines;
    close(IN);
    
    my $homeRooms = {};

    my $headerLine = shift @lines;
    my @headers = split(/,/, $headerLine);

    my @records;

    my @splitLines = quoted_csv_split(@lines);

    foreach my $line (@splitLines) {
        $homeRooms->{$line->[0]} = $line->[1];
    }

    return $homeRooms;
}

sub process_db_schedule {
    my $dbh = Util::DB::getDatabaseConnection();

    my %dayMapping = ('2015-08-06' => 'Thu', '2015-08-07' => 'Fri', '2015-08-08' => 'Sat', '2015-08-09' => 'Sun');

    my $error; my $program; my $programTracks; my $programGuests; my $programFlags;

    ($program, $error) = Util::DB::dbSelect($dbh, 'id', 'id, start_date, start_time, title, subtitle, type, short_description, description, loc, mins', ['program'], '1=1', []);
    if (!defined($program)) {
        print STDERR "DIE: can't get program\n";
        die;
    }

    ($programTracks, $error) = Util::DB::dbSelect($dbh, 'id', 'program_track.id AS id, program_id, name', ['program_track', 'track'], 'program_track.track_id = track.id', []);
    ($programFlags, $error) = Util::DB::dbSelect($dbh, 'id', 'program_flags.id AS id, program_id, name, flag_type', ['program_flags', 'flags'], 'program_flags.flag_id = flags.id', []);
    ($programGuests, $error) = Util::DB::dbSelect($dbh, 'id', 'program_people.id AS id, program_id, people.id AS guest_id, full_name, prefix, forename, surname, bio, link_bio, link_img', ['program_people', 'people'], 'program_people.people_id = people.id', []);

    my $dayFormatter = DateTime::Format::Strptime->new(locale => 'en_GB', time_zone => $localTZ, pattern => '%a');
    my $timeFormatter = DateTime::Format::Strptime->new(locale => 'en_GB', time_zone => $localTZ, pattern => '%R');

    my @records;

    foreach my $programTrackId (keys %$programTracks) {
        push @{$program->{$programTracks->{$programTrackId}{'program_id'}}{'Tracks'}}, $programTracks->{$programTrackId}{'name'};
    }

    foreach my $programGuestId (keys %$programGuests) {
        push @{$program->{$programGuests->{$programGuestId}{'program_id'}}{'Guests'}}, $programGuests->{$programGuestId};
    }

    foreach my $programFlagId (keys %$programFlags) {
        push @{$program->{$programFlags->{$programFlagId}{'program_id'}}{'Flags'}}, $programFlags->{$programFlagId};
    }

    foreach my $programId (keys %$program) {
        my $programItem = $program->{$programId};
        # Track,Track,EventShort,Event,EventClass,Flags,StartDay,StartTime,StartDT,EndDay,EndTime,EndDT,Room,Guests
        my $programRecord = {};
        if (!defined($programItem->{'title'})) {
            print STDERR "*** Program ID $programId is empty, skipping\n";
            next;
        }
        if (!exists($programItem->{'Tracks'})) {
            print STDERR "*** Program ID $programId has no tracks, skipping\n";
            next;
        }
        $programRecord->{'Tracks'} = $programItem->{'Tracks'};
        $programRecord->{'Guests'} = $programItem->{'Guests'};
        $programRecord->{'Flags'} = $programItem->{'Flags'};
        $programRecord->{'Id'} = $programId;
        $programRecord->{'EventShort'} = $programItem->{'title'};
        $programRecord->{'Event'} = $programItem->{'title'} . (defined($programItem->{'subtitle'}) && ($programItem->{'subtitle'} ne '') ? ' - ' . $programItem->{'subtitle'} : '');
        $programRecord->{'EventClass'} = $programItem->{'type'};
        $programRecord->{'Room'} = $programItem->{'loc'};
        $programRecord->{'Rooms'} = handle_room($programItem->{'loc'});
        $programRecord->{'Description'} = $programItem->{'description'};
        $programRecord->{'StartDay'} = $dayMapping{$programItem->{'start_date'}};
        $programRecord->{'StartTime'} = $programItem->{'start_time'};
        $programRecord->{'StartTime'} =~ s/:00$//;
        $programRecord->{'StartDT'} = $programRecord->{'StartDay'} . ' ' . $programRecord->{'StartTime'};
        $programRecord->{'StartObj'} = dt_to_obj($programRecord->{'StartDT'});
        $programRecord->{'EndObj'} = $programRecord->{'StartObj'}->clone->add(minutes => $programItem->{'mins'});
        $programRecord->{'EndDay'} = $dayFormatter->format_datetime($programRecord->{'EndObj'});
        $programRecord->{'EndTime'} = $timeFormatter->format_datetime($programRecord->{'EndObj'});
        $programRecord->{'EndDT'} = $programRecord->{'EndDay'} . ' ' . $programRecord->{'EndTime'};
        push @records, $programRecord;
    }

    Util::DB::dropDatabaseConnection($dbh);
    return \@records;
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
        $date = 6;
    } elsif ($day eq 'Fri') {
        $date = 7;
    } elsif ($day eq 'Sat') {
        $date = 8;
    } else {
        $date = 9;
    }
    my ($hr, $min) = split(/:/, $time);

    return DateTime->new(
        year => 2015,
        month => 8,
        day => $date,
        hour => $hr,
        minute => $min,
        second => 0,
        time_zone => $localTZ);
}

sub validate_schedule {
    my $schedule = shift;

    my $rooms = {}; my $guests = {}; my $tracks = {};

    foreach my $session (@$schedule) {
        # don't bother booking anything if the session is cancelled
        next if (has_session_flag($session, "Cancelled"));
        foreach my $room (@{$session->{'Rooms'}}) {
            $rooms = book_room($rooms, $room, $session);
        }
        foreach my $guest (@{$session->{'Guests'}}) {
            $guests = book_guest($guests, $guest, $session);
        }
        foreach my $track (@{$session->{'Tracks'}}) {
            $tracks = book_track($tracks, $track, $session);
        }
    }
    return ($rooms, $guests, $tracks);
}

sub book_room {
    my ($rooms, $room, $session) = @_;

    if (session_does_not_clash($rooms->{$room}, $session)) {
        push @{$rooms->{$room}}, $session;
        print 'Booked room "' . $room . '" for "' . $session->{'Event'} . '" OK!' .  "\n" if $DEBUG;
    } else {
        print "*** CLASH for $room\n";
    }

    return $rooms;
}

sub book_guest {
    my ($guests, $guest, $session) = @_;

    if (session_does_not_clash($guests->{$guest->{'full_name'}}, $session, 1)) {
        push @{$guests->{$guest->{'full_name'}}}, $session;
        print 'Booked guest "' . $guest->{'full_name'} . '" for "' . $session->{'Event'} . '" OK!' .  "\n" if $DEBUG;
    } else {
        print "*** CLASH for $guest->{'full_name'}\n";
    }
    return $guests;
}

sub book_track {
    my ($tracks, $track, $session) = @_;
    push @{$tracks->{$track}}, $session;
    print 'Added "' . $session->{'Event'} . '" to track "' . $track . '" OK!' . "\n" if $DEBUG;
}

sub session_does_not_clash {
    my ($list, $newSession, $guestCheck) = @_;

    foreach my $bookedSession (@$list) {
        # it is NOT a clash if:
        # the end time of bS is before the start time of nS
        # the start time of bS is after the end time of nS
        my $bookedStart = $bookedSession->{'StartObj'}->clone;
        my $bookedEnd = $bookedSession->{'EndObj'}->clone;
        my $newStart = $newSession->{'StartObj'}->clone;
        my $newEnd = $newSession->{'EndObj'}->clone;

        if (defined($guestCheck) && $guestCheck) {
            $bookedStart->subtract(minutes => 1);
            $bookedEnd->add(minutes => 1);
            $newStart->subtract(minutes => 1);
            $newEnd->add(minutes => 1);
        }

        next if ($bookedStart >= $newEnd);
        next if ($bookedEnd <= $newStart);

        if (has_session_flag($newSession, 'NoClash') || has_session_flag($bookedSession, 'NoClash')) {
            # print "*!* Disregarding clash because saw the NoClash flag!\n";
            next;
        }
        if (has_session_flag($newSession, 'Cancelled') || has_session_flag($bookedSession, 'Cancelled')) {
            # print "*!* Disregarding clash because saw the Cancelled flag!\n";
            next;
        }
        print "Clash! " . $bookedSession->{'StartDT'} . " - " . $bookedSession->{'EndDT'} . " -vs- " . $newSession->{'StartDT'} . " - " . $newSession->{'EndDT'} . "\n";
        return 0;
    }

    return 1;
}

sub fileNamize {
    my $input = shift;
    my $output = $input;
    $output =~ s/\s+(.)/\U$1\L/g;
    $output =~ s/\//_/g;
    return $output . ".txt";
}

sub produce_individual_schedules {
    my ($rooms, $guests, $tracks) = @_;

    mkdir('rooms') unless (-d 'rooms');

    foreach my $thisRoom (sort keys %$rooms) {
        my $roomFileName = 'rooms/' . fileNamize($thisRoom);
        print "Generating timetable for room '$thisRoom' to: $roomFileName\n" if $DEBUG;

        open(OUT, '>'. $roomFileName);
        binmode(OUT, ":utf8"); 
        print OUT "Room: $thisRoom\n\n";

        foreach my $s (sort {$a->{'StartObj'} <=> $b->{'StartObj'}} @{$rooms->{$thisRoom}}) {
            print OUT $s->{'StartDT'} . ' - ' . $s->{'EndDT'} . ': ' . $s->{'Event'} . render_text_flags($s->{'Flags'}) . ' (' . join(", ", sort @{$s->{'Tracks'}}) . ")\n";
        }
        print OUT "\n\n";

        close(OUT);
    }

    mkdir('guests') unless (-d 'guests');

    foreach my $thisGuest (sort keys %$guests) {
        my $guestFileName = 'guests/' . fileNamize($thisGuest);
        print "Generating timetable for guest '$thisGuest' to: $guestFileName\n" if $DEBUG;

        open(OUT, '>'. $guestFileName);
        binmode(OUT, ":utf8"); 
        print OUT "Guest: $thisGuest\n\n";

        foreach my $s (sort {$a->{'StartObj'} <=> $b->{'StartObj'}} @{$guests->{$thisGuest}}) {
            print OUT $s->{'StartDT'} . ' - ' . $s->{'EndDT'} . ': ' . $s->{'Event'} . render_text_flags($s->{'Flags'}) . ' (' . join(", ", sort @{$s->{'Tracks'}}) . ') - Room: ' . $s->{'Room'} . "\n";
        }
        print OUT "\n\n";

        close(OUT);
    }

    mkdir('tracks') unless (-d 'tracks');

    foreach my $thisTrack (sort keys %$tracks) {
        my $trackFileName = 'tracks/' . fileNamize($thisTrack);
        print "Generating timetable for track '$thisTrack' to: $trackFileName\n" if $DEBUG;

        open(OUT, '>'. $trackFileName);
        binmode(OUT, ":utf8"); 
        print OUT "Track: $thisTrack\n\n";

        foreach my $s (sort {$a->{'StartObj'} <=> $b->{'StartObj'}} @{$tracks->{$thisTrack}}) {
            print OUT $s->{'StartDT'} . ' - ' . $s->{'EndDT'} . ': ' . $s->{'Event'} . render_text_flags($s->{'Flags'}) . ' - Room: ' . $s->{'Room'} . "\n";
        }
        print OUT "\n\n";

        close(OUT);
    }
}

sub format_time_for_print {
    my $thisTime = shift;
    my $timeFormatter = DateTime::Format::Strptime->new(locale => 'en_GB', time_zone => $localTZ, pattern => '%I:%M%p');
    my $formattedTime = lc($timeFormatter->format_datetime($thisTime));
    $formattedTime =~ s/^0//;
    chomp $formattedTime;
    return $formattedTime;
}

sub render_room {
    my ($room, $asText) = @_;
    my $printableRoom = $room;
    if ($room =~ m/^\d+/) {
        if ($asText) {
            $printableRoom = "Room $room";
        } else {
            $printableRoom = "Room&nbsp;$room";
        }
    }
    if ($asText) {
        $printableRoom =~ s/-/ /;
    } else {
        $printableRoom =~ s/-/&nbsp;/;
    }
    return $printableRoom;
}

sub has_session_flag {
    my ($s, $name) = @_;
    foreach my $flag (@{$s->{'Flags'}}) {
        return 1 if ($flag->{'name'} eq $name);
    }
    return 0;
}

sub render_span_flags {
    my $flags = shift;
    my $output = '';
    foreach my $thisFlag (sort {($a->{'flag_type'} cmp $b->{'flag_type'}) || ($a->{'name'} cmp $b->{'name'})} @$flags) {
        my $type = $thisFlag->{'flag_type'};
        my $name = $thisFlag->{'name'};
        next if ($type eq 'hidden');
        $name =~ s/ /&nbsp;/g;
        $output .= qq( <span class="flag_${type}">$name</span>);
    }
    return $output;
}

sub render_text_flags {
    my $flags = shift;
    if (!defined($flags) || ((ref($flags) eq 'ARRAY') && !scalar(@$flags))) {
        return '';
    }
    my @printableList;
    foreach my $thisFlag (sort {($a->{'flag_type'} cmp $b->{'flag_type'}) || ($a->{'name'} cmp $b->{'name'})} @$flags) {
        my $type = $thisFlag->{'flag_type'};
        my $name = $thisFlag->{'name'};
        next if ($type eq 'hidden');
        push @printableList, $name;
    }
    if (!scalar(@printableList)) {
        return '';
    }
    return ' [' . join(", ", @printableList) . ']';
}

sub produce_printable_schedules {
    my ($rooms, $homeRooms) = @_;

    my @strictLHSTimes = qw(09:00 10:00 11:15 11:45 13:00 13:30 14:45 15:15 16:30 17:00 18:15 18:45 20:00 20:30 21:45 22:15 23:30);
    my @roomGroupings = (['County-A', 'County-B', 'County-C', '11', '12'], ['Connaught-A', 'Connaught-B', '32', '38'], ['Royal-A', 'Royal-B', 'Royal-C', '40', '41'], 
                         ['Commonwealth-West', 'Commonwealth-East', 'Newbury-1', 'Atrium', '16', '30']);
    my %extraRoomMappings = (0 => {room => 'County', colspan => 3}, 1 => {room => 'Connaught', colspan => 2}, 2 => {room => 'Royal', colspan => 3}, 3 => {room => 'Commonwealth', colspan => 2});

    my %scheduleDays = ('Thu' => {start => dt_to_obj('Thu 18:00'), end => dt_to_obj('Fri 02:00')},
                        'Fri' => {start => dt_to_obj('Fri 08:00'), end => dt_to_obj('Sat 02:00')},
                        'Sat' => {start => dt_to_obj('Sat 08:00'), end => dt_to_obj('Sun 02:00')},
                        'Sun' => {start => dt_to_obj('Sun 08:00'), end => dt_to_obj('Sun 23:45')}
                        );

    mkdir('printable') unless (-d 'printable');

    # group sessions per room per day
    
    my %roomsPerDay = (map {$_ => {}} keys %scheduleDays);

    foreach my $roomName (keys %$rooms) {

SESSION:
        foreach my $session (@{$rooms->{$roomName}}) {
            foreach my $scheduleDay (keys %scheduleDays) {
                if (($session->{'StartObj'} >= $scheduleDays{$scheduleDay}->{'start'}) && 
                    ($session->{'StartObj'} <  $scheduleDays{$scheduleDay}->{'end'})) {
                    push @{$roomsPerDay{$scheduleDay}->{$roomName}}, $session;
                    next SESSION;
                }
            }
            print "Warning: " . $session->{'Event'} . " didn't match a day!\n";
        }
    }

    # foreach day, now identify track groupings
    my %pageMappings;

    foreach my $thisDay (keys %roomsPerDay) {
        my @dayRooms = sort keys %{$roomsPerDay{$thisDay}};
        my $pageNum = 0;
        foreach my $roomPageList (@roomGroupings) {
            foreach my $room (@$roomPageList) {
                if (scalar(@{$roomsPerDay{$thisDay}->{$room}})) {
                    push @{$pageMappings{$thisDay}->{$pageNum}}, $room;
                }
            }
            $pageNum++;
        }
        foreach my $pageNum (keys %extraRoomMappings) {
            my $room = $extraRoomMappings{$pageNum}->{'room'};
            if (scalar(@{$roomsPerDay{$thisDay}->{$room}})) {
                push @{$pageMappings{$thisDay}->{$pageNum}}, $room;
            }
        }
    }

    # now, given room groupings, iterate through the times between the day-times and build the LHS

    foreach my $thisDay (keys %pageMappings) {
        my @todaysTimes = ();
        # seed with strictLHSTimes
        foreach my $thisTime (@strictLHSTimes) {
            my $thisTimeObj = dt_to_obj($thisDay . ' ' . $thisTime);
            if (($thisTimeObj >= $scheduleDays{$thisDay}->{'start'}) &&
                ($thisTimeObj <= $scheduleDays{$thisDay}->{'end'})) {
                push @todaysTimes, $thisTimeObj;
            }
        }
                
        foreach my $pageNum (sort {$a <=> $b} keys %{$pageMappings{$thisDay}}) {
            my @thisPageTimes; my %timesForThisPage; my %sessionsByTime; my %trackColSpans; my $totalColumns;
            push @thisPageTimes, @todaysTimes;
            my @roomGrouping = @{$pageMappings{$thisDay}->{$pageNum}};
            foreach my $roomName (@roomGrouping) {
                my @wantedSessions = @{$roomsPerDay{$thisDay}->{$roomName}};
                foreach my $session (@wantedSessions) {
                    push @thisPageTimes, $session->{'StartObj'};
                    push @thisPageTimes, $session->{'EndObj'};
                    my $formattedTime = format_time_for_print($session->{'StartObj'});
                    push @{$sessionsByTime{$formattedTime}->{$roomName}}, $session;
                }
            }

            foreach my $thisPageTime (@thisPageTimes) {
                my $formattedTime = format_time_for_print($thisPageTime);
                $timesForThisPage{$formattedTime} = $thisPageTime;
            }

            my @orderedTimes = sort {$timesForThisPage{$a} <=> $timesForThisPage{$b}} keys %timesForThisPage;
            print "$thisDay (" . join(',', @roomGrouping) . "):\n\t" . join("\n\t", map {$_ . ' => ' . $timesForThisPage{$_}} @orderedTimes) . "\n\n" if $DEBUG;
            # $totalColumns = scalar(@roomGrouping);
            $totalColumns = scalar(@{$roomGroupings[$pageNum]});
           

            open(OUT, ">", "printable/" . $thisDay . $pageNum . '.html');
            binmode(OUT, ":utf8"); 
            print OUT <<EOHD;
<html>
    <head>
    <meta http-equiv="Content-Type" content="text/html; charset=utf-8"/>
    <title>$thisDay p$pageNum</title>
    <link rel="stylesheet" href="bootstrap.min.css" />
    <link rel="stylesheet" href="bootstrap-theme.min.css" />
    <link rel="stylesheet" href="schedule-extra.css" />
    </head>
    <body>
        <table width="100%" class="table table-bordered table-condensed">
            <col style="width: 5%" />
EOHD
            my @colSizes;
            my $baseFactor = 95 / $totalColumns;
            my $modFactor = 2.5;
            my $remainder = 95; my $smallColCount = 0;
            my $singleWidth = $baseFactor;

            foreach my $roomName (@{$roomGroupings[$pageNum]}) {
                print OUT "         <col style=\"width: " . $singleWidth . "%\" />\n";
            }
                
            print OUT <<EOHD;
            <thead>
            <tr>
                <th class="text-right">Time</th>
EOHD
            my @columnIndexes;
            foreach my $roomName (@{$roomGroupings[$pageNum]}) {
                my $roomOutput = $roomName;
                $roomOutput =~ s/-/ /;
                if ($roomOutput =~ m/^\d+$/) {
                    $roomOutput = "Room $roomName";
                } elsif ($roomOutput =~ m/ C$/) {
                    $roomOutput .= ' &amp; D';
                }
                push @columnIndexes, $roomName;
                print OUT "             <th class=\"text-center\">$roomOutput</th>\n";
            }
            print OUT "         </tr>\n";
            print OUT "         </thead>\n";
            print OUT "         <tbody>\n";

            my $timeIndex = 0; my %rowSpanByColumn; my %colSpanByRow;
            foreach my $orderedTime (@orderedTimes) {
                print OUT "         <tr>\n";
                print OUT "         <td class=\"text-right\">$orderedTime</td>\n";
                my $columnIdx = 0;
                foreach my $roomName (@{$roomGroupings[$pageNum]}) {
                    if ($rowSpanByColumn{$columnIndexes[$columnIdx]} > 0) {
                        $rowSpanByColumn{$columnIndexes[$columnIdx]}--; # no output, we rowspanned over...
                        $columnIdx++;
                    } elsif (($colSpanByRow{$timeIndex} > 0) && ($columnIdx < $colSpanByRow{$timeIndex})) {
                        $columnIdx++; # no output, we colspanned over...
                    } elsif (($columnIdx == 0) && (exists($sessionsByTime{$orderedTime}->{$extraRoomMappings{$pageNum}->{'room'}}))) {
                        my $s = $sessionsByTime{$orderedTime}->{$extraRoomMappings{$pageNum}->{'room'}}[0];
                        my $rowSpan = 1;
                        foreach my $laterTime (@orderedTimes[($timeIndex+1)..$#orderedTimes]) {
                            if ($s->{'EndObj'} > $timesForThisPage{$laterTime}) {
                                $rowSpan++;
                            }
                        }
                        my $colSpanPrint = ' colspan="'. $extraRoomMappings{$pageNum}->{'colspan'} . '"';
                        my $rowSpanPrint = '';
                        if ($rowSpan > 1) {
                            $rowSpanPrint = ' rowspan="'. $rowSpan .'"';
                            $rowSpanByColumn{$columnIndexes[$columnIdx]} = $rowSpan - 1;
                        }

                        foreach my $rowSpanRows (0..($rowSpan - 1)) {
                            $colSpanByRow{$timeIndex + $rowSpanRows} = $extraRoomMappings{$pageNum}->{'colspan'};
                        }
                            
                        my $boxContents = ($s->{'EventShort'}) . render_text_flags($s->{'Flags'}) . ' <i>(' . join(", ", sort @{$s->{'Tracks'}}) . ')</i>';
                        print OUT "             <td class=\"content\"$rowSpanPrint$colSpanPrint>" . $boxContents . "</td>\n";
                        $columnIdx++;
                    } elsif (!exists($sessionsByTime{$orderedTime}->{$roomName})) {
                        print OUT "             <td class=\"empty\">&nbsp;</td>\n";
                        $columnIdx++;
                    } else {
                        my $s = $sessionsByTime{$orderedTime}->{$roomName}[0];
                        my $rowSpan = 1;
                        foreach my $laterTime (@orderedTimes[($timeIndex+1)..$#orderedTimes]) {
                            if ($s->{'EndObj'} > $timesForThisPage{$laterTime}) {
                                $rowSpan++;
                            }
                        }
                        my $rowSpanPrint = '';
                        if ($rowSpan > 1) {
                            $rowSpanPrint = ' rowspan="'. $rowSpan .'"';
                            $rowSpanByColumn{$columnIndexes[$columnIdx]} = $rowSpan - 1;
                        }
                            
                        my $boxContents = ($s->{'EventShort'}) . render_text_flags($s->{'Flags'}) . ' <i>(' . join(", ", sort @{$s->{'Tracks'}}) . ')</i>';
                        print OUT "             <td class=\"content\"$rowSpanPrint>" . $boxContents . "</td>\n";
                        $columnIdx++;
                    }
                }
                print OUT "</tr>\n";
                $timeIndex++;
            }
            print OUT <<EOHD;
            </tbody>
        </table>
    </body>
</html>
EOHD
            close(OUT);
        }
    }
}

sub make_konopas_data {
    my ($schedule) = @_;
    my $personHash = {};
    
    my $programList = [];

    my $unpublishedCtr = 0; my $hideGuestsCtr = 0;

    foreach my $s (@$schedule) {
        if (has_session_flag($s, "Unpublished")) {
            print "* Unpublished: $s->{'Id'} ($s->{'Event'})\n";
            $unpublishedCtr++;
            next;
        }
        if (has_session_flag($s, "Hide guests")) {
            print "* Hiding guests: $s->{'Id'} ($s->{'Event'})\n";
            $hideGuestsCtr++;
        }

        my $boxContents = ($USESHORT ? $s->{'EventShort'} : $s->{'Event'}) . render_span_flags($s->{'Flags'});

        # *id, *title, *tags: [], *date, *time, *mins, *loc: [], people: [{id, name}], *desc
        my $newProgramItem = {id => "$s->{'Id'}", title => $boxContents, desc => $s->{'Description'}, tags => $s->{'Tracks'}, loc => [render_room($s->{'Room'}, 1)]};

        my $dateFormatter = DateTime::Format::Strptime->new(locale => 'en_GB', time_zone => $localTZ, pattern => '%F');
        my $formattedDate = $dateFormatter->format_datetime($s->{'StartObj'});
        my $timeFormatter = DateTime::Format::Strptime->new(locale => 'en_GB', time_zone => $localTZ, pattern => '%R');
        my $formattedTime = $timeFormatter->format_datetime($s->{'StartObj'});
        
        $newProgramItem->{'date'} = $formattedDate;
        $newProgramItem->{'time'} = $formattedTime;
        my $dtLength = $s->{'EndObj'}->subtract_datetime($s->{'StartObj'});
        $newProgramItem->{'mins'} = $dtLength->in_units('minutes') . "";

        unless (has_session_flag($s, "Hide guests")) {
            foreach my $g (@{$s->{'Guests'}}) {
                if (!exists($personHash->{$g->{'full_name'}})) {
                    my $newPersonItem = {id => "$g->{'guest_id'}", links => {img => $g->{'link_img'}, bio => $g->{'link_bio'}}, prog => [], tags => [], bio => $g->{'bio'}, name => [@{$g}{qw(forename surname prefix)}]};
                    $personHash->{$g->{'full_name'}} = $newPersonItem;
                }

                push @{$personHash->{$g->{'full_name'}}{'prog'}}, "$s->{'Id'}";
                push @{$newProgramItem->{'people'}}, {id => "$g->{'guest_id'}", name => $g->{'full_name'}};
            }
        }

        push @$programList, $newProgramItem;
    }
    print "Unpublished sessions: $unpublishedCtr\n";
    print "Sessions with hidden guests: $hideGuestsCtr\n";

    open(OUT, ">", "people.js");
    print OUT "var people = " . encode_json([values %$personHash]) . ";\n";
    close(OUT);

    open(OUT, ">", "program.js");
    binmode(OUT, ":bytes"); 
    # fuck me I have NO idea why Perl is confused about utf8-ness of this string
    # but just telling it "this is a stream of bytes goddammit" fixes it
    print OUT "var program = " . encode_json($programList) . ";\n";
    close(OUT);

    return 1;
}

sub make_cache_manifest {
    open(OUT, ">", "cache.manifest");
    my $key = time;

    print OUT <<EOHD;
CACHE MANIFEST
# $key

CACHE:
data/program.js
data/people.js
konopas.min.js
skin/skin.css
skin/fonts.css
skin/konopas.css
skin/konopas.min.css
skin/icons.png
skin/Roboto300.ttf
skin/Roboto500.ttf
skin/Oswald400.ttf
/sites/nineworlds.co.uk/files/favicon.ico
/sites/nineworlds.co.uk/themes/nineworlds/logo.png
/sites/nineworlds.co.uk/themes/nineworlds/fonts/hammersmithone-webfont.eot
/sites/nineworlds.co.uk/themes/nineworlds/fonts/hammersmithone-webfont.woff
/sites/nineworlds.co.uk/themes/nineworlds/fonts/hammersmithone-webfont.ttf

NETWORK:
*
EOHD
    close(OUT);
}

sub make_fake_cache_manifest {
    open(OUT, ">", "cache.manifest");
    my $key = time;

    print OUT <<EOHD;
CACHE MANIFEST
# $key

CACHE:
skin/icons.png
skin/Roboto300.ttf
skin/Roboto500.ttf
skin/Oswald400.ttf
/sites/nineworlds.co.uk/files/favicon.ico
/sites/nineworlds.co.uk/themes/nineworlds/logo.png
/sites/nineworlds.co.uk/themes/nineworlds/fonts/hammersmithone-webfont.eot
/sites/nineworlds.co.uk/themes/nineworlds/fonts/hammersmithone-webfont.woff
/sites/nineworlds.co.uk/themes/nineworlds/fonts/hammersmithone-webfont.ttf

NETWORK:
*
EOHD
    close(OUT);
}
