#!/usr/local/bin/perl

use strict;
use warnings;

use IO::Socket::SSL;
use Mojo::UserAgent;
use Util::DB;

my $ua = Mojo::UserAgent->new();

my $page = $ua->get('https://nineworlds.co.uk/2015/guest');
my $dom = $page->res->dom;
my $rows = $dom->at(".view-content")->children(".views-row");

my %guests;

foreach my $row (@$rows) {
    my $name = $row->at('.views-field-title .field-content a')->content;
    my $link = $row->at('.views-field-title .field-content a')->attr('href');
    my $img = $row->at('.views-field-field-image .field-content a img')->attr('src');
    $img =~ s/\?itok=.*//;
    $img =~ s/grid_listings_image/guestimage/;
    my $short;
    if (!defined($row->at('.views-field-field-five-word-biography div a'))) {
        print "- Hm, no bio for $name\n";
        $short = '';
    } else {
        $short = $row->at('.views-field-field-five-word-biography div a')->content;
    }

    my @splitName = split(/\s+/, $name);
    my $namePrefix = '';
    if (($splitName[0] eq 'Dr') || ($splitName[0] eq "Rev'd")) {
        $namePrefix = shift @splitName;
    }
    my $firstName = join(' ', @splitName[0..($#splitName - 1)]);
    my $lastName = $splitName[$#splitName];
    $guests{$name} = {name => $name, splitName => [$firstName, $lastName, $namePrefix], link => 'https://nineworlds.co.uk' . $link, img => $img, short => $short};
}

open(OUT, '>', 'scraped-guests.csv');

foreach my $guestName (sort keys %guests) {
    my $guest = $guests{$guestName};
    print "Generating $guest->{'name'}\n";
    print OUT join(",", @{$guest}{qw(name link img short)}) . "\n";
}

close(OUT);

my $dbh = Util::DB::getDatabaseConnection();
my ($people, $error) = Util::DB::dbSelect($dbh, 'id', 'id, bio, link_bio, link_img, full_name, prefix, forename, surname', ['people'], '1=1', []);

foreach my $personId (keys %$people) {
    my $person = $people->{$personId};
    if (exists($guests{$person->{'full_name'}})) {
        print "+ Found existing: $person->{'full_name'} - ";
        my $thisGuest = $guests{$person->{'full_name'}};
        $thisGuest->{'found'} = 1;
        my $updateRecord = {};
        if ($thisGuest->{'short'} ne $person->{'bio'}) {
            $updateRecord->{'bio'} = $thisGuest->{'short'};
            print "new bio - ";
        }
        if ($thisGuest->{'link'} ne $person->{'link_bio'}) {
            $updateRecord->{'link_bio'} = $thisGuest->{'link'};
            print "new link - ";
        }
        if ($thisGuest->{'img'} ne $person->{'link_img'}) {
            $updateRecord->{'link_img'} = $thisGuest->{'img'};
            print "new img - ";
        }
        if (scalar(keys(%$updateRecord))) {
            my ($updatePerson, $updateError) = Util::DB::dbUpdate($dbh, 'people', 'id = ?', [$person->{'id'}], $updateRecord);
            if (!$updatePerson) {
                print "FAILED! $updateError\n";
            } else {
                print "done!\n";
            }
        } else {
            print "no changes.\n";
        }
    } else {
        print "- No match for $person->{'full_name'}, that's a bit weird\n";
    }
}

foreach my $guestName (keys %guests) {
    next if (exists($guests{$guestName}->{'found'}));
    my $thisGuest = $guests{$guestName};
    print "* Inserting $guestName - ";

    my $insertRecord = {bio => $thisGuest->{'short'}, link_bio => $thisGuest->{'link'}, link_img => $thisGuest->{'img'}, full_name => $thisGuest->{'name'}, prefix => $thisGuest->{'splitName'}[2], forename => $thisGuest->{'splitName'}[0], surname => $thisGuest->{'splitName'}[1]};
    my ($insertPerson, $insertError) = Util::DB::dbInsert($dbh, 'people', [qw(bio link_bio link_img full_name prefix forename surname)], $insertRecord);

    if (!$insertPerson) {
        print "FAILED! $insertError\n";
    } else {
        print "done!\n";
    }
}

Util::DB::dropDatabaseConnection($dbh);
