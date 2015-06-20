#!/usr/local/bin/perl

use strict;
use warnings;

use IO::Socket::SSL;
use Mojo::UserAgent;

my $ua = Mojo::UserAgent->new();

my $page = $ua->get('https://nineworlds.co.uk/2015/guest');
my $dom = $page->res->dom;
my $rows = $dom->at(".view-content")->children(".views-row");

my @guests;

foreach my $row (@$rows) {
    my $name = $row->at('.views-field-title .field-content a')->content;
    my $link = $row->at('.views-field-title .field-content a')->attr('href');
    my $img = $row->at('.views-field-field-image .field-content a img')->attr('src');
    $img =~ s/\?itok=.*//;
    my $short = $row->at('.views-field-field-five-word-biography div a')->content;

    push @guests, {name => $name, link => 'https://nineworlds.co.uk' . $link, img => $img, short => $short}; 
}

open(OUT, '>', 'scraped-guests.csv');

foreach my $guest (sort {$a->{'name'} cmp $b->{'name'}} @guests) {
    print "Generating $guest->{'name'}\n";
    print OUT join(",", @{$guest}{qw(name link img short)}) . "\n";
}

close(OUT);
