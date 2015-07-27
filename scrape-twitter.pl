#!/usr/local/bin/perl

use strict;
use warnings;

use IO::Socket::SSL;
use Mojo::UserAgent;

my $ua = Mojo::UserAgent->new();

my $page = $ua->get('https://nineworlds.co.uk/2015/guest');
my $dom = $page->res->dom;
my $rows = $dom->at(".view-content")->children(".views-row");

my @twitters;

foreach my $row (@$rows) {
    my $name = $row->at('.views-field-title .field-content a')->content;
    my $link = $row->at('.views-field-title .field-content a')->attr('href');
    print STDERR "Recursing into $name ($link)...\n";
    my $guestPage = $ua->get('https://nineworlds.co.uk' . $link);
    my $guestDom = $guestPage->res->dom;
    my $guestTwitter = $guestDom->at(".field-name-field-twitter .field-items .field-item a");
    if (defined($guestTwitter)) {
        print "Found " . $guestTwitter->content . "\n";
        push @twitters, $guestTwitter->content;
    }
}

open(OUT, ">", 'twitter.txt');
print OUT join("\n", @twitters) . "\n";
close(OUT);
