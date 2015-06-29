package Util::DB;

use DBI;
use Data::Dumper;

sub getDatabaseConnection {
    my $dsn = "DBI:mysql:database=konopas;host=localhost;mysql_socket=/var/lib/mysql/mysql.sock";
    my $dbh = DBI->connect($dsn, 'konopas', 'konopas');
    return $dbh;
}

sub dropDatabaseConnection {
    my $dbh = shift;
    if (defined($dbh)) {
        $dbh->disconnect();
    }
    return 1;
}

sub dbSelect {
    my ($dbh, $keyField, $sigma, $fromTableList, $whereStatement, $whereValues) = @_;

    my $statement = "SELECT $sigma FROM " . join(", ", @$fromTableList) . " WHERE $whereStatement";

    if ($WantDebugging) {
        print STDERR "DB.STMT: $statement\n";
    }

    my $sth = $dbh->prepare($statement) or return (undef, $dbh->errstr);

    my @bindVals; my $wantResultsList = 0;

    if (ref($whereValues->[0]) eq 'ARRAY') {
        # actually a list of lists.
        @bindVals = @$whereValues;
        $wantResultsList = 1;
    } else {
        @bindVals = ($whereValues);
    }

    my @results;

    foreach my $singleBind (@bindVals) {
        if ($WantDebugging) {
            print STDERR "DB.BIND: " . join(' || ', @$singleBind) . "\n";
        }

        $sth->execute(@$singleBind) or return (undef, $dbh->errstr);
        if (defined($keyField)) {
            push @results, $sth->fetchall_hashref($keyField) or return (undef, $dbh->errstr);
        } else {
            $wantResultsList = 1;
            my $innerResult = $sth->fetchall_arrayref({}) or return (undef, $dbh->errstr);
            push @results, @$innerResult;
        }
    }

    $sth->finish();

    if ($wantResultsList) {
        return (\@results, undef);
    }

    return ($results[0], undef);
}

sub dbUpdate {
    my ($dbh, $updateTable, $whereStatement, $whereValues, $record) = @_;

    my @recordKeys = keys %$record;

    my $statement = "UPDATE $updateTable SET " . join(", ", map {"$_ = ?"} @recordKeys) . " WHERE $whereStatement";

    if ($WantDebugging) {
        print STDERR "DB.STMT: $statement\n";
    }

    my $startResult = $dbh->begin_work;

    if (!$startResult) {
        return (undef, "Couldn't start transaction, update aborted: $dbh->errstr");
    }

    my $updateSTH = $dbh->prepare($statement) or return (undef, $dbh->errstr);

    my @whereValueList;

    if (ref($whereValues->[0]) eq 'ARRAY') {
        @whereValueList = @$whereValues;
    } else {
        push @whereValueList, $whereValues;
    }

    my @results; my $failureCount = 0;

    foreach my $whereValueSet (@whereValueList) {
        return ([], undef) if (!scalar(@$whereValueSet));
        my @bind = (@{$record}{@recordKeys}, @$whereValueSet);

        if ($WantDebugging) {
            print STDERR "DB.BIND: " . join(' || ', @bind) . "\n";
        }

        my $result = $updateSTH->execute(@bind);

        if (!$result || !defined($result)) {
            $failureCount++;
        }
        push @results, $result;
    }

    $updateSTH->finish();

    if ($failureCount) {
        return (undef, "Update failed for $failureCount where-values, update rolled back. Most recent error: $dbh->errstr");
        $dbh->rollback;
    }

    $dbh->commit;

    return (\@results, undef);
}

sub dbDelete {
    my ($dbh, $deleteTable, $whereStatement, $whereValues) = @_;

    my $statement = "DELETE FROM $deleteTable WHERE $whereStatement";

    if ($WantDebugging) {
        print STDERR "DB.STMT: $statement\n";
    }

    my $deleteSTH = $dbh->prepare($statement) or return (undef, $dbh->errstr);

    my @whereValueList;

    if (ref($whereValues->[0]) eq 'ARRAY') {
        @whereValueList = @$whereValues;
    } else {
        push @whereValueList, $whereValues;
    }

    my @results;

    foreach my $whereValueSet (@whereValueList) {
        return ([], undef) if (!scalar(@$whereValueSet));

        my @bind = (@$whereValueSet);

        if ($WantDebugging) {
            print STDERR "DB.BIND: " . join(' || ', @bind) . "\n";
        }

        my $result = $deleteSTH->execute(@bind) or return (undef, $dbh->errstr);
        push @results, $result;
    }

    $deleteSTH->finish();

    return (\@results, undef);
}

sub dbInsert {
    my ($dbh, $table, $keys, $record) = @_;

    my $statement = "INSERT INTO $table (" . join(", ", @$keys) . ") VALUES (" . join(",", map {'?'} @$keys) . ")";

    if ($WantDebugging) {
        print STDERR "DB.STMT: $statement\n";
    }

    my $recordList; my $isBulk = 0; my $insertSTH;

    if (ref($record) eq 'ARRAY') {
        return (1, undef) if (!scalar(@$record));
        $recordList = $record;
        $insertSTH = $dbh->prepare($statement) or return (undef, $dbh->errstr);
        $isBulk = 1;
    } else {
        $recordList = [$record];
        $insertSTH = $dbh->prepare($statement) or return (undef, $dbh->errstr);
    }

    foreach my $singleRecord (@$recordList) {
        my @bind = @{$singleRecord}{@$keys};
        if ($WantDebugging) {
            print STDERR "DB.BIND: " . join(' || ', map {defined($_) ? $_ : 'undef'} @bind) . "\n";
        }
        my $result = $insertSTH->execute(@bind) or return (undef, $dbh->errstr);
    }

    $dbh->commit if $isBulk;

    $insertSTH->finish();

    return (scalar(@$recordList), undef);
}

1;
