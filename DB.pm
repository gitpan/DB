package DB;

use strict;
use Config '%Config';
use Data::Dumper;
use DBIx::Password;
use Carp qw(cluck);

use vars qw($VERSION @ISA);

$VERSION = ' $Revision: 0.99 $ ' =~ /\$Revision:\s+([^\s]+)/;

=pod


DB.pm (to be named ... DBIx::?)
- (documentation to be released)

=cut

# Copyright (C) 2005 Patrick Galbraith
# 
# This program is free software; you can redistribute it and/or modify
# it under the terms of the GNU General Public License as published by
# the Free Software Foundation; either version 2 of the License, or
# (at your option) any later version.
# 
# This program is distributed in the hope that it will be useful,
# but WITHOUT ANY WARRANTY; without even the implied warranty of
# MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
# GNU General Public License for more details.
# 
# You should have received a copy of the GNU General Public License
# along with this program; if not, write to the Free Software
# Foundation, Inc., 59 Temple Place, Suite 330, Boston, MA  02111-1307  USA */
# 

##################################################
sub new {
  my ($caller, $opts) = @_;

  my $class = ref($caller) || $caller;

  my $self = {}; 
  # set this to your liking!
  $self->{_timeout}= 30;
  $self->{virtual_user} = $opts->{virtual_user} ;

  bless ($self, ref($class) || $class);
  $self->sqlConnect();
  return($self);
}

##################################################
sub sqlConnect {
  my ($self, $reset) = @_;

  if ($reset) {
    $self->{_dbh}->disconnect;
  }

  if (!(defined $self->{_dbh}) || ! $self->{_dbh}->ping) {

    {
      local @_ ;

      eval {
        local $SIG{'ALRM'} = sub { die 'Connection timed out!'};
        alarm $self->{_timeout} if $Config{d_alarm};
        $self->{_dbh} = DBIx::Password->connect_cached($self->{virtual_user});
        alarm 0 if $Config{d_alarm};
      };

      if ( $@ || ! (defined $self->{_dbh}) ) {
# couldn't connect
        return 0;
      }
    }
  }
  $self->{_dbh}->{RaiseError} = 1;
# we have a connect
  return 1;
}

##################################################
sub _whereClauseBind {
  my ($self, $whereref) = @_;

  return({}) unless $whereref;

  my $values = [];
  my @tmp;
  my $where = '';
  my $whereClauseRef = {};


  if (ref($whereref) eq 'HASH') {
    for (keys %$whereref) {
      if ($_ =~ /^-(\w+)/) {
        push(@tmp, $1 . " = " . $whereref->{$_}); 

      } elsif ($_ =~ /^\~(\w+)/) {
        push(@tmp, "$1 LIKE ?"); 
        push(@$values,$whereref->{$_});

      } else {
        push(@tmp, "$_ = ?"); 
        push(@$values,$whereref->{$_});
      }
    }
    $where = join (" AND ", @tmp);
  } else {
    $where = $whereref;
  }
  $whereClauseRef = {
    where => $where,
    values => $values 
  };

  return($whereClauseRef);
}

##################################################
sub _whereClause {
  my ($self, $whereref) = @_;
  return({}) unless $whereref;

  my @tmp;
  my $whereClauseRef = {};

  if (ref($whereref) eq 'HASH') {
    map { push(@tmp,"$_ = " . $self->{_dbh}->quote($whereref->{$_}) ) } keys %$whereref;
    $whereClauseRef->{where} = join (" AND ", @tmp);
  } else {
    $whereClauseRef->{where} = $whereref;
  }
  return($whereClauseRef);
}

##################################################
sub sqlExec {
  my ($self, $params) = @_;

  my $sth;
  my $rows;

  eval { $sth = $self->{_dbh}->prepare_cached($params->{sql}); };
  if ($@) {
    return $self->errorHandler("DB::sqlExec prepare_cached sql $params->{sql} error ");
  }

  eval { $rows = $sth->execute(@{$params->{values}}); };
  if ($@) {
    return $self->errorHandler("DB::sqlExec sth->execute error ");
  }

  return $sth;
}
########################################################
sub sqlSelectColumns {
  my($self, $table) = @_;
  return unless $table;

  my $rows = $self->{_dbh}->selectcol_arrayref("SHOW COLUMNS FROM $table");
  return $rows;
}

##################################################
sub sqlCount {
  my ($self, $select, $from, $whereref, $other, $options) = @_;
  my $count;
  $count = ($count == '*') ? '*' : '';
  $count = ($select =~ /^(\w+)\,?/) ? $1 : '*';

  my $val = $self->sqlSelect("count($count)", $from, $whereref, '', $options);

  return($val);
}

##################################################
sub sqlSelect {
  my ($self, $select, $from, $whereref, $other, $options) = @_;

  my @row;

  my $whereClauseRef = $self->_whereClauseBind($whereref);

  my $distinct = ($options && $options->{distinct}) ? "DISTINCT" : "";
  my $sql = "SELECT $distinct $select ";
  $sql .= "FROM $from " if $from; 
  $sql .= "WHERE $whereClauseRef->{where} " if $whereClauseRef->{where};
  $sql .= "$other " if $other;

  my $sth = $self->sqlExec({ sql => $sql, values => $whereClauseRef->{values} });
  return(0) unless $sth;

  if (@row = $sth->fetchrow) {
    $sth->finish;

    if (wantarray()) {
      return @row;
    } else {
      return $row[0];
    }
  } else {
    return(0);
  }
}

########################################################
sub getLastInsertId {
  my($self) = @_;
  return $self->sqlSelect('LAST_INSERT_ID()');
}

########################################################
sub sqlSelectArrayRef {
  my ($self, $select, $from, $whereref, $other, $options) = @_;

  my $arrayRef = [];

  my $whereClauseRef = $self->_whereClauseBind($whereref);

  my $sql = "SELECT $select ";
  $sql .= "FROM $from " if $from;
  $sql .= "WHERE $whereClauseRef->{where} " if $whereClauseRef->{where};
  $sql .= "$other" if $other;

  my $sth = $self->sqlExec({ sql => $sql, values => $whereClauseRef->{values} });
  return(0) unless $sth;

  if ($arrayRef = $sth->fetchrow_arrayref) {
    $sth->finish;
    return $arrayRef;
  } else {
    return(0);
  }
}

########################################################
sub sqlSelectHashRef {
  my ($self, $select, $from, $whereref, $other, $options) = @_;

  my $hashRef = {};

  my $whereClauseRef = $self->_whereClauseBind($whereref);

  my $sql = "SELECT $select ";
  $sql .= "FROM $from " if $from;
  $sql .= "WHERE $whereClauseRef->{where} " if $whereClauseRef->{where};
  $sql .= "$other" if $other;

  my $sth = $self->sqlExec({ sql => $sql, values => $whereClauseRef->{values} });
  return(0) unless $sth;

  if ($hashRef = $sth->fetchrow_hashref) {
    $sth->finish;
    return $hashRef;
  } else {
    return(0);
  }
}

########################################################
sub sqlSelectAllArrayRef {
  my ($self, $select, $from, $whereref, $other, $options) = @_;

  my $arrayRef = [];

  my $whereClauseRef = $self->_whereClauseBind($whereref);

  my $distinct = $options->{distinct} ? 'DISTINCT' : '';

  my $sql = "SELECT $distinct $select ";
  $sql .= "FROM $from " if $from;
  $sql .= "WHERE $whereClauseRef->{where} " if $whereClauseRef->{where};
  $sql .= "$other" if $other;

  my $sth = $self->sqlExec({ sql => $sql, values => $whereClauseRef->{values} });
  return(0) unless $sth;

  if ($arrayRef = $sth->fetchall_arrayref()) {
    $sth->finish;
    return $arrayRef;
  } else {
    return(0);
  }
}

########################################################
sub sqlSelectAllHashRef {
  my ($self, $select, $from, $whereref, $other, $options) = @_;

  my $hashRef;

# fetchall_hashref needs a prime to key it's results by
# assume first field in select, else try to get
# from options
  my $prime = $options->{prime} ? $options->{prime} : ($select =~ /^(\w+)\,?/) ? $1 : undef ;

  if (! $prime) {
    $self->{err_msg} = "NO PRIME PROVIDED";
    return(0);
  }

  my $whereClauseRef = $self->_whereClauseBind($whereref);

  my $distinct = $options->{distinct} ? 'DISTINCT' : '';

  my $sql = "SELECT $distinct $select ";
  $sql .= "FROM $from " if $from;
  $sql .= "WHERE $whereClauseRef->{where} " if $whereClauseRef->{where};
  $sql .= "$other" if $other;

  my $sth = $self->sqlExec({ sql => $sql, values => $whereClauseRef->{values} });
  return(0) unless $sth;

  if ($hashRef = $sth->fetchall_hashref($prime)) {
    $sth->finish;
    return $hashRef;
  } else {
    return(0);
  }

}

##################################################
sub sqlInsert {
  my ($self, $table, $params, $options) = @_;

  my @bindValues;
  my ($i, $sql, $fieldString, $valueString) = (1, '', '','');
  my $numParams = (keys %$params);
  my $sth;

  for (keys %$params) {
    if ($_ =~ /^-/) {
      $valueString .= $params->{$_};
      $valueString .= "," if $i < $numParams;
      $_ =~ s/^-//;
    } else {
      $valueString .= "?"; 
      $valueString .= "," if $i < $numParams;
      push(@bindValues,$params->{$_});
    }
    $fieldString .= $_ . ",";
  }

  chop($fieldString);
  chop($valueString);

  $sql = sprintf("INSERT INTO %s (%s) VALUES (%s)", $table, $fieldString, $valueString);

  $sth = $self->sqlExec({ sql => $sql, values => \@bindValues });

  return(0) unless $sth;
  return($sth->rows());
}

##################################################
# currently, only same columns for every record
sub sqlInsertMany {
  my ($self, $table, $records, $options) = @_;

  my ($i, $rows, $sql) = (0, 0, '');
  my $sth;
  my (@values, @fields);

  @fields = sort keys %{$records->[0]}; # sort required
    my $noQuoteFlag = 0;
  map { $noQuoteFlag++ if $_ =~ /^-?(\w+)/ } @fields;

  if ($noQuoteFlag) {
    for my $record (@$records) {
      $rows += $self->sqlInsert($table, $record, $options);
    }
    return($rows);
  } else {
    my $fieldList = join (',',@fields);
    my $valueList = join (',', ('?')x@fields);

    $sql = sprintf "INSERT INTO %s (%s) VALUES (%s)",
    $table, join(',', @fields), join(',', ('?')x@fields);

    eval { $sth = $self->{_dbh}->prepare_cached($sql); };
    return $self->errorHandler() if $@;

    for (@$records) {
      my @values = @{$_}{@fields};
      eval { $sth->execute(@values); };
      return $self->errorHandler() if $@;
    }

    return(0) unless $sth;

    return($sth->{rows});
  }
}

##################################################
sub sqlUpdate {
  my ($self, $table, $params, $whereref, $options) = @_;

  my ($sql, $updateString) = ('', '');
  my @bindValues;

  $sql = "UPDATE $table SET ";
  my $whereClauseRef = $self->_whereClause($whereref);

  for (keys %$params) {
    if ($_ =~ /^-(\w+)/) {
      $updateString .= $1 . " = " . $params->{$_} . ",";
    } else {
      $updateString .= $_ . " = ?,"; 
      push(@bindValues,$params->{$_});
    }
  }

  chop($updateString);
  $sql .= $updateString;
  $sql .= " WHERE $whereClauseRef->{where} " if $whereClauseRef->{where};

  my $sth = $self->sqlExec({ sql => $sql, values => \@bindValues});

  return($sth ? $sth->{rows} : 0 );
}

##################################################
sub sqlDelete {
  my ($self, $from, $whereref, $other, $options) = @_;

  my $whereClauseRef = $self->_whereClauseBind($whereref);

  my $sql = "DELETE FROM $from ";
  $sql .= "WHERE $whereClauseRef->{where} " if $whereClauseRef->{where};
  $sql .= "$other" if $other;

  my $sth = $self->sqlExec({ sql => $sql, values => $whereClauseRef->{values} });
  return($sth->{rows});
}

##################################################
sub sqlDeleteMany {
  my ($self, $from, $whereref, $other, $options) = @_;

  my $rows = 0;
  for my $i(0 .. $#{$whereref}) {
    $rows += $self->sqlDelete($from, $whereref->[$i],$other, $options);
  }
  return($rows);
}

##################################################
sub errorHandler {
  my ($self, $msg) = @_;
  $self->{err_msg} .= "ERROR: " . $msg . " " . $@; 
  $self->{err_msg} .= "\nDBI::errstr " . $self->{_dbh}->errstr;
  return(0);
}

##################################################
sub errorMessage {
  my ($self) = @_;
  return($self->{err_msg});
}
##################################################
sub DESTROY {
  my ($self) = @_;
  $self->{_dbh}->disconnect();

}

1;
