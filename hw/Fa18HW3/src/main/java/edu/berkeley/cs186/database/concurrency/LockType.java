package edu.berkeley.cs186.database.concurrency;

import java.util.Arrays;
import java.util.List;

// Lock Conflict Table
// +------------------------+
// |    | S  | X  | IS | IX |
// +----+----+----+----+----+
// | S  |    | y  |    | y  |
// +----+----+----+----+----+
// | X  | y  | y  | y  | y  |
// +----+----+----+----+----+
// | IS |    | y  |    |    |
// +----+----+----+----+----+
// | IX | y  | y  |    |    |
// +----+----+----+----+----+
public enum LockType {
  S,  // shared
  X,  // exclusive
  IS, // intention shared
  IX; // intention exclusive

  public static List<LockType> conflictingLockTypes(LockType type) {
      switch (type) {
        case S: return Arrays.asList(X, IX);
        case X: return Arrays.asList(S, X, IS, IX);
        case IS: return Arrays.asList(X);
        case IX: return Arrays.asList(S, X);
        default: throw new UnsupportedOperationException();
      }
  }
};
