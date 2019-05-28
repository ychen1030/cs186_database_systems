# Project 5: Locking

## Logistics

**Due date: Wednesday 11/28/2018, 11:59:59 PM**

## Overview

In this assignment, you will implement multigranularity locking and integrate
it into the codebase.

## Part 0: Fetching the Assignment

Similar to previous homeworks, to avoid redownloading our maven dependencies every time we start our docker container, we will start our docker container. The first time you work on this homework run the following command:
```
docker run --name cs186hw5 -v <pathname-to-directory-on-your-machine>:/cs186 -it cs186/environment bash
```

Now startup your container again with the following command:
```
docker start cs186hw5
```

The only thing that should happen is the terminal should print cs186hw5. After running those 2 commands once, to open your container in the future the only command you need to run is:
```
docker exec -it cs186hw5 bash
```

While inside your container, navigate to the shared directory:
```
cd cs186
```

Clone the HW5 repo:
```
git clone https://github.com/berkeley-cs186/Fa18HW5.git
```
If you get an error like: `Could not resolve host: github.com`, try restarting your docker machine (run `docker-machine restart` after exiting the docker container), and if that doesn't work restart your entire computer.

If you get an error like fatal: could not create work tree dir: Permission denied, run
```
sudo git clone https://github.com/berkeley-cs186/Fa18HW5.git
```

Before submitting your assignment you must run `mvn clean test` and ensure it works in the docker container.
**We will not accept "the test ran in my IDE" as an excuse.** You should be running the maven tests periodically as you work through the project.

To compile and test your code, run:
```bash
mvn clean compile
mvn clean test
```

If you see this error:
```
Failed to execute goal org.apache.maven.plugins:maven-compiler-plugin:3.2:compile (default-compile) on project database: Fatal error compiling: directory not found: /cs186/FallHW3/target/classes
```
run
```
sudo mvn clean compile
sudo mvn clean test
```

You should see 163 tests run, 63 errors, and 13 skipped tests.

If you wish to speed up running maven tests while doing the assignment, you can run:
```
mvn clean test -Dtest=TestLockType,TestLockManager,TestLockContext,TestLockUtil,TestDatabaseLocking,TestDatabaseQueries
```

This only runs tests directly related to the project (and you should see 63 tests run, 63 errors, 0 skipped tests). **You
are still responsible for passing *all* the tests**, so you should run `mvn clean test` before submitting.

## Part 1: Implementing the Lock Manager and Lock Context

The `concurrency` directory contains a partial implementation of a
lock manager (`LockManager`) and lock context (`LockContext`), which
you must complete in this assignment.

The `LockManager` is a simple lock manager that does one thing: it maintains
a mapping between transactions, objects, and locks held. **It does not
handle any form of multigranularity**: it treats a lock on the database exactly
the same way as a lock on Table A, Page 34 (to `LockManager`, these are
considered **independent** objects).

The `LockContext` is a layer on top of `LockManager`, which represents an object
that can be locked. In theory, we would have a `LockContext` for the database,
one for each table, and then one for every single page in the table. This forms
a tree of `LockContext` objects, which is the hierarchy we will be using for
multigranularity locking.

The provided code creates and maintains this tree for you: the tree is constructed top down,
starting with the `LockContext` for the database. Children are created via
the `LockContext#childContext` method, and the parent context can be accessed
via the `LockContext#parentContext` method. To avoid having to create tons of
objects when we start the database, we only create the `LockContext` for pages as
we need them.

Within the codebase, all lock acquisition is done through the appropriate `LockContext`: to
acquire a lock on a page, we call the `acquire` method on that page's `LockContext`.

The `LockContext` is in charge of checking and enforcing multigranularity
constraints. It uses the `LockManager` to request locks, but then ensures, for example,
that a transaction has an IX lock on the database before it can request an X lock
on a table (and errors out if the IX lock is not held).

In this part, do the following:

1. Read through all of the code in the `concurrency` directory. Many comments contain
   critical information on how you must implement certain functions. If you do not obey
   the comments, you will lose points.
2. Implement the `compatible`, `parentLock`, and `substitutable` methods of `LockType`.
3. Implement the `acquireAndRelease`, `acquire`, `release`, `promote`, `getLockType`,
   and both `getLocks` methods of `LockManager`.
3. Implement the `acquire`, `release`, `promote`, and `escalate` methods of
   `LockContext`.
4. Implement the `requestLocks` method in `LockUtil`, a helper method to acquire
   a lock, and all the intent locks needed to have the lock. **Note**: once you call
   `block()` on a transaction, you should assume that the transaction has acquired the
   lock for any following lines of code in the same method.

After this, you should pass all the tests we have provided to you under `database.concurrency.*`.

You are strongly encouraged to test your code further with your own tests before continuing to the
next part (see the Testing section for instructions on how). Although you will not be graded
on testing, the provided tests are in no sense comprehensive, and bugs in this part may 
cause additional difficulties in implementing and testing in the rest of the assignment.

Note that you may **not** modify the signature of any methods or classes that we
provide to you, but you're free to add helper methods. Also, you should only modify code in
the `concurrency` directory for this part.

## Part 2: Integrating the Lock Manager and Lock Context
At this point, you should have a working, standalone lock manager and context. In
this part, you will modify code throughout the codebase to use locking.

This part will be mostly concerned with code in `database.io.*` and with the Database
and Transaction classes.

The `database.io` package contains the code that actually interfaces with the disk. The
`Page` class represents a single page of data, and exposes an interface for reading
and writing data on this page via the `PageBuffer` inner class. The `PageAllocator` represents
the set of pages for a table or index, and exposes an interface for allocating and unallocating
pages.

The `Database` class represents the entire database, and is the class that users of our
database use to execute queries and create/delete tables. The `Transaction` class is an inner class
of `Database`, and represents a single transaction. All queries and changes in our database are done
through transactions.

#### Task 1: Page-level locking

The simplest scheme for locking is to simply lock pages as we need them. As all reads and
writes to pages are performed via the `Page.PageBuffer` class, it suffices to
change only that.

Modify the `get` and `put` methods of `Page.PageBuffer` to lock the page (and
acquire locks up the hierarchy as needed) with the *least permissive* lock
types.

Note: no more tests will pass after doing this, see the next task for why.

You should modify `database/io/Page.java` for this task (PageBuffer is an
inner class of Page).

#### Task 2: Releasing Locks

At this point, transactions should be acquiring lots of locks needed to do their
queries, but no locks are ever released, which means that
our database won't be able to do much after someone requests an X lock...and we do
just that when initializing the database!

We will be using Strict Two-Phase Locking in our database, which means that lock
releases only happen when the transaction finishes, in the `end` method.

Modify the `end` method of `Database.Transaction` to release all locks the
transaction acquired.

Hint: you can't just release the locks in any order! Think about in what order you are allowed to release
the locks.

You should modify `database/Database.java` for this task (Transaction is an inner class
of Database).

#### Task 3: Writes

At this point, you have successfully added locking to the database! This task,
and the remaining integration tasks, are focused on locking in a more efficient manner.

The first operation that we perform in an insert/update/delete of
a record is reading a bitmap. Following our logic of reactively acquiring locks
only as needed, we would request an S lock on the page when reading the bitmap,
then request a promotion to an X lock. Given that we know we *will* want an X lock,
we should request it to begin with.

Modify `addRecord`, `updateRecord`, `deleteRecord`, and `cleanup` in `Table` to
request the appropriate X lock upfront.

You should modify `database/table/Table.java` for this task.

#### Task 4: Scans

Our approach of locking every page can be inefficient:
a full table scan over a table of 3000 pages will require acquiring over 3000 locks.

Reduce the locking overhead when performing table scans, by locking the
entire table when any of `Table#ridIterator`, `Database.Transaction#sortedScan`, and
`Database.Transaction#sortedScanFrom` is called.

Some helper methods for this task are: `Database#getTableContext` and `Database#getIndexContext`.

You should modify `database/table/Table.java` and `database/Database.java` for this task.

#### Task 5: Indices

Locking pages as we read or write from them works for simple queries. But this is rather
inefficient for indices: if you use an index in any way, locking page by page, you
acquire around `h` locks (where `h` is the height of the index), but effectively have locked
the entire B+ tree (think about why this is the case!), which is bad for concurrency.

There are many ways to lock B+ trees efficiently (one simple method is known as "crabbing",
see the textbook for details and other approaches), but for
simplicity, we are just going to lock the entire tree for both reads and writes (locking page
by page with strict 2PL already acts as a lock on the entire tree, so we may as well just
get a lock on the whole tree to begin with). This is no better for concurrency, but at least reduces the locking overhead.

First, modify BPlusTree constructors to disable locking at lower granularities
(take a look at the methods in `LockContext` for this). Then preemptively acquire locks on relevant indices in the following
methods in `Database.Transaction`: `sortedScan`, `sortedScanFrom`, `lookupKey`, `contains`, `addRecord`, `deleteRecord`,
`updateRecord`, `runUpdateRecordWhere`. (Which indices are relevant for each operation?)

You should modify `database/index/BPlusTree.java` and `database/Database.java` for this task.

#### Task 6: Temporary Tables

As you may recall from HW3 and HW4, there are various places in more complex queries where
we write intermediate relations into a temporary table. At the moment, our database locks
on every single page we touch in the table, even though we know that only one transaction
even has access to the temporary table.

Modify `createTempTable` in `Database.Transaction` to disable locking at lower granularities,
and then acquire an appropriate lock on the temporary table. (What lock is appropriate here?)

You should modify `database/Database.java` for this task.

#### Task 7: Auto-escalation

Although we already optimized table scans to lock the entire table, we may still run into cases
where a single transaction holds locks on a significant portion of a table's pages.

To do anything about this problem, we must first have some idea of how many pages a table uses:
if a table had 1000 pages, and a transaction holds locks on 900 pages, it's a problem, but if a table
had 100000 pages, then a transaction holding 900 page locks is something we can't really optimize.
Begin by modifying methods `PageAllocator` (in `database/io/PageAllocator.java`) to set the capacity of
the table-level lock contexts appropriately.

Once we have a measure of how many pages a table has, and how many pages of it a transaction holds locks on,
we can implement a simple policy to automatically escalate page-level locks into a table-level lock. You
should modify the codebase to escalate locks from page-level to table-level when both of two conditions are met:

1. The transaction holds at least 20% of the table's pages.
2. The table has at least 10 pages (to avoid locking the entire table unnecessarily when the table is very small).

You should count both header *and* data pages for both conditions (but do not count pages in any indices).
Auto-escalation should only happen when a transaction requests a new lock on the table's pages, where both of the
above conditions are met before the new request is considered. You should escalate the lock first, then request the
new lock only if the escalated lock is insufficient.

This task is more open-ended than the other tasks. You may modify any method that has a HW5 todo in them, and you
may additionally add code to any class modified in Part 1 (LockType, LockManager, LockContext, and LockUtil) by
either modifying existing methods or adding new public or private methods. Your code must still pass Part 1 tests,
so it is recommended that you backup your implementations of Part 1 if you opt to modify those classes.

Hint: when are we automatically escalating locks? Think about where you can place the auto-escalation code to ensure
that we always escalate when we need to.

You may modify any of the following files in this task: `database/concurrency/LockType.java`, `database/concurrency/LockManager.java`,
`database/concurrency/LockContext.java`, `database/concurrency/LockUtil.java`, `database/index/BPlusTree.java`, `database/io/Page.java`,
`database/io/PageAllocator.java`, `database/table/Table.java`, `database/Database.java`.

#### Task 8: Transactional DDL

So far (aside from locking temporary table creation), we have been focusing on locking DML\* operations.
We'll now focus on locking DDL\* operations. Our database supports transactional DDL, which is to say,
we *allow* operations like table creation to be done during a transaction.

There are four operations that our database supports that fall under DDL: `createTable`, `createTableWithIndices`,
`deleteTable`, and `deleteAllTables` (all methods of `Database.Transaction`). Carefully consider when in each method
you should acquire locks, and modify them to request the appropriate locks.

You should modify `database/Database.java` for this task.

\*DML and DDL are terms you might remember from the first few lectures. DML is the Data Manipulation Language, which
encompasses operations that play with the data itself (e.g. inserts, queries), whereas DDL is the Data Definition
Language, which encompasses operations that touch the *structure* of the data (e.g. table creation, changes to the
schema).

#### Additional Notes

At this point, you should pass all the test cases in `database.TestDatabaseLocking` and `database.TestDatabaseQueries`.

Note that you may **not** modify the signature of any methods or classes that we
provide to you, but you're free to add helper methods. The only methods that you should
modify are methods that have a HW5 todo comment in them, and any methods you add.

You should make sure that all code you modify belongs to files with HW5 todo comments in them
(e.g. don't add helper methods to PNLJOperator). A full list of files that you may modify follows:

- concurrency/LockType.java
- concurrency/LockManager.java
- concurrency/LockContext.java
- concurrency/LockUtil.java
- index/BPlusTree.java
- io/Page.java
- io/PageAllocator.java
- table/Table.java
- Database.java

## Submitting the Assignment

See Piazza for submission instructions.

## Testing

We strongly encourage testing your code yourself, especially after each part (rather than all at the end). The given
tests for this project (even more so than previous projects) are **not** comprehensive tests: it **is** possible to write
incorrect code that passes them all.

Things that you might consider testing for include: anything that we specify in the comments or in this document that a method should do
that you don't see a test already testing for, and any edge cases that you can think of. Think of what valid inputs
might break your code and cause it not to perform as intended, and add a test to make sure things are working.

To help you get started, here is one case that is **not** in the given tests (and will be included in the hidden tests): when adding
a record in a table with multiple indices, you must lock all of the indices before adding anything.

To add a unit test, open up the appropriate test file (all test files are located in `src/test/java/edu/berkeley/cs186/database`
or subdirectories of it), and simply add a new method to the test class, for example:

```
@Test
public void testDatabaseSLock() {
    // your test code here
}
```

Many test classes have some setup code done for you already: take a look at other tests in the file for an idea of how to write the test code.

## Grading
- 50% of your grade will be made up of the tests released in this homework (the tests that we provided
  in `database.concurrency.*`, `database.TestDatabaseLocking`, and
  `database.TestDatabaseQueries`).
- 50% will be made up of hidden, unreleased tests that we will run on your submission after the deadline.
