# Nidurodb

Nidurodb is a [Nim](https://nim-lang.org/) interface for DuroDBMS, a relational database library.

Nidurodb exploits Nim's metaprogramming facilities to provide database access using a relational algebra instead of SQL.

For example, the following SQL statement:

```
select a as x, b as y
from t
where a > 1000;
```

is equivalent to the following code in Nidurodb:

```
@@t.where(@@a $> 1000){a, b}.rename(a as x, b as y)
```

Loading the result into a sequence:

```
var
  s: seq[tuple[x: int, y: int]]
load(s, @@t.where(@@a $> 1000){a, b}.rename(a as x, b as y), tx)
```

DuroDBMS is based on the principles laid down in the book *Databases, Types, and the Relational Model: The Third Manifesto* by C. J. Date and Hugh Darwen.

## Getting Started

To use Nidurodb, you need to download and install DuroDBMS ([Github](https://github.com/rehartmann/durodbms)|[Sourceforge](https://sourceforge.net/projects/duro/files/duro/)).

Add the lib directory to the library path (LD_LIBRARY_PATH on Linux).

To create databases and tables and to run the tests you will also need the interpreter [Duro D/T](http://duro.sourceforge.net/docs/durodt/tut.html).
To make the interpreter available on the command line, add the bin directory to the search path.

Nidurodb has been tested with Nim 1.0.2. It may not work with versions of Nim earlier than 1.0.

## Running the tests

After adding the DuroDBMS lib directory to the library path and the DuroDBMS bin directory to the system path,
go to the test directory and type:

```
nim -p:../src c -r update.nim
nim -p:../src c -r ra.nim
nim -p:../src c -r datetime.nim
nim -p:../src c -r tva.nim
nim -p:../src c -r rva.nim
nim -p:../src c -r possreps.nim
```

## Authors

[René Hartmann](https://github.com/rehartmann)

## License

This project is licensed under the MIT License - see the [LICENSE](LICENSE) file for details

DuroDBMS is licensed under the LGPL License.
