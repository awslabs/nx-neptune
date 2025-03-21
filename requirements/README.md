# pip requirements files

## Index

- [`default.txt`](default.txt)
  Default requirements
- [`developer.txt`](developer.txt)
  Optional requirements that may require extra steps to install
- [`examples.txt`](examples.txt)
  Requirements for Neptune Analytic examples
- [`test.txt`](test.txt)
  Requirements for running test suite

## Examples

### Installing requirements

```bash
$ pip install -e .[default]
```

### Running the tests

```bash
$ pip install -e ".[default, test]"
```
