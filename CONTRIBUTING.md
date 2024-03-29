# Contributing

Thank you for checking this document! This framework is free software, and we (the
maintainers) encourage and value any contribution.

Here are some guidelines to help you get started.

## Report a bug

Like any non-trivial piece of software, this framework is probably not bug-free. If you
found a bug, feel free to report it to us via GitHub, in the Issues section.

Before doing so, try to search for your bug in the already existing ones - maybe it is
already known, and maybe there's already a solution in place.

Be sure to use the latest version of the framework, and to provide in your bug report:

- Information about your environment (at the very least, operating system and Python
  version).
- Description of what is going on (e.g. logging output, stacktraces).
- A mininum reproducible example, so that other developers can try things around,
  reproduce the bug, and fix it.
- Any additional information that you deem necessary.

## Open a Pull Request

Pull Requests fall into 2 categories:

- Simple changes (typos, documentation, new unit tests, small bugs, etc): go ahead, open
  a new Pull Request against the `master` branch!
- Complex changes (architecture, supported databases, functionalities, etc): we will ask
  you to discuss this with us beforehand. To do so, simply create a new Issue with all
  the relevant information and rationale. It's better to discuss big changes beforehand,
  to give everyone the opportunity to participate to the conversation, reach agreements
  for how things should be done, and avoid work that might end up being rejected.

When you open a Pull Request, the build script will ensure Python style guidelines are
followed. You can look at our various linter configurations for more details, but in a
nutshell:

- [black](https://black.readthedocs.io/en/stable/) is used for formatting.
- [pylint](https://www.pylint.org/), [flake8](https://flake8.pycqa.org/en/latest/) and
  [isort](https://pycqa.github.io/isort/) for Python linting.
- [darglint](https://github.com/terrencepreilly/darglint) and
  [pydocstyle](http://www.pydocstyle.org/en/stable/) for docstrings.

After you have opened a pull request, please add a line in `CHANGELOG.md`
"Unreleased" section.

## Local environment

In order to develop this framework locally, start by creating a virtual environment. You
can use your favourite tool, or:

```shell
python3 -m venv ../diepvries-venv
source ../diepvries-venv/bin/activate
```

Once your virtual environment is active, run:

```shell
python3 -m pip install -e .
```

to install this library as a development package.

In this environment, you can now run any script using this framework.

The easiest way to run the test suite and the linters is to install **tox**:

```shell
pip install -U tox
```

And then run everything with:

```shell
tox
```

To automatically run checks before you commit your changes you should:

* install **pre-commit**
following the instructions from
[https://pre-commit.com/#installation](https://pre-commit.com/#installation):

* install the git hook scripts
```shell
pre-commit install
```

now ```pre-commit``` will run automatically on ```git commit```.

If you have any question or doubt, don't hesitate to open an Issue, we're happy to help!
