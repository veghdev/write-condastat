# Contributing

## Issues

You can find our open issues in the project's [issue tracker](https://github.com/vizzuhq/write-condastat/issues/). Please let us know if you find any issues or have any feature requests there.

## CI-CD

### Development environment

You can initialize the development environment of write-condastat with python virtual env.

Run the `dev` make target to set up your environment.

```sh
make dev
```

**Note:** The `dev` make target is going to set up pre-commit and pre-push hooks into your local git repository. Pre-commit hook is going to format the code with `black` and pre-push hook is going to run the CI steps.

Run the `clean` make target to clear your environment.

```sh
make clean
```

### CI

The CI steps check code formatting, run code analyses, check typing and run unit tests over the write-condastat project.

The `check` make target collects the above tasks. Run the `check` make target to run the CI steps.

```sh
make check
```

#### Formatting

The write-condastat project is formatted with `black`.

Run the `format` make target to format your code.

```sh
make format
```

Run the `check-format` target to check code formatting.

```sh
make check-format
```

#### Code analyses

The write-condastat project is analysed with `pylint`.

Run the `lint` make target to run code analyses.

```sh
make lint
```

#### Typing

The write-condastat project is using type hints.

Run the `check-typing` make target to run check code typing.

```sh
make check-typing
```

#### Testing

The write-condastat project is tested with `unittest` testing framework.

Run the `test` make target to install write-condastat into your virtual environment and run the tests.

```sh
make test
```

### Documentation

Run the `doc` make target to build the documentation.

```sh
make doc
```

The documentation will be published to the gh-pages branch with the `doc` workflow.
Online version can be read at [vizzuhq.github.io/write-condastat](https://vizzuhq.github.io/write-condastat).

### Release

write-condastat is distributed on [pypi](https://pypi.org/project/write-condastat). **Note:** You need to be an administrator to release the project.

If you want to release write-condastat follow the steps below.

- You should increase the version number in `setup.py`. The version bump should be in a separated commit.

- Generate the release notes and publish the new release on [Releases](https://github.com/vizzuhq/write-condastat/releases).

 **Note:** Publishing a new release will automatically trigger the `release` workflow which builds, checks and uploads the write-condastat package to [pypi](https://pypi.org/project/write-condastat).

You can build and check the package before a release with the `release` make target.

```sh
make release
```
