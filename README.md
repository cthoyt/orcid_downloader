<!--
<p align="center">
  <img src="https://github.com/cthoyt/orcid_downloader/raw/main/docs/source/logo.png" height="150">
</p>
-->

<h1 align="center">
  ORCID Downloader
</h1>

<p align="center">
    <a href="https://github.com/cthoyt/orcid_downloader/actions/workflows/tests.yml">
        <img alt="Tests" src="https://github.com/cthoyt/orcid_downloader/workflows/Tests/badge.svg" />
    </a>
    <a href="https://pypi.org/project/orcid_downloader">
        <img alt="PyPI" src="https://img.shields.io/pypi/v/orcid_downloader" />
    </a>
    <a href="https://pypi.org/project/orcid_downloader">
        <img alt="PyPI - Python Version" src="https://img.shields.io/pypi/pyversions/orcid_downloader" />
    </a>
    <a href="https://github.com/cthoyt/orcid_downloader/blob/main/LICENSE">
        <img alt="PyPI - License" src="https://img.shields.io/pypi/l/orcid_downloader" />
    </a>
    <a href='https://orcid_downloader.readthedocs.io/en/latest/?badge=latest'>
        <img src='https://readthedocs.org/projects/orcid_downloader/badge/?version=latest' alt='Documentation Status' />
    </a>
    <a href="https://codecov.io/gh/cthoyt/orcid_downloader/branch/main">
        <img src="https://codecov.io/gh/cthoyt/orcid_downloader/branch/main/graph/badge.svg" alt="Codecov status" />
    </a>  
    <a href="https://github.com/cthoyt/cookiecutter-python-package">
        <img alt="Cookiecutter template from @cthoyt" src="https://img.shields.io/badge/Cookiecutter-snekpack-blue" /> 
    </a>
    <a href='https://github.com/psf/black'>
        <img src='https://img.shields.io/badge/code%20style-black-000000.svg' alt='Code style: black' />
    </a>
    <a href="https://github.com/cthoyt/orcid_downloader/blob/main/.github/CODE_OF_CONDUCT.md">
        <img src="https://img.shields.io/badge/Contributor%20Covenant-2.1-4baaaa.svg" alt="Contributor Covenant"/>
    </a>
</p>

Download and process ORCID in bulk

## 💪 Getting Started

```python
import orcid_downloader

# Takes 10-15 minutes to download
path = orcid_downloader.ensure_summaries()

# Takes a bit more than an hour to parse after downloading was done
records = orcid_downloader.get_records()
```

The processed records are distributed on [![DOI](https://zenodo.org/badge/DOI/10.5281/zenodo.10137940.svg)](https://doi.org/10.5281/zenodo.10137940).

Grounding can be done on the name field, aliases, and credit name field:

```python
>>> orcid_downloader.ground_researcher("Charles Hoyt")
[ScoredMatch(Term(charles hoyt,Charles Hoyt,orcid,0000-0003-4423-4370,Charles Tapley Hoyt,synonym,orcid,None,None,None),0.5555555555555556,Match(query=Charles Hoyt,ref=Charles Hoyt,exact=True,space_mismatch=False,dash_mismatches={},cap_combos=[]))]
```

> **Note**
> It takes about 5 minutes to warm up the grounder, if the data has already been downloaded and processed.


## 🚀 Installation

<!-- Uncomment this section after your first ``tox -e finish``
The most recent release can be installed from
[PyPI](https://pypi.org/project/orcid_downloader/) with:

```shell
pip install orcid_downloader
```
-->

The most recent code and data can be installed directly from GitHub with:

```shell
pip install git+https://github.com/cthoyt/orcid_downloader.git
```

## 👐 Contributing

Contributions, whether filing an issue, making a pull request, or forking, are appreciated. See
[CONTRIBUTING.md](https://github.com/cthoyt/orcid_downloader/blob/master/.github/CONTRIBUTING.md) for more information on getting involved.

## 👋 Attribution

### ⚖️ License

The code in this package is licensed under the MIT License.

<!--
### 📖 Citation

Citation goes here!
-->

<!--
### 🎁 Support

This project has been supported by the following organizations (in alphabetical order):

- [Harvard Program in Therapeutic Science - Laboratory of Systems Pharmacology](https://hits.harvard.edu/the-program/laboratory-of-systems-pharmacology/)

-->

<!--
### 💰 Funding

This project has been supported by the following grants:

| Funding Body                                             | Program                                                                                                                       | Grant           |
|----------------------------------------------------------|-------------------------------------------------------------------------------------------------------------------------------|-----------------|
| DARPA                                                    | [Automating Scientific Knowledge Extraction (ASKE)](https://www.darpa.mil/program/automating-scientific-knowledge-extraction) | HR00111990009   |
-->

### 🍪 Cookiecutter

This package was created with [@audreyfeldroy](https://github.com/audreyfeldroy)'s
[cookiecutter](https://github.com/cookiecutter/cookiecutter) package using [@cthoyt](https://github.com/cthoyt)'s
[cookiecutter-snekpack](https://github.com/cthoyt/cookiecutter-snekpack) template.

## 🛠️ For Developers

<details>
  <summary>See developer instructions</summary>

The final section of the README is for if you want to get involved by making a code contribution.

### Development Installation

To install in development mode, use the following:

```bash
git clone git+https://github.com/cthoyt/orcid_downloader.git
cd orcid_downloader
pip install -e .
```

### 🥼 Testing

After cloning the repository and installing `tox` with `pip install tox`, the unit tests in the `tests/` folder can be
run reproducibly with:

```shell
tox
```

Additionally, these tests are automatically re-run with each commit in a [GitHub Action](https://github.com/cthoyt/orcid_downloader/actions?query=workflow%3ATests).

### 📖 Building the Documentation

The documentation can be built locally using the following:

```shell
git clone git+https://github.com/cthoyt/orcid_downloader.git
cd orcid_downloader
tox -e docs
open docs/build/html/index.html
``` 

The documentation automatically installs the package as well as the `docs`
extra specified in the [`setup.cfg`](setup.cfg). `sphinx` plugins
like `texext` can be added there. Additionally, they need to be added to the
`extensions` list in [`docs/source/conf.py`](docs/source/conf.py).

The documentation can be deployed to [ReadTheDocs](https://readthedocs.io) using 
[this guide](https://docs.readthedocs.io/en/stable/intro/import-guide.html).
The [`.readthedocs.yml`](.readthedocs.yml) YAML file contains all the configuration you'll need.
You can also set up continuous integration on GitHub to check not only that
Sphinx can build the documentation in an isolated environment (i.e., with ``tox -e docs-test``)
but also that [ReadTheDocs can build it too](https://docs.readthedocs.io/en/stable/pull-requests.html).

### 📦 Making a Release

After installing the package in development mode and installing
`tox` with `pip install tox`, the commands for making a new release are contained within the `finish` environment
in `tox.ini`. Run the following from the shell:

```shell
tox -e finish
```

This script does the following:

1. Uses [Bump2Version](https://github.com/c4urself/bump2version) to switch the version number in the `setup.cfg`,
   `src/orcid_downloader/version.py`, and [`docs/source/conf.py`](docs/source/conf.py) to not have the `-dev` suffix
2. Packages the code in both a tar archive and a wheel using [`build`](https://github.com/pypa/build)
3. Uploads to PyPI using [`twine`](https://github.com/pypa/twine). Be sure to have a `.pypirc` file configured to avoid the need for manual input at this
   step
4. Push to GitHub. You'll need to make a release going with the commit where the version was bumped.
5. Bump the version to the next patch. If you made big changes and want to bump the version by minor, you can
   use `tox -e bumpversion -- minor` after.
</details>
