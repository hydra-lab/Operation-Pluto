# Operation Pluto

[![PyPI version](https://badge.fury.io/py/Operation-Pluto.svg)](https://pypi.python.org/pypi/Operation-Pluto) [![Codacy Badge](https://api.codacy.com/project/badge/Coverage/ae24c1a0b93a45bb972c40af136a01b2)](https://www.codacy.com/app/tc-ying/Operation-Pluto-upstream?utm_source=github.com&utm_medium=referral&utm_content=hydra-lab/Operation-Pluto&utm_campaign=Badge_Coverage)

![data-pipeline](https://github.com/hydra-lab/Operation-Pluto/blob/master/docs/data-pipeline-teaser.png)

[Operation Pluto](https://en.wikipedia.org/wiki/Operation_Pluto) is a pipeline set-up. It plumbs financial and economic data. Focused markets are *Hong Kong*, *U.S.* and *China*.

This data pipeline is organized in [Luigi framework](https://github.com/spotify/luigi) with Python.

## Available Data

Currently connected data sources :

#### Hong Kong

- [Census and Statistics Department](https://www.censtatd.gov.hk)
- [The Hong Kong Association of Banks](https://www.hkab.org.hk)
- [Hong Kong Government Bond Programme](http://www.hkgb.gov.hk)
- [Hong Kong Monetary Authority](https://www.hkma.gov.hk)
- [Hang Seng Indexes Company](https://www.hsi.com.hk)
- [Rating and Valuation Department](https://www.rvd.gov.hk/en)

#### United States

- [U.S. Bureau of Labor Statistics](https://www.bls.gov)
- [Federal Reserve System](https://www.federalreserve.gov)
- [St. Louis Federal Reserve Economic Data](https://fred.stlouisfed.org)

#### China

- ?

#### Master Data

- [Holiday API](https://holidayapi.com)

---
#### *Pool streams into lake. Share data sources.*

###### [![contributions welcome](https://img.shields.io/badge/contributions-welcome-brightgreen.svg?style=flat)](https://github.com/hydra-lab/Operation-Pluto/issues)
---

## Pipeline Organization

- Crawl websites, back-fill past data, and construct file directories. All done as code.
- One table in data source corresponds to one target file.
- Pipeline task is stateful. Overwrite source file the least possible.

![staging-file](https://github.com/hydra-lab/Operation-Pluto/blob/master/docs/staging-file-teaser.png)

## Prerequisites

- [Python 3.5](https://conda.io/miniconda.html)
- [Luigi 2.7](https://luigi.readthedocs.io/en/stable/)

## Getting Started

Have Python 3.5 installed and clone this repository :

    # Clone this repository
    $ git clone https://github.com/hydra-lab/operation-pluto

Install Python dependencies :

    # Installing with Conda may not work
    $ pip install -r requirements.txt

Set up Luigi configuration file :

    # Rename luigi.cfg.sample to luigi.cfg
    $ mv luigi.cfg.sample luigi.cfg

Configure proxies in `luigi.cfg` if you're behind any :

    [proxies]
    https = https://username:password@hostname:port/

Test the installation. New data should be extracted and parsed into folder `test/data` :

    $ python -m luigi --module main RunMock --local-scheduler
    $ ls test/data

High-level job orchestration is done in `main.py`. e.g. `RunAll()` is the wrapper class to initialize whole `data` directory and trigger all processing tasks. In production, tasks should be run on Luigi server. Because Luigi daemon will not run on Windows, simply run :

    # Run Luigi server on http://localhost:8082
    $ luigid
    # Run task on Luigi server
    $ python -m luigi --module main RunAll

Schedule pipeline to run periodically in Task Scheduler or cron. Set up `run.sh` on Windows :

    # Script on Windows
    start luigid
    python -m luigi --module main RunAll
    cmd "/c taskkill /IM "luigid.exe" /T /F"

## License

[![License: AGPL v3](https://img.shields.io/badge/License-AGPL%20v3-blue.svg)](https://www.gnu.org/licenses/agpl-3.0)

This project is licensed under GNU Affero General Public License, Version 3.0. See LICENSE for full license text.
