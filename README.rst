Operation Pluto
===============

`Operation Pluto <https://en.wikipedia.org/wiki/Operation_Pluto>`__ is a
pipeline set-up. It plumbs financial and economic data. Focused markets
are *Hong Kong*, *U.S.* and *China*.

This data pipeline is organized in `Luigi
framework <https://github.com/spotify/luigi>`__ with Python.

Available Data
--------------

Currently connected data sources :

Hong Kong
^^^^^^^^^

-  `Census and Statistics Department <https://www.censtatd.gov.hk>`__
-  `The Hong Kong Association of Banks <https://www.hkab.org.hk>`__
-  `Hong Kong Government Bond Programme <http://www.hkgb.gov.hk>`__
-  `Hong Kong Monetary Authority <https://www.hkma.gov.hk>`__
-  `Hang Seng Indexes Company <https://www.hsi.com.hk>`__
-  `Rating and Valuation Department <https://www.rvd.gov.hk/en>`__

United States
^^^^^^^^^^^^^

-  `U.S. Bureau of Labor Statistics <https://www.bls.gov>`__
-  `Federal Reserve System <https://www.federalreserve.gov>`__
-  `St. Louis Federal Reserve Economic
   Data <https://fred.stlouisfed.org>`__

China
^^^^^

-  ?

Master Data
^^^^^^^^^^^

-  `Holiday API <https://holidayapi.com>`__

Pipeline Organization
---------------------

-  Crawl websites, back-fill past data, and construct file directories.
   All done as code.
-  One table in data source corresponds to one target file.
-  Pipeline task is stateful. Overwrite source file the least possible.


Prerequisites
-------------

-  `Python 3.5 <https://conda.io/miniconda.html>`__
-  `Luigi 2.7 <https://luigi.readthedocs.io/en/stable/>`__

Getting Started
---------------

Have Python 3.5 installed and clone this repository :

::

    # Clone this repository
    $ git clone https://github.com/hydra-lab/operation-pluto

Install Python dependencies :

::

    # Installing with Conda may not work
    $ pip install -r requirements.txt

Set up Luigi configuration file :

::

    # Rename luigi.cfg.sample to luigi.cfg
    $ mv luigi.cfg.sample luigi.cfg

Configure proxies in ``luigi.cfg`` if you’re behind any :

::

    [proxies]
    https = https://username:password@hostname:port/

Test the installation. New data should be extracted and parsed into
folder ``test/data`` :

::

    $ python -m luigi --module main RunMock --local-scheduler
    $ ls test/data

High-level job orchestration is done in ``main.py``. e.g. ``RunAll()``
is the wrapper class to initialize whole ``data`` directory and trigger
all processing tasks. In production, tasks should be run on Luigi
server. Because Luigi daemon will not run on Windows, simply run :

::

    # Run Luigi server on http://localhost:8082
    $ luigid
    # Run task on Luigi server
    $ python -m luigi --module main RunAll

Schedule pipeline to run periodically in Task Scheduler or cron. Set up
``run.sh`` on Windows :

::

    # Script on Windows
    start luigid
    python -m luigi --module main RunAll
    cmd "/c taskkill /IM "luigid.exe" /T /F"

License
-------

|License: AGPL v3|

This project is licensed under GNU Affero General Public License,
Version 3.0. See LICENSE for full license text.

.. |License: AGPL v3| image:: https://img.shields.io/badge/License-AGPL%20v3-blue.svg
   :target: https://www.gnu.org/licenses/agpl-3.0
