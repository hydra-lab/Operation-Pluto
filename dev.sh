#!/bin/bash
# Run mock pipeline on luigi local scheduler
# source environment.env
python -m luigi --module main RunMock --local-scheduler
# source deactivate
