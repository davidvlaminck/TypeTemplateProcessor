#!/bin/bash
source venv/bin/activate
python --version
python -m pytest UnitTests/ --cov -v
