#!/bin/bash
source venv/bin/activate
python --version
python -m pytest UnitTests/ --cov -v --cov-report=html
xdg-open htmlcov/index.html