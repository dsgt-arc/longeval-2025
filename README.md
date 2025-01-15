# LongEval 2025 Quickstart Guide

Code for the DS@GT CLEF 2025 LongEval team.
Refer to [docs](docs/) for more information.

## Prerequisites

If you haven't already, install mvn
```bash
brew install maven
```

Install the package into your environment:

```bash
pip install -e .

# install pyspark connectors
./scripts/spark-jars.sh
```

This will add a command line tool `longeval` to your environment.

Populate your local .env file before proceeding. 
For the password variable, you can check the strength of your password [at this website](https://lowe.github.io/tryzxcvbn/).


