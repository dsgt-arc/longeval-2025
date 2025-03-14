# DSGT CLEF LongEval 2025 Guide

Code for the DS@GT CLEF 2025 LongEval team.
Refer to [docs](docs/) for more information.

# Table of Contents

- [Prerequisites](#prerequisites)
- [GCP Development Workflow](#gcp-development-workflow)
- [LongEval Workflow](#longeval-workflow)
- [Creating New LongEval Commands](#creating-new-longeval-commands)

---

### Prerequisites

| Library | Version |
|----------|----------|
| Spark    | 3.5.4   |
| Scala    | 2.12.18   |
| Python    | 3.11.6   |
| Java    | 11.0.19   |

---

1. If you haven't already, install mvn.
This might take a while depending on the strength of your internet connection.
```bash
brew install maven
```

2. Install the package into your environment by running these at the root. This will add a command line tool `longeval` to your environment.
```sh
pip install -e .
mvn clean install
# install pyspark connectors
./scripts/utils/spark-jars.sh
```

4. Ensure that you have all the environment variables in `.env.template` populated and run `source ~/.bash_profile` if using that as well.
- For the password variable, you can check the strength of your password [at this website](https://lowe.github.io/tryzxcvbn/).
- You can populate the `SPARK_JARS` environment variable with the directory where the `opensearch-spark-30_2.12` library lives
- For the `SPARK_HOME` environment variable, it can be pointed to the path of the manually downloaded **spark-3.5.4 version without hadoop**. Downloads can be found [here](https://apache.root.lu/spark/spark-3.2.4/).

5. Create virtual environment and activate it
```sh
python -m venv test_env
source test_env/bin/activate
````
6. Run `docker-compose up` and ensure you can view the OpenSearch Dashboard properly by navigating here: http://localhost:5601/app/home#/

---

### GCP Development Workflow

1. Download the appropriate SDK for your operating system [here](https://cloud.google.com/sdk/docs/install-sdk)
2. Update your PATH env var & shell
    - `echo 'export PATH="$PATH:$HOME/google-cloud-sdk/bin"' >> ~/.zshrc`
    - `source ~/.zshrc`
3. Verify installation happened successfully `gcloud version`
4. Run `gcloud init`
5. When prompted, select
    - `longeval-2025` cloud project
    - `us-central1-a` engine zone
6. To start/stop an instance, you can run
    - `gcloud compute instances start dsgt-longeval-2025`
    - `gcloud compute instances stop dsgt-longeval-2025 --discard-local-ssd=true`
7. Run `gcloud compute config-ssh` to generate a ssh-key
8. Open your `~/.ssh/config` file and take note of server details. Ensure the Hostname matches the external IP address of the VM instance.
9. If running into connection issues, try checking your firewall  settings on GCP.
    - Navigate from `VPC Network` --> `Firewall`
        - Name --> `ssh-allow`
        - Targets --> `All instances in the network`
        - Source IP ranges --> `0.0.0.0/0`
        - Protocols and ports -> `tcp:22`
    - Validate your ssh keys are registered by clicking on `Metadata` in `Settings` in the `Compute Engine Settings`
10. There is an option to ssh into the instance via GCP console, but if you'd like to ssh via CS Code, you may follow the [directions outlined here](https://github.com/dsgt-kaggle-clef/clef-2024-infra/blob/main/docs/onboarding.md#step-3-install-visual-studio-code-and-ssh-extension)

---

### LongEval Workflow

1. Ensure you have a LongEval file directory that models a topology similar to the one below

```
LongEvalTrainCollection
    │
    └── 2023_01
        │
        ├── English
        │     ├── Documents
        │     │       └── Json
        │     │            └─── collector_kodicare_1.txt
        │     │                 collector_kodicare_2.txt
        │     │                           ...
        │     ├── Qrels
        │     │     └─── train.txt
        │     └── Queries
        │           └─── train.tsv
        │
        └── French
                ├── Documents
                │       └── Json
                │            └─── collector_kodicare_1.txt
                │                 collector_kodicare_2.txt
                │                           ...
                ├── Qrels
                │     └─── train.txt
                └── Queries
                      └─── train.tsv
```

2. Run `longeval etl parquet` to convert the txt files above into a parquet format. Before doing so, make sure to update the parameterized `input_path` and `output_path` luigi variables with the appropriate directory location.

3. Run `longeval etl opensearch` to index the parquet files into Opensearch. Before doing so, make sure to update the parameterized `root` luigi variable with the appropriate directory location.

- If you navigate to the OpenSearch Dashboard on your localhost and click the left menu bar -> Dev Tools, you can enter console queries such as the one below to view the indices available.
```
GET _cat/indices?v=true
```
- The above etl commands should yield an index named `devlongevaltraincollection-test-parquet`
- To view the number of entries under that undex you may run

```
GET devlongevaltraincollection-test-parquet/_count
```

---

### Creating New LongEval Commands

- To run an existing longeval command, you may look at the `__init__.py` file under the `/etl` directory to see the possible options to run
    - If you'd like to run a parquet transformation task for your Documents, Queries, and Qrels, you can run `! longeval etl parquet` in the terminal
- If you'd like to create a new luigi workflow, make sure to update these locations
    - Add your new directory within `/etl`
    - Add an empty `__init__.py` file under your new folder
    - Create a `workflow.py` for your new task
    - Update the `__init__.py` directly under the `/etl` directory
