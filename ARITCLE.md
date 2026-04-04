![](https://substackcdn.com/image/fetch/$s_!nrLr!,w_1456,c_limit,f_auto,q_auto:good,fl_progressive:steep/https%3A%2F%2Fsubstack-post-media.s3.amazonaws.com%2Fpublic%2Fimages%2F2e7935d8-ef89-442b-a8c3-15da135b0cf3_1536x1024.png)

Hi everyone, I’m Mark - Analytics Engineer at Insurify.

**TLDR: P**art 1 - Ingestion pipeline that scrapes 160,000+ airline reviews from [AirlineQuality.com](http://airlinequality.com/), stages partitioned data to S3 (Managed by terraform), and loads into Snowflake for down stream dbt.

Hi everyone, my name is Mark - Analytics Engineer at Insurify - graduated from TCU in summer 2025 majoring in Computer system Analyst with a minor in Math, Fintech.

This is the first of my series where I built an end-to-end data pipelines/Infrastructure that follow some best practice I learned at Insurify. In this first part, we will dig dive into the extract-load process (ingestion) where we “extract” data from a data source, load it into a simple staging area in S3, process data from `raw` to `processed` directory. Then load data into a landing area in Snowflake. Also. I need to mention that all of the infrastructure (AWS S3, Iam role, Iam users) are being managed by `Terraform` , so a AWS free trial account, AWS CLI, and some exposure to Terraform - Infrastructure as code is needed.

# The Ingestion pipelines process

Overall architecture details can be found [here:](https://github.com/MarkPhamm/skytrax_reviews_extract_load/blob/main/README.md)

Tech stacks:

- Python: scraper, processing data - `Python 3.12, pandas, BeautifulSoup`
- Git/Github - CICD process, version control
- Docker: To run airflow in container
- Airflow on Astronomer: Orchestration layer
- S3/Iam role: Landing area, managed with Terraform
- Snowflake: Data warehouse: Database, Schema, and Table all managed by terraform

Data source → **`scraper.py`** → staging dir (local/s3) - raw → `processing.py` →  staging dir (local/s3) - processed → **`snowflake_load.py`**

![image.png](attachment:58497876-de24-4639-aff2-221f0b7d765b:image.png)

You must be wondering, why the additional transformation from `raw` to `processed`? Why not just dump everything into snowflake and transform with dbt. I believe there are several reason for this (at Insurify we implement this too)

- **Early data validation:** A `raw` to `processed` step helps catch issues before data reaches the DWH.
- **Better observability:** It gives visibility into incoming partner files before loading into Snowflake.
- **Partner delivery workflow:** At Insurify, we have multiple data source, eg:external partners often send reports directly to the `raw` directory via SFTP. This can help us validate data quality before it reach the DWH
- **Pre-processing outside dbt:** We use Python scripts to clean, standardize, and validate files before ingestion.
- **Catch incremental issues sooner:** Things like missing columns or schema changes can be detected early.
- **Faster alerting:** For example, if a required column is missing, we can send a Slack alert immediately.
- **Cleaner warehouse inputs:** Only validated, structured data gets loaded into Snowflake, which keeps downstream models more reliable.

Finally, we will orchestrate using Airflow on astronomer, though there’ no production process, we will run airflow locally with `astro dev start` . There are 3 main dags:

- `dag_crawl.py`
- `dag_process.py`
- `dag_snowflake.py`

# Prerequisite

I would expect you have some experience with Python - with an Orchestration tools (General knowledge of what’s an orchestration tool). Some experience with Docker would be nice, but not required. Experience with git is crucial to clone/replicate the projects. And also, obviously, we

Main AWS/Snowflake infrastructure are all set up in the `Terraform` dir. You will need to create a AWS account, and set up an `terraform-admin` profile. Don’t worry if you don’t have one, we will walk through the set up of creating a profile really quickly

AWS - Terraform learning repo: <https://github.com/MarkPhamm/AWS>. I highly recommend learning this repo before doing this project.

# Step 1: Clone the repo and set up virtual env

The repo link is here: <https://github.com/MarkPhamm/skytrax_reviews_extract_load>. Feel free to clone it where ever you want and chose an IDE that you like. From there we will start running our first scripts.

Before we run our python scripts, we need to resolve our dependencies first. One of the most important concept when developing with python is a virtual environment. Think about as a isolate env where you can download/install any packages you want without effecting other projects. That’s what we will be doing now.

```sql
uv sync
```

`uv` is a dependencies management tool (similar to pip and poetry) that will resolve dependencies and download all the libraries that we need into a dedicated `.venv`

# Step 2:  Run the `scraper.py`

Now everything is set up, we are ready to run everything locally. All of the local development doc can be found [here](https://github.com/MarkPhamm/skytrax_reviews_extract_load/blob/main/docs/local-dev.md)

Copy the example env file and fill in your values (for local-only development, the defaults work as-is):

`cp .env.example .env`

## 2.1 Testing out `scraper.py`with small amount of data

### **2.1.1 Step 1: Run a smoke test**

Scrape 1 airline, 1 page (~100 rows). Output goes to `landing/raw/`:

```
make scrape-smoke
```

Verify the output:

```
ls landing/raw/
# You should see YYYY/MM/raw_data_YYYYMMDD.csv files
```

### **2.1.2 Step 2: Process a subset of data**

Process a specific date's raw file into a cleaned CSV:

```
make process DATE=2026-03-12
```

Or process yesterday's data:

```
make process-yesterday
```

Output goes to `landing/processed/YYYY/MM/clean_data_YYYYMMDD.csv`.

## 2.2 Fully scrape historical data - Took about 10 minute

Scrape all airlines across all pages. This takes a while:

```
make scrape
```

After scrapping everything you should be able to see all raw data in the `landing/raw` dir:

![image.png](attachment:dd24b243-9734-4cb1-9899-4e5996eab205:image.png)

## 2.3 Fully process historical data

After scraping all historical data, you can also process all of them using:

`make process`

# Step 3: Setting up a AWS account and terraform-admin role

1. First, you might need to sign up for a AWS free tier account: regarding this, I’ve already created a end to end document you can find [here](https://github.com/MarkPhamm/AWS/blob/main/docs/01-aws-account-setup.md). Don’t worry if they ask for your credit card details, I’ve the same fear before, but if you’re careful, it’s gonna be all fine ;)
2. Second, you will need to set up an `iam user` call `terraform-admin` . The set up can be found [here](https://github.com/MarkPhamm/AWS/blob/main/docs/02-iam-user-setup.md). This user will have access to AWS cli only. Remember to copy these 2 some where secure:
    - **Access Key ID** (starts with `AKIA...`)
    - **Secret Access Key** (shown only once!)
3. From there, you can set up AWS CLI and Terraform following instruction [here](https://github.com/MarkPhamm/AWS/blob/main/docs/03-aws-cli-terraform-setup.md). The set up will also give you an ARN (**Amazon Resource Name)**. It's a unique address for anything in AWS (a user, a bucket, a role, etc.) - like a mailing address for AWS resources. This set up will live in your `~/.aws/credentials` dir. You will be able to modify it using `nano ~/.aws/credentials`
4. That’s it 🙂. Now the profile is configured, and `Terraform` will pick it up whenever we want to plan/apply/destroy any resources. We will this profile later to create some `iam roles` and `iam users` along with an S3 Bucket. Feel free to checkout some Terraform basics [here](https://chatgpt.com/c/69b86860-fdec-832e-8aca-5af7d11f2300)

# Step 4: Creating a Snowflake trial account

This is very straight forward. Snowflake give you a 30 days free trial with $400. Which is way more than what we need.

1. Go to <https://signup.snowflake.com/>
2. Enter your credential
3. Chose AWS as your cloud service
4. Then verify/create a password and then voila, your cloud DWH is now ready. This is where most of our transformation via `dbt` happen in [part 2.](https://github.com/MarkPhamm/skytrax_reviews_transformation)

# Step 5: Set up AWS IAM role, S3

Now that we have our AWS account and `terraform-admin` profile configured, we can provision all the AWS infrastructure using Terraform. Everything lives in `terraform/aws/` — this is where we define our S3 bucket, IAM role, IAM user, and policies. No clicking around the AWS console, everything is code.

## 5.1 What Terraform creates for us

Let me walk through what resources we're spinning up:

**S3 Bucket (`skytrax-reviews-landing-<your_account_id>`)** — This is our landing zone. All scraped CSVs (both `raw/` and `processed/`) get uploaded here before loading into Snowflake. The bucket name is auto-generated using your AWS account ID, so it's guaranteed to be globally unique. The bucket comes with:

- **Versioning** enabled — so we can recover if something goes wrong
- **Lifecycle rules** — raw and processed files transition to `STANDARD_IA` (Infrequent Access) after 30 days to save cost. Old versions get expired after 90 days
- **Server-side encryption** (AES256) — data at rest is encrypted
- **Public access fully blocked** — no one is accidentally exposing our data

Besides the S3 bucket, Terraform also creates the following IAM resources:

1. **IAM Role** (`skytrax-snowflake-s3-dev`) — the role Snowflake assumes to read from S3 during `COPY INTO`. The trust policy is auto-configured to allow your AWS account (and later Snowflake) to assume it
2. **IAM Policy** (`skytrax-airflow-s3-dev`) — defines the actual S3 permissions: `s3:ListBucket`, `s3:GetObject`, `s3:PutObject`, `s3:DeleteObject`, etc. Basically everything Airflow needs to upload raw files, upload processed files, and clean up old ones
3. **IAM Role Policy Attachment** — attaches the S3 policy to the role
4. **IAM User** (`skytrax-airflow-dev`) — a programmatic-access-only user for Airflow
5. **IAM User Policy Attachment** — attaches the S3 policy directly to the user
6. **IAM Access Key** — auto-generated `access_key_id` + `secret_access_key` credentials for the user — we'll use these later when setting up the Airflow AWS connection

In total, Terraform creates 11 resources: the S3 bucket (with versioning, lifecycle, encryption, and public access block configs) + the 6 IAM resources above.

## 5.2 Configure your variables

First, copy the example tfvars file:

```
cp terraform/aws/terraform.tfvars.example terraform/aws/terraform.tfvars
```

Then edit `terraform/aws/terraform.tfvars` with your values:

```hcl
aws_region  = "us-east-1"
environment = "dev"
```

That's it — the defaults work fine for most people. You don't need to configure a bucket name or any IAM ARNs. Terraform automatically detects your AWS account ID from the `terraform-admin` profile and uses it to:

- Name the bucket `skytrax-reviews-landing-<your_account_id>` (guaranteed unique since account IDs are unique)
- Set up the IAM trust policy so your account can assume the Airflow role

The `terraform.tfvars` file is gitignored so you don't accidentally commit any sensitive info.

## 5.3 Plan and Apply

Now let's provision everything. `cd` into the terraform directory and run:

```
cd terraform/aws
terraform init
terraform plan
```

`terraform plan` will show you exactly what resources are going to be created — review it and make sure everything looks right. You should see 11 resources being created.

Once you're happy with the plan:

```
terraform apply
```

Type `yes` when prompted. After it finishes, Terraform will output some important values:

```
Outputs:

bucket_name               = "skytrax-reviews-landing-XXXXXXXXXXXX"
bucket_arn                = "arn:aws:s3:::skytrax-reviews-landing-XXXXXXXXXXXX"
snowflake_s3_role_arn     = "arn:aws:iam::XXXXXXXXXXXX:role/skytrax-snowflake-s3-dev"
airflow_access_key_id     = "AKIAXXXXXXXXXXXXXXXX"
airflow_secret_access_key = <sensitive>
```

**Important:** The `airflow_secret_access_key` is marked as sensitive, so Terraform won't show it directly. To retrieve it:

```
terraform output -raw airflow_secret_access_key
```

Save both the `airflow_access_key_id` and `airflow_secret_access_key` somewhere secure — we will need them in Step 7 when we set up the Airflow AWS connection.

## 5.4 Verify in AWS Console

If you want to double check, you can go to the AWS console and verify:

- S3 → you should see your bucket with versioning enabled
- IAM → you should see the `skytrax-snowflake-s3-dev` role and `skytrax-airflow-dev` user

## 5.5 Initial data load to S3

If you already scraped and processed data locally in Step 2, you can do an initial sync to upload everything from your local `landing/` directory to S3 using the AWS CLI:

```bash
aws s3 sync landing/ s3://skytrax-reviews-landing-<your_account_id>/ --profile terraform-admin --exclude ".gitkeep"
```

This will upload all your `raw/` and `processed/` CSVs to the matching S3 paths. After this, your S3 bucket should mirror the same directory structure as your local `landing/` dir.

That's it for the AWS setup. All managed by Terraform — if you ever need to tear everything down, just run `terraform destroy` and it's all gone. Clean and reproducible.

# Step 6: Set up Staging area between AWS and Snowflake

Now here's where it gets interesting. We need to connect Snowflake to our S3 bucket so that Snowflake can read data from it. This is done through an **external stage** — basically telling Snowflake "hey, here's an S3 path and an IAM role you can use to access it."

The Snowflake Terraform module lives in `terraform/snowflake/` and creates:
- **Database** (`SKYTRAX_REVIEWS_DB`)
- **Schema** (`RAW`)
- **Table** (`AIRLINE_REVIEWS`) — column order matches our processed CSV output
- **External Stage** (`SKYTRAX_S3_STAGE`) — points to our S3 bucket, uses the `skytrax-snowflake-s3-dev` IAM role so Snowflake can assume it and read from S3

## 6.1 Configure Snowflake variables

First, grab the outputs from the AWS module — you'll need `bucket_name` and `snowflake_s3_role_arn`:

```bash
cd terraform/aws
terraform output
```

Then copy and fill in the Snowflake tfvars:

```bash
cp terraform/snowflake/terraform.tfvars.example terraform/snowflake/terraform.tfvars
```

Edit `terraform/snowflake/terraform.tfvars` with your Snowflake credentials and the AWS outputs:

```hcl
snowflake_org            = "MYORG"
snowflake_account        = "MYACCOUNT"
snowflake_admin_user     = "your_username"
snowflake_admin_password = "your_password"

bucket_name      = "skytrax-reviews-landing-XXXXXXXXXXXX"
snowflake_s3_role_arn = "arn:aws:iam::XXXXXXXXXXXX:role/skytrax-snowflake-s3-dev"
```

You can find your `snowflake_org` and `snowflake_account` from your Snowflake account URL — it's in the format `https://MYORG-MYACCOUNT.snowflakecomputing.com`.

## 6.2 Apply the Snowflake module

```bash
cd terraform/snowflake
terraform init
terraform plan
terraform apply
```

This creates the database, schema, table, and external stage in Snowflake.

## 6.3 Get the Snowflake IAM user ARN (important!)

Here's the tricky part. When you create a stage with `AWS_ROLE`, Snowflake doesn't create anything in *your* AWS account. What actually happens is Snowflake already has its own internal AWS account with its own IAM users. It assigns one of those internal users to your stage — so when Snowflake runs `COPY INTO`, that internal user calls `sts:AssumeRole` on your `skytrax-snowflake-s3-dev` role to get temporary credentials, and uses those to read from your S3 bucket.

The problem is: by default, your role doesn't trust some random user from Snowflake's AWS account. So we need to grab that user's ARN and add it to the role's trust policy — basically telling AWS "hey, this external user from Snowflake is allowed to assume this role."

Run this in Snowflake (via the Snowflake UI or `snowsql`):

```sql
DESC STAGE SKYTRAX_REVIEWS_DB.RAW.SKYTRAX_S3_STAGE;
```

Look for `STORAGE_AWS_IAM_USER_ARN` in the output — it'll look something like `arn:aws:iam::612990353424:user/651j1000-s`. Copy that value.

## 6.4 Update AWS trust policy with the Snowflake ARN

Now go back to the AWS module and add the Snowflake ARN to your `terraform/aws/terraform.tfvars`:

```hcl
aws_region  = "us-east-1"
environment = "dev"

snowflake_iam_user_arn = "arn:aws:iam::612990353424:user/651j1000-s"
```

Why do we need to apply the AWS module again? Right now, the `skytrax-snowflake-s3-dev` role's trust policy only trusts your own AWS account. Snowflake's internal user lives in a completely different AWS account — so when it tries to call `sts:AssumeRole`, AWS rejects it with "I don't know you." By adding the `snowflake_iam_user_arn` to your tfvars, Terraform updates the trust policy to say "I trust both my own account AND this Snowflake user." That's the two-pass setup — you can't do it in one shot because you don't know Snowflake's IAM user ARN until after you create the stage.

Re-apply the AWS module:

```bash
cd terraform/aws
terraform apply
```

This updates the trust policy. Without this step, you'll get the error: `User is not authorized to perform: sts:AssumeRole`.

You can verify the connection works by running a quick test in Snowflake:

```sql
LIST @SKYTRAX_REVIEWS_DB.RAW.SKYTRAX_S3_STAGE/processed/;
```

If you see your files listed, the staging area is set up correctly.

# Step 7: Set up Airflow connections with AWS and Snowflake

Before we wire everything up, let's be clear about the two different authentication flows in this pipeline — because they use different IAM resources and it's easy to mix them up:

**Airflow → S3 (user-level auth via access keys)**

Airflow uploads and downloads files to/from S3 using an **IAM User** (`skytrax-airflow-dev`). Terraform creates this user with an access key pair (`access_key_id` + `secret_access_key`). We put those credentials in the Airflow `.env` file as the `AIRFLOW_CONN_AWS_S3_CONNECTION`. This is direct, user-level authentication — Airflow sends the access key with every S3 API call. No role assumption involved.

**S3 → Snowflake (role-level auth via `sts:AssumeRole`)**

Snowflake reads files from S3 during `COPY INTO` using an **IAM Role** (`skytrax-snowflake-s3-dev`). When we created the external stage in Step 6, we gave Snowflake the role ARN. Under the hood, Snowflake has its own internal AWS account — it calls `sts:AssumeRole` on our role to get temporary credentials, then uses those to read from our S3 bucket. That's why we needed the two-pass setup: first create the role, then tell AWS to trust Snowflake's IAM user in the role's trust policy.

**Why two different approaches?** The IAM user gives Airflow long-lived credentials that work in a Docker container with no AWS metadata service. The IAM role gives Snowflake temporary, scoped credentials without us ever sharing secrets — Snowflake never sees an access key, it just assumes the role.

Now let's set up the actual connections. Airflow connections are configured through environment variables in a `.env` file — no clicking around the Airflow UI needed.

## 7.1 Install Astro CLI and start Airflow

If you haven't already, install the [Astro CLI](https://www.astronomer.io/docs/astro/cli/install-cli). Then build the Astronomer Docker image:

```bash
make dev-setup
```

This builds the Docker image and starts Airflow locally. The Airflow UI will be available at `http://localhost:8080`.

## 7.2 Create the `.env` file

We already created a `.env` file in Step 2 for local development. Now we need to update it with our AWS and Snowflake credentials so Airflow can actually talk to S3 and Snowflake.

Open your `.env` file and update it to:

```bash
STORAGE_MODE=s3
S3_BUCKET=skytrax-reviews-landing-XXXXXXXXXXXX
AIRFLOW_CONN_AWS_S3_CONNECTION=aws://<ACCESS_KEY_ID>:<URL_ENCODED_SECRET>@/?region_name=us-east-1
AIRFLOW_CONN_SNOWFLAKE_DEFAULT='{"conn_type":"snowflake","login":"<SNOWFLAKE_USER>","password":"<SNOWFLAKE_PASSWORD>","schema":"RAW","extra":{"account":"<ORG>-<ACCOUNT>","database":"SKYTRAX_REVIEWS_DB","warehouse":"COMPUTE_WH","role":"SYSADMIN"}}'
```

Let me break down what each variable does:

- **`STORAGE_MODE=s3`** — tells the pipeline to use S3 instead of local storage. When this was `local`, the DAGs skipped S3/Snowflake entirely
- **`S3_BUCKET`** — your bucket name from `terraform output bucket_name`
- **`AIRFLOW_CONN_AWS_S3_CONNECTION`** — the AWS connection in URI format. Airflow reads this env var and auto-registers it as a connection with ID `aws_s3_connection`
- **`AIRFLOW_CONN_SNOWFLAKE_DEFAULT`** — the Snowflake connection in JSON format. Must be wrapped in single quotes. Airflow registers this as `snowflake_default`

### Where to get the values

| Variable | How to get it |
| -------- | ------------- |
| `S3_BUCKET` | `cd terraform/aws && terraform output bucket_name` |
| `ACCESS_KEY_ID` | `cd terraform/aws && terraform output airflow_access_key_id` |
| `URL_ENCODED_SECRET` | `cd terraform/aws && terraform output -raw airflow_secret_access_key` — then URL-encode special characters |
| `SNOWFLAKE_USER` | Your Snowflake username |
| `SNOWFLAKE_PASSWORD` | Your Snowflake password |
| `ORG-ACCOUNT` | Your Snowflake account identifier (e.g. `nvnjoib-on80344`) |

### URL-encoding the AWS secret key

This is important — if your secret key contains special characters like `+`, `/`, `=`, or `@`, you need to URL-encode them or the connection will fail with `SignatureDoesNotMatch`:

| Character | Encoded |
| --------- | ------- |
| `+` | `%2B` |
| `/` | `%2F` |
| `=` | `%3D` |
| `@` | `%40` |

For example: `GSstxwFTvbwTIvIczSpGZLv810qLwLG+EpaVi5St` becomes `GSstxwFTvbwTIvIczSpGZLv810qLwLG%2BEpaVi5St`

> **Heads up — zsh `%` gotcha:** If you use `terraform output -raw` to grab the secret key, zsh will print a `%` at the end of output that doesn't end with a newline. That `%` is **not** part of your key — it's just zsh telling you there's no trailing newline. Don't include it.

## 7.3 Test the connection before restarting

Before restarting Airflow, verify your credentials work by running a quick S3 list from your terminal. Use the **raw** (non-URL-encoded) secret key here — this is a direct AWS CLI call, not an Airflow URI:

```bash
AWS_ACCESS_KEY_ID=<ACCESS_KEY_ID> AWS_SECRET_ACCESS_KEY='<SECRET_KEY>' aws s3 ls s3://<BUCKET_NAME>/ --region us-east-1
```

You should see something like:

```
PRE processed/
PRE raw/
```

If you see your prefixes listed, your credentials are valid. If you get `SignatureDoesNotMatch`, the secret key is wrong — go back to the IAM console and double-check it (and remember: that trailing `%` from `terraform output -raw` is not part of the key).

Once the CLI test passes, you know any future `SignatureDoesNotMatch` in Airflow is a URL-encoding issue in the `.env`, not a bad key.

## 7.4 Restart Airflow

After updating `.env`, restart Airflow so it picks up the new connections:

```bash
astro dev restart
```

You can verify the connections are registered by going to the Airflow UI → Admin → Connections. You should see `aws_s3_connection` and `snowflake_default`.

# Step 8: Set up COPY INTO via DAG

Now everything is wired up — S3 bucket, Snowflake stage, Airflow connections. Time to run the full pipeline.

## 8.1 How the DAGs work

The pipeline consists of 3 DAGs chained together via **Airflow Datasets**:

1. **`skytrax_crawl`** (Extract) — scrapes reviews from airlinequality.com, splits by review date, uploads raw CSVs to S3. Emits the `skytrax://raw` dataset when done
2. **`skytrax_process`** (Transform) — triggered automatically when new raw data lands. Downloads the raw CSV, runs the cleaning pipeline, uploads processed CSV to S3. Emits `skytrax://processed`
3. **`skytrax_snowflake`** (Load) — triggered automatically when processed data is ready. Runs `COPY INTO` for each review date to load into Snowflake

The key thing here is that these DAGs are **event-driven**. You don't need to schedule them separately — when `skytrax_crawl` finishes, it automatically triggers `skytrax_process`, which then triggers `skytrax_snowflake`. Clean chain.

## 8.2 The COPY INTO logic

The actual loading happens in `include/tasks/load/snowflake_load.py`. For each review date, it runs this SQL template (`include/sql/copy_into.sql`):

```sql
COPY INTO SKYTRAX_REVIEWS_DB.RAW.AIRLINE_REVIEWS
FROM @SKYTRAX_REVIEWS_DB.RAW.SKYTRAX_S3_STAGE/{{ s3_key }}
ON_ERROR = 'CONTINUE'
PURGE    = FALSE;
```

Where `{{ s3_key }}` gets replaced with the actual S3 path like `processed/2026/03/clean_data_20260312.csv`. A few things to note:

- **`ON_ERROR = 'CONTINUE'`** — keeps loading even if some rows fail, so one bad row doesn't block the whole file
- **`PURGE = FALSE`** — doesn't delete the source file after loading, so you can always re-run
- Snowflake internally tracks which files have been loaded, so running the same `COPY INTO` twice won't create duplicates

The `skytrax_snowflake` DAG uses **dynamic task mapping** — it creates one `load_date` task per review date. So if the crawler found reviews for 5 different dates, you'll see 5 parallel load tasks in the Airflow UI.

## 8.3 Run the full pipeline

### Daily incremental run

The `skytrax_crawl` DAG is scheduled to run daily. It scrapes yesterday's reviews, and the downstream DAGs trigger automatically via Datasets.

To trigger manually: click the play button on `skytrax_crawl` in the Airflow UI.

### Full initial load

For the first time, you want to load all historical reviews:

1. Go to the `skytrax_crawl` DAG in the Airflow UI
2. Click **Trigger DAG w/ config**
3. Set `full_scrape` to `true`
4. Click **Trigger**

This scrapes all historical reviews going back to 2002. The downstream `skytrax_process` and `skytrax_snowflake` DAGs trigger automatically and load everything into Snowflake.

## 8.4 Verify the data in Snowflake

Once the pipeline finishes, verify the data landed:

```sql
SELECT COUNT(*) FROM SKYTRAX_REVIEWS_DB.RAW.AIRLINE_REVIEWS;
-- Should see 160,000+ rows

SELECT * FROM SKYTRAX_REVIEWS_DB.RAW.AIRLINE_REVIEWS LIMIT 10;
```

You can also check S3 to make sure files are there:

```bash
aws s3 ls s3://skytrax-reviews-landing-<your_account_id>/processed/ --recursive | head -20
```

And that's it! You now have a fully working ingestion pipeline — scraping data from the web, staging it in S3, and loading it into Snowflake. All orchestrated by Airflow, all infrastructure managed by Terraform. In [part 2](https://github.com/MarkPhamm/skytrax_reviews_transformation), we'll transform this raw data using `dbt` into analytical models.
