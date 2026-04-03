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

1. **IAM Role** (`skytrax-airflow-dev`) — the role Airflow assumes to access S3. The trust policy is auto-configured to allow your AWS account to assume it
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
airflow_role_arn          = "arn:aws:iam::XXXXXXXXXXXX:role/skytrax-airflow-dev"
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
- IAM → you should see the `skytrax-airflow-dev` role and user

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
- **External Stage** (`SKYTRAX_S3_STAGE`) — points to our S3 bucket, uses the Airflow IAM role to authenticate

## 6.1 Configure Snowflake variables

First, grab the outputs from the AWS module — you'll need `bucket_name` and `airflow_role_arn`:

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
airflow_role_arn = "arn:aws:iam::XXXXXXXXXXXX:role/skytrax-airflow-dev"
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

Here's the tricky part. When Snowflake creates the external stage with `AWS_ROLE`, it uses its own internal AWS IAM user to assume your role. We need to trust that user in our IAM role's trust policy.

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

Then re-apply the AWS module:

```bash
cd terraform/aws
terraform apply
```

This updates the IAM role's trust policy to allow Snowflake to assume it. Without this step, you'll get the error: `User is not authorized to perform: sts:AssumeRole`.

You can verify the connection works by running a quick test in Snowflake:

```sql
LIST @SKYTRAX_REVIEWS_DB.RAW.SKYTRAX_S3_STAGE/processed/;
```

If you see your files listed, the staging area is set up correctly.

# Step 7: Set up Airflow connections with AWS and Snowflake

# Step 8: Set up COPY INTO via dag
