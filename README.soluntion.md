## Data exploration
---

I have explained all the exploration on the **data-exploration.ipynb** notebook and its markdown version.

## How to execute and deploy
---

I have tested this project locally and on an EMR cluster (version 5.30.1)

To test locally, you only have to use the Makefile on the root folder.

If you want to deploy to the same infrastructure that I have used, you can use the Terraform on the root folder.

## Config handling
---

This solution accepts two ways of configurating its parameters: a local INI file and a parameter path from AWS Parameter Store.

It is currently configurable the source and destination path for the data, and for task 2, it also possible to configure which ingredients should be used to create the average.

## Alerting
---

This solution does not have any alert on by default, but if you can turn it on by uncommenting and editing the **send_slack_notification_on_validation_result** section of great_expectations.yml
Also, a tool like Apache Airflow can catch the application errors, and alert the developers by e-mail, Slack, PagerDuty, etc.

## CI/CD explained
---

To create a CI/CD pipeline of this project, you can install the requirements.txt on a Python container and execute its unit tests. 
Then you can use the **make deploy** command to send the spark_jobs package, great_expectations folder and the setup.py to AWS S3, where you can use an EMR cluster to execute the required jobs.
A way to schedule jobs is to use Apache Airflow and its operators to call the Terraform scripts to create the cluster and add execution Steps after it becomes available.

## Considerations
---

Great Expectations is a brand-new tool to validate data on the ETL pipeline. As such, it can hurt the application performance is data grows larger. But if performance can be compromised a bit for data quality check, it can be a good way to impose and track business logic.
