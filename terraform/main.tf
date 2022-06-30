resource "aws_emr_cluster" "spark_cluster" {
  name          = var.cluster_name
  release_label = "emr-5.30.1"

  applications = ["Hadoop", "Tez", "Spark", "Hive"]

  keep_job_flow_alive_when_no_steps = true
  termination_protection            = true

  service_role = var.emr_service_role

  master_instance_group {
    instance_type = "r5.xlarge"

    ebs_config {
      size                 = "100"
      type                 = "gp2"
      volumes_per_instance = 1
    }
  }

  core_instance_group {
    instance_type  = "r5.2xlarge"
    instance_count = 1

    ebs_config {
      size                 = "300"
      type                 = "gp2"
      volumes_per_instance = 1
    }
  }

  ec2_attributes {
    subnet_id        = var.subnet_id
    instance_profile = var.ec2_service_role
    key_name         = var.ssh_key_name
  }

  ebs_root_volume_size = 100
}

provider "aws" {
  region = "us-east-1"
}

