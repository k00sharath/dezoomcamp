terraform {
  required_providers {
    aws = {
      source = "hashicorp/aws"
      version = "5.0.0"
    }
  }
}

provider "aws" {
  region = "us-east-1"
  profile = "default"
}

resource "aws_s3_bucket" "dezoomcampbucket"{

bucket = "debucketnew1"

}
