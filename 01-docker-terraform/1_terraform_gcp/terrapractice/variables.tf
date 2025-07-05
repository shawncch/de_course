variable "creds" {
  description = "path to credentials json file"
  default     = "./keys/my-creds.json"
}

variable "project_id" {
  description = "project id"
  default     = "dtc-de-course-464416"
}

variable "region" {
  description = "region for resource"
  default     = "asia-southeast1"
}

variable "location" {
  description = "location for resource"
  default     = "asia-southeast1"
}

variable "gsb_name" {
  description = "name for google storage bucket"
  default     = "dtc-de-course-464416-demo-bucket"
}

variable "dataset_id" {
  description = "dataset id for bq dataset"
  default     = "demo_dataset"
}
