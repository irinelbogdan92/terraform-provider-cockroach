terraform {
  required_providers {
    cockroach = {
      source = "irinelbogdan92/cockroach"
    }
  }
}

provider "cockroach" {
  username = "username"
  password = "password"
  # Configuration options
  kube_config {
    kube_config_path = "~/.kubeconfig"
    namespace        = "namespace"
    service_name     = "service_name_for_cockroach"
    remote_port      = "26257"
  }
}
