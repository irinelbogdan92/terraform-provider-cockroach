terraform {
  required_providers {
    cockroach = {
      source  = "hashicorp.com/aybabtme/cockroach"
      version = "0.1"
    }
  }
}

provider "cockroach" {
  dsn = "postgresql://root@localhost:26257?sslmode=disable"
}

resource "cockroach_database" "helloworld" {
  name = "helloworld"
}

data "cockroach_database" "name" {
  name = cockroach_database.helloworld.name
}
