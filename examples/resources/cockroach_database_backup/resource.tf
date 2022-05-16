resource "cockroach_database" "example" {
  name       = "footest"
  owner      = cockroach_user.example.username
  encoding   = "utf-8"
  local_port = "26258"
}

resource "cockroach_database_backup" "example" {
  name             = "scheduler_foo123"
  backup_path      = "nodelocal://0/foo"
  database_name    = cockroach_database.example.name
  backup_recurring = "@daily"
  local_port       = "26259"
}
