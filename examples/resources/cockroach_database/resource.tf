resource "cockroach_user" "example" {
  username   = "example_user"
  password   = "example_password"
  roles      = "SQLLOGIN VIEWACTIVITY (other roles)"
  is_admin   = false
  local_port = "26257"
}

resource "cockroach_database" "example" {
  name       = "example_database"
  owner      = cockroach_user.example.username
  encoding   = "utf-8"
  local_port = "26258"
}
