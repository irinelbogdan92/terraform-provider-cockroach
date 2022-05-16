resource "cockroach_user" "example" {
  username   = "example_user"
  password   = "example_password"
  roles      = "SQLLOGIN VIEWACTIVITY (other roles)"
  is_admin   = false
  local_port = "26257"
}
