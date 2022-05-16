package provider

import (
	"regexp"
	"testing"

	"github.com/hashicorp/terraform-plugin-sdk/v2/helper/resource"
)

func TestAccResourceDatabaseBackup(t *testing.T) {
	t.Skip("resource not yet implemented, remove this once you add your own code")

	resource.UnitTest(t, resource.TestCase{
		PreCheck:          func() { testAccPreCheck(t) },
		ProviderFactories: providerFactories,
		Steps: []resource.TestStep{
			{
				Config: testAccResourceDatabaseBackup,
				Check: resource.ComposeTestCheckFunc(
					resource.TestMatchResourceAttr(
						"cockroach_database_backup.foo", "name", regexp.MustCompile("^scheduller$")),
				),
			},
		},
	})
}

const testAccResourceDatabaseBackup = `
resource "cockroach_database_backup" "foo" {
  name = "scheduller"
  backup_path = "nodelocal://test"
  database_name = "test"
  local_port = "23455"
}
`
