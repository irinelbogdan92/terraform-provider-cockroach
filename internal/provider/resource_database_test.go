package provider

import (
	"regexp"
	"testing"

	"github.com/hashicorp/terraform-plugin-sdk/v2/helper/resource"
)

func TestAccResourceDatabase(t *testing.T) {
	t.Skip("resource not yet implemented, remove this once you add your own code")

	resource.UnitTest(t, resource.TestCase{
		PreCheck:          func() { testAccPreCheck(t) },
		ProviderFactories: providerFactories,
		Steps: []resource.TestStep{
			{
				Config: testAccResourceDatabase,
				Check: resource.ComposeTestCheckFunc(
					resource.TestMatchResourceAttr(
						"cockroach_database.foo", "name", regexp.MustCompile("^bar$")),
				),
			},
		},
	})
}

const testAccResourceDatabase = `
resource "cockroach_database" "foo" {
  name = "bar"
}
`
