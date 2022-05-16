package provider

import (
	"regexp"
	"testing"

	"github.com/hashicorp/terraform-plugin-sdk/v2/helper/resource"
)

func TestAccDataSourceDatabase(t *testing.T) {
	t.Skip("data source not yet implemented, remove this once you add your own code")

	resource.UnitTest(t, resource.TestCase{
		PreCheck:          func() { testAccPreCheck(t) },
		ProviderFactories: providerFactories,
		Steps: []resource.TestStep{
			{
				Config: testAccDataSourceDatabase,
				Check: resource.ComposeTestCheckFunc(
					resource.TestMatchResourceAttr(
						"data.cockroach_database.foo", "name", regexp.MustCompile("^bar$")),
				),
			},
		},
	})
}

const testAccDataSourceDatabase = `
data "cockroach_database" "foo" {
  name = "bar"
}
`
