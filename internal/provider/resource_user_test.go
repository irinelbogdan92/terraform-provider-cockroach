package provider

import (
	"regexp"
	"testing"

	"github.com/hashicorp/terraform-plugin-sdk/v2/helper/resource"
)

func TestAccResourceUser(t *testing.T) {
	t.Skip("resource not yet implemented, remove this once you add your own code")

	resource.UnitTest(t, resource.TestCase{
		PreCheck:          func() { testAccPreCheck(t) },
		ProviderFactories: providerFactories,
		Steps: []resource.TestStep{
			{
				Config: testAccResourceUser,
				Check: resource.ComposeTestCheckFunc(
					resource.TestMatchResourceAttr(
						"cockroach_user.foo", "username", regexp.MustCompile("^bar$")),
				),
			},
		},
	})
}

const testAccResourceUser = `
resource "cockroach_user" "foo" {
  username = "bar"
  password = "bar123"
  roles = ""
  is_admin = true
  local_port = 23244
}
`
