package provider

import (
	"context"
	"strconv"
	"strings"

	"github.com/hashicorp/terraform-plugin-sdk/v2/diag"
	"github.com/hashicorp/terraform-plugin-sdk/v2/helper/schema"
	"github.com/jackc/pgx/v4"
)

func dataSourceDatabase() *schema.Resource {
	return &schema.Resource{
		// This description is used by the documentation generator and the language server.
		Description: "Sample data source in the Terraform provider scaffolding.",

		ReadContext: dataSourceDatabaseRead,

		Schema: map[string]*schema.Schema{
			dbNameAttr: {
				// This description is used by the documentation generator and the language server.
				Description: "Name of the database.",
				Type:        schema.TypeString,
				Required:    true,
			},
			dbOwnerAttr: {
				Description: "Owner of the database.",
				Type:        schema.TypeString,
				Optional:    true,
				Computed:    true,
			},
			argLocalPort: {
				Description: "Local port to be used for port-forward. (default is 26259), use different port to avoid same port opening.",
				Type:        schema.TypeString,
				Optional:    true,
				Default:     "26259",
			},
		},
	}
}

func dataSourceDatabaseRead(ctx context.Context, d *schema.ResourceData, meta interface{}) diag.Diagnostics {
	cockroachClient := meta.(*cockroachClient)

	local_port := d.Get(argLocalPort).(string)
	dns := strings.Replace(cockroachClient.dns, "<local_port>", local_port, 1)

	// stopCh control the port forwarding lifecycle. When it gets closed the
	// port forward will terminate
	stopCh := make(chan struct{}, 1)
	// readyCh communicate when the port forward is ready to get traffic
	readyCh := make(chan struct{})

	tryPortForwardIfNeeded(ctx, d, meta, stopCh, readyCh, local_port)

	conn, err := pgx.Connect(ctx, dns)

	if err := conn.Ping(ctx); err != nil {
		return diag.FromErr(err)
	}
	name := d.Get("name").(string)
	var (
		id    int
		owner string
	)
	err = conn.QueryRow(ctx, `SELECT id, owner FROM crdb_internal.databases WHERE name = $1`, name).Scan(
		&id,
		&owner,
	)
	if err != nil {
		return diag.FromErr(err)
	}
	d.SetId(strconv.Itoa(id))
	if err := d.Set(dbNameAttr, name); err != nil {
		return diag.FromErr(err)
	}
	if err := d.Set(dbOwnerAttr, owner); err != nil {
		return diag.FromErr(err)
	}

	close(stopCh)

	return diag.Diagnostics{}
}
