package provider

import (
	"strconv"

	"github.com/jackc/pgx/v4"
	"github.com/lib/pq"

	"context"
	"database/sql"
	"github.com/hashicorp/terraform-plugin-sdk/v2/diag"
	"github.com/hashicorp/terraform-plugin-sdk/v2/helper/schema"
	"strings"
)

const (
	dbNameAttr          = "name"
	dbOwnerAttr         = "owner"
	dbOptionsAttr       = "options"
	dbEncodingAttr      = "encoding"
	dbPrimaryRegionAttr = "primary_region"
	dbRegionsAttr       = "regions"
)

func resourceDatabase() *schema.Resource {
	return &schema.Resource{
		// This description is used by the documentation generator and the language server.
		Description: "Resource used to create a new database in a CockroachDB cluster.",

		CreateContext: resourceDatabaseCreate,
		ReadContext:   resourceDatabaseRead,
		UpdateContext: resourceDatabaseUpdate,
		DeleteContext: resourceDatabaseDelete,
		Importer: &schema.ResourceImporter{
			StateContext: resourceDatabaseImporter,
		},

		Schema: map[string]*schema.Schema{
			dbNameAttr: {
				Description: "Name of the database.",
				Type:        schema.TypeString,
				Required:    true,
			},
			dbOwnerAttr: {
				Description: "Owner of the database.",
				Type:        schema.TypeString,
				Optional:    true,
				Default:     "",
			},
			dbEncodingAttr: {
				Description: "Encoding to set to the database. (Optional argument, do not specify if not required)",
				Type:        schema.TypeString,
				Optional:    true,
				Default:     "",
			},
			dbPrimaryRegionAttr: {
				Description: "Primary region of the database. (Optional argument, do not specify if not required)",
				Type:        schema.TypeString,
				Optional:    true,
				Default:     "",
			},
			dbRegionsAttr: {
				Description: "Regions where the database is created. (Optional argument, do not specify if not required)",
				Type:        schema.TypeList,
				Elem: &schema.Schema{
					Type: schema.TypeString,
				},
				Optional: true,
			},
			argLocalPort: {
				Description: "Local port to be used for port-forward. (default is 26258), use different port to avoid same port opening.",
				Type:        schema.TypeString,
				Optional:    true,
				Default:     "26258",
			},
		},
	}
}

func resourceDatabaseCreate(ctx context.Context, d *schema.ResourceData, meta interface{}) diag.Diagnostics {
	cockroachClient := meta.(*cockroachClient)

	local_port := d.Get(argLocalPort).(string)
	name := d.Get(dbNameAttr).(string)
	owner := d.Get(dbOwnerAttr).(string)
	encoding := d.Get(dbEncodingAttr).(string)
	primary_region := d.Get(dbPrimaryRegionAttr).(string)
	regions := convertToString(d.Get(dbRegionsAttr).([]interface{}))
	dns := strings.Replace(cockroachClient.dns, "<local_port>", local_port, 1)

	// stopCh control the port forwarding lifecycle. When it gets closed the
	// port forward will terminate
	stopCh := make(chan struct{}, 1)
	// readyCh communicate when the port forward is ready to get traffic
	readyCh := make(chan struct{})

	set_encoding := ""
	set_primary_region := ""
	set_regions := ""

	if name == "" {
		return diag.Errorf("database name can't be an empty string")
	}

	if encoding != "" {
		set_encoding = "ENCODING " + pq.QuoteIdentifier(encoding)
	}

	if primary_region != "" {
		set_primary_region = "PRIMARY REGION " + pq.QuoteIdentifier(primary_region)
	}

	if len(regions) != 0 {
		set_regions = "REGIONS " + pq.QuoteIdentifier(strings.Join(regions, ""))
	}

	tryPortForwardIfNeeded(ctx, d, meta, stopCh, readyCh, local_port)

	conn, err := pgx.Connect(ctx, dns)

	if err != nil {
		return diag.FromErr(err)
	}

	if err := conn.Ping(ctx); err != nil {
		return diag.FromErr(err)
	}

	_, err = conn.Exec(ctx,
		`CREATE DATABASE `+
			pq.QuoteIdentifier(name)+
			` `+
			set_encoding+
			` `+
			set_primary_region+
			` `+
			set_regions,
	)
	if err != nil {
		return diag.FromErr(err)
	}

	_, err = conn.Exec(ctx,
		`ALTER DATABASE `+
			pq.QuoteIdentifier(name)+
			` OWNER TO `+
			pq.QuoteIdentifier(owner),
	)
	if err != nil {
		return diag.FromErr(err)
	}

	var id int
	err = conn.QueryRow(ctx, `SELECT id FROM crdb_internal.databases WHERE name = $1`, name).Scan(
		&id,
	)
	if err != nil {
		return diag.FromErr(err)
	}

	d.SetId(strconv.Itoa(id))
	d.Set(dbNameAttr, name)
	d.Set(dbOwnerAttr, owner)
	d.Set(dbEncodingAttr, encoding)
	d.Set(dbPrimaryRegionAttr, primary_region)
	d.Set(dbRegionsAttr, regions)

	close(stopCh)

	return diag.Diagnostics{}
}

func resourceDatabaseRead(ctx context.Context, d *schema.ResourceData, meta interface{}) diag.Diagnostics {
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

	if err != nil {
		return diag.FromErr(err)
	}

	if err := conn.Ping(ctx); err != nil {
		return diag.FromErr(err)
	}

	name := d.Get(dbNameAttr).(string)

	rows, err := conn.Query(ctx, "SELECT name AS database_name, owner, primary_region, regions, survival_goal FROM crdb_internal.databases")
	if err != nil {
		// handle this error better than this
		return diag.FromErr(err)
	}
	found := false
	defer rows.Close()

	// database_name |     owner     | primary_region | regions | survival_goal
	for rows.Next() {
		var (
			database_name    string
			owner            string
			primary_region   string
			primary_region_n sql.NullString
			regions          []string
			survival_goal    sql.NullString
		)
		err = rows.Scan(&database_name, &owner, &primary_region_n, &regions, &survival_goal)
		if err != nil {
			// handle this error
			return diag.FromErr(err)
		}

		if primary_region_n.Valid {
			primary_region = primary_region_n.String
		}

		if database_name == name {
			// TODO: find a way to read all the roles
			// if err := d.Set(dbRolesAttr, options); err != nil {
			// 	return diag.FromErr(err)
			// }

			if err := d.Set(dbOwnerAttr, owner); err != nil {
				return diag.FromErr(err)
			}

			if err := d.Set(dbPrimaryRegionAttr, primary_region); err != nil {
				return diag.FromErr(err)
			}

			if err := d.Set(dbRegionsAttr, regions); err != nil {
				return diag.FromErr(err)
			}

			found = true
			break
		}
	}
	// get any error encountered during iteration
	err = rows.Err()
	if err != nil {
		return diag.FromErr(err)
	}

	close(stopCh)
	if found == false {
		return diag.Errorf("Cannot find database with name: " + name)
	}

	return diag.Diagnostics{}
}

func resourceDatabaseUpdate(ctx context.Context, d *schema.ResourceData, meta interface{}) diag.Diagnostics {
	cockroachClient := meta.(*cockroachClient)

	local_port := d.Get(argLocalPort).(string)
	dns := strings.Replace(cockroachClient.dns, "<local_port>", local_port, 1)

	d.Partial(true)

	// stopCh control the port forwarding lifecycle. When it gets closed the
	// port forward will terminate
	stopCh := make(chan struct{}, 1)
	// readyCh communicate when the port forward is ready to get traffic
	readyCh := make(chan struct{})

	tryPortForwardIfNeeded(ctx, d, meta, stopCh, readyCh, local_port)

	conn, err := pgx.Connect(ctx, dns)

	if err != nil {
		return diag.FromErr(err)
	}

	if err := conn.Ping(ctx); err != nil {
		return diag.FromErr(err)
	}

	if d.HasChange(dbNameAttr) {
		oraw, nraw := d.GetChange(dbNameAttr)
		o := oraw.(string)
		n := nraw.(string)
		if n == "" {
			return diag.Errorf("database name can't be an empty string")
		}
		_, err := conn.Exec(ctx,
			`ALTER DATABASE `+
				pq.QuoteIdentifier(o)+
				` RENAME TO `+
				pq.QuoteIdentifier(n),
		)
		if err != nil {
			return diag.FromErr(err)
		}
		d.Set(dbNameAttr, n)
	}

	if d.HasChange(dbOwnerAttr) {
		name := d.Get(dbNameAttr).(string)
		_, nraw := d.GetChange(dbOwnerAttr)
		// o := oraw.(string)
		n := nraw.(string)

		_, err = conn.Exec(ctx,
			`ALTER DATABASE `+
				pq.QuoteIdentifier(name)+
				` OWNER TO `+
				pq.QuoteIdentifier(n),
		)
		if err != nil {
			return diag.FromErr(err)
		}
	}

	if d.HasChange(dbPrimaryRegionAttr) {
		name := d.Get(dbNameAttr).(string)
		_, nraw := d.GetChange(dbPrimaryRegionAttr)
		// o := oraw.(string)
		n := nraw.(string)

		_, err = conn.Exec(ctx,
			`ALTER DATABASE `+
				pq.QuoteIdentifier(name)+
				` SET PRIMARY REGION `+
				pq.QuoteIdentifier(n),
		)
		if err != nil {
			return diag.FromErr(err)
		}
	}

	if d.HasChange(dbRegionsAttr) {
		name := d.Get(dbNameAttr).(string)
		oraw, nraw := d.GetChange(dbRegionsAttr)
		o := convertToString(oraw.([]interface{}))
		n := convertToString(nraw.([]interface{}))

		// drop unused regions
		for _, region := range o {
			if !contains(n, region) {
				_, err = conn.Exec(ctx,
					`ALTER DATABASE `+
						pq.QuoteIdentifier(name)+
						` DROP REGION `+
						pq.QuoteIdentifier(region),
				)
				if err != nil {
					return diag.FromErr(err)
				}
			}
		}

		// create new regions
		for _, region := range n {
			if !contains(o, region) {
				_, err = conn.Exec(ctx,
					`ALTER DATABASE `+
						pq.QuoteIdentifier(name)+
						` ADD REGION `+
						pq.QuoteIdentifier(region),
				)
				if err != nil {
					return diag.FromErr(err)
				}
			}
		}
	}

	d.Partial(false)
	close(stopCh)
	return diag.Diagnostics{}
}

func resourceDatabaseDelete(ctx context.Context, d *schema.ResourceData, meta interface{}) diag.Diagnostics {
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

	if err != nil {
		return diag.FromErr(err)
	}

	if err := conn.Ping(ctx); err != nil {
		return diag.FromErr(err)
	}
	name := d.Get(dbNameAttr).(string)

	if name == "" {
		return diag.Errorf("database name can't be an empty string")
	}

	_, err = conn.Exec(ctx, `DROP DATABASE `+pq.QuoteIdentifier(name))
	if err != nil {
		return diag.FromErr(err)
	}

	d.SetId("")
	d.Set(dbNameAttr, "")

	close(stopCh)
	return diag.Diagnostics{}
}

func resourceDatabaseImporter(ctx context.Context, d *schema.ResourceData, meta interface{}) ([]*schema.ResourceData, error) {
	cockroachClient := meta.(*cockroachClient)

	local_port := d.Get(argLocalPort).(string)
	dns := strings.Replace(cockroachClient.dns, "<local_port>", local_port, 1)

	// id is the name of the database from the cockroachdb
	name := d.Id()

	// stopCh control the port forwarding lifecycle. When it gets closed the
	// port forward will terminate
	stopCh := make(chan struct{}, 1)
	// readyCh communicate when the port forward is ready to get traffic
	readyCh := make(chan struct{})

	tryPortForwardIfNeeded(ctx, d, meta, stopCh, readyCh, local_port)

	conn, err := pgx.Connect(ctx, dns)

	if err != nil {
		logError("failed connect to cockroachdb, error: %v", err)
		return nil, err
	}

	if err := conn.Ping(ctx); err != nil {
		logError("failed ping cockroachdb, error: %v", err)
		return nil, err
	}

	var (
		id    int
		owner string
	)
	err = conn.QueryRow(ctx, `SELECT id, owner FROM crdb_internal.databases WHERE name = $1`, name).Scan(
		&id,
		&owner,
	)
	if err != nil {
		logError("failed query cockroachdb, error: %v", err)
		return nil, err
	}

	d.SetId(strconv.Itoa(id))

	if err := d.Set(dbNameAttr, name); err != nil {
		logError("failed set name, error: %v", err)
		return nil, err
	}

	if err := d.Set(dbOwnerAttr, owner); err != nil {
		logError("failed set owner, error: %v", err)
		return nil, err
	}

	close(stopCh)

	return []*schema.ResourceData{d}, nil
}
