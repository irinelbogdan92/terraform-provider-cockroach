package provider

import (
	"github.com/jackc/pgx/v4"
	"github.com/lib/pq"

	"context"
	"fmt"
	"github.com/hashicorp/terraform-plugin-sdk/v2/diag"
	"github.com/hashicorp/terraform-plugin-sdk/v2/helper/schema"
	"strings"
)

const (
	dbUsernameAttr = "username"
	dbPasswordAttr = "password"
	dbRolesAttr    = "roles"
	dbAdminAttr    = "is_admin"
)

func resourceUser() *schema.Resource {
	return &schema.Resource{
		// This description is used by the documentation generator and the language server.
		Description: "Resource used to create a new user inside Cockroachdb cluster, and to attach required roles to the user.",

		CreateContext: resourceUserCreate,
		ReadContext:   resourceUserRead,
		UpdateContext: resourceUserUpdate,
		DeleteContext: resourceUserDelete,
		Importer: &schema.ResourceImporter{
			StateContext: resourceUserImporter,
		},

		Schema: map[string]*schema.Schema{
			dbUsernameAttr: {
				Description: "Name of the user to create.",
				Type:        schema.TypeString,
				Required:    true,
				ForceNew:    true,
			},
			dbPasswordAttr: {
				Description: "Password of the user to create.",
				Type:        schema.TypeString,
				Optional:    true,
				Sensitive:   true,
				Default:     "NULL",
			},
			dbRolesAttr: {
				Description: "Roles to attach to the created user.",
				Type:        schema.TypeString,
				Optional:    true,
				Default:     "",
			},
			dbAdminAttr: {
				Description: "True if the user is admin or false otherwise.",
				Type:        schema.TypeBool,
				Optional:    true,
				Default:     false,
			},
			argLocalPort: {
				Description: "Local port to be used for port-forward. (default is 26257), use different port to avoid same port opening.",
				Type:        schema.TypeString,
				Required:    true,
			},
		},
	}
}

func resourceUserCreate(ctx context.Context, d *schema.ResourceData, meta interface{}) diag.Diagnostics {
	cockroachClient := meta.(*cockroachClient)

	local_port := d.Get(argLocalPort).(string)
	name := d.Get(dbUsernameAttr).(string)
	password := d.Get(dbPasswordAttr).(string)
	roles := d.Get(dbRolesAttr).(string)
	isAdmin := d.Get(dbAdminAttr).(bool)
	dns := strings.Replace(cockroachClient.dns, "<local_port>", local_port, 1)

	// stopCh control the port forwarding lifecycle. When it gets closed the
	// port forward will terminate
	stopCh := make(chan struct{}, 1)
	// readyCh communicate when the port forward is ready to get traffic
	readyCh := make(chan struct{})

	if local_port == "" {
		return diag.Errorf("local_port can't be an empty string")
	}

	if name == "" {
		return diag.Errorf("username can't be an empty string")
	}

	if password == "" {
		return diag.Errorf("password can't be an empty string")
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
		`CREATE USER `+
			pq.QuoteIdentifier(name)+
			` WITH PASSWORD '`+
			password+
			`' `+
			roles,
	)

	if err != nil {
		return diag.FromErr(err)
	}

	if isAdmin {
		_, err := conn.Exec(ctx,
			`GRANT admin TO `+
				pq.QuoteIdentifier(name)+
				` WITH ADMIN OPTION`,
		)

		// _, err = conn.Exec(ctx, fmt.Sprintf("GRANT admin to %s", pq.QuoteIdentifier(name)))

		if err != nil {
			return diag.FromErr(err)
		}
	}

	d.SetId(name)
	d.Set(dbNameAttr, name)
	d.Set(dbPasswordAttr, password)
	d.Set(dbRolesAttr, roles)
	d.Set(dbAdminAttr, isAdmin)

	close(stopCh)

	return diag.Diagnostics{}
}

func resourceUserRead(ctx context.Context, d *schema.ResourceData, meta interface{}) diag.Diagnostics {
	cockroachClient := meta.(*cockroachClient)

	local_port := d.Get(argLocalPort).(string)
	dns := strings.Replace(cockroachClient.dns, "<local_port>", local_port, 1)

	if local_port == "" {
		return diag.Errorf("local_port can't be an empty string")
	}

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

	name := d.Id()

	rows, err := conn.Query(ctx, "SHOW USERS")
	if err != nil {
		// handle this error better than this
		return diag.FromErr(err)
	}
	found := false
	defer rows.Close()
	for rows.Next() {
		var (
			username  string
			options   string
			member_of []string
		)
		err = rows.Scan(&username, &options, &member_of)
		if err != nil {
			// handle this error
			return diag.FromErr(err)
		}

		if username == name {
			// TODO: find a way to read all the roles
			// if err := d.Set(dbRolesAttr, options); err != nil {
			// 	return diag.FromErr(err)
			// }

			if err := d.Set(dbAdminAttr, contains(member_of, "admin")); err != nil {
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

	if found == false {
		if err := d.Set(dbNameAttr, ""); err != nil {
			return diag.FromErr(err)
		}
	}

	close(stopCh)

	return nil
}

func resourceUserUpdate(ctx context.Context, d *schema.ResourceData, meta interface{}) diag.Diagnostics {
	cockroachClient := meta.(*cockroachClient)

	local_port := d.Get(argLocalPort).(string)
	dns := strings.Replace(cockroachClient.dns, "<local_port>", local_port, 1)

	if local_port == "" {
		return diag.Errorf("local_port can't be an empty string")
	}

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
	if d.HasChange(dbAdminAttr) || d.HasChange(dbRolesAttr) || d.HasChange(dbPasswordAttr) {
		_, npass := d.GetChange(dbPasswordAttr)
		oadmin, nadmin := d.GetChange(dbAdminAttr)
		_, nroles := d.GetChange(dbRolesAttr)
		oadmin = oadmin.(bool)
		nadmin = nadmin.(bool)

		name := d.Id()
		password := npass.(string)
		roles := nroles.(string)

		if password == "" {
			return diag.Errorf("User password cannot be empty")
		}

		// ALTER user
		_, err := conn.Exec(ctx,
			`ALTER USER `+
				pq.QuoteIdentifier(name)+
				` WITH PASSWORD '`+
				password+
				`' `+
				roles,
		)

		if err != nil {
			return diag.FromErr(err)
		}

		// disable or grant admin
		if oadmin == true && nadmin == false {
			// revoke admin
			_, err := conn.Exec(ctx,
				`REVOKE admin from `+
					pq.QuoteIdentifier(name),
			)

			if err != nil {
				return diag.FromErr(err)
			}
		}

		if oadmin == false && nadmin == true {
			// grant admin priviledged
			_, err := conn.Exec(ctx,
				`GRANT admin to `+
					pq.QuoteIdentifier(name)+
					` WITH ADMIN OPTION`,
			)

			if err != nil {
				return diag.FromErr(err)
			}
		}

		d.Set(dbAdminAttr, nadmin)
		d.Set(dbRolesAttr, nroles)
		d.Set(dbPasswordAttr, npass)
	}

	close(stopCh)
	d.Partial(false)
	return diag.Diagnostics{}
}

func resourceUserDelete(ctx context.Context, d *schema.ResourceData, meta interface{}) diag.Diagnostics {
	cockroachClient := meta.(*cockroachClient)

	local_port := d.Get(argLocalPort).(string)
	dns := strings.Replace(cockroachClient.dns, "<local_port>", local_port, 1)

	if local_port == "" {
		return diag.Errorf("local_port can't be an empty string")
	}

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
	username := d.Get(dbUsernameAttr).(string)

	if username == "" {
		return diag.Errorf("User name can't be an empty string")
	}

	_, err = conn.Exec(ctx, `DROP USER `+pq.QuoteIdentifier(username))
	if err != nil {
		return diag.FromErr(err)
	}

	d.SetId("")

	if err := d.Set(dbUsernameAttr, ""); err != nil {
		return diag.FromErr(err)
	}

	if err := d.Set(dbPasswordAttr, ""); err != nil {
		return diag.FromErr(err)
	}

	if err := d.Set(dbRolesAttr, ""); err != nil {
		return diag.FromErr(err)
	}

	if err := d.Set(dbAdminAttr, false); err != nil {
		return diag.FromErr(err)
	}

	close(stopCh)
	return diag.Diagnostics{}
}

func resourceUserImporter(ctx context.Context, d *schema.ResourceData, meta interface{}) ([]*schema.ResourceData, error) {
	err := resourceUserRead(ctx, d, meta)
	if err != nil {
		return nil, fmt.Errorf("Unable to import resource")
	}

	return []*schema.ResourceData{d}, nil
}
