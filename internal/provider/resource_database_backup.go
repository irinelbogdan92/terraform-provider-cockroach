package provider

import (
	"strconv"

	"github.com/jackc/pgx/v4"
	"github.com/lib/pq"

	"context"
	"github.com/hashicorp/terraform-plugin-sdk/v2/diag"
	"github.com/hashicorp/terraform-plugin-sdk/v2/helper/schema"
	_ "k8s.io/client-go/plugin/pkg/client/auth/gcp"
	"strings"
)

const (
	schedulerNameAttr       = "name"
	schedulerBackupPathAttr = "backup_path"
	schedulerDbNameAttr     = "database_name"
	backupOptionsAttr       = "backup_options"
	backupReccuringAttr     = "backup_recurring"
	backupFullBackupAttr    = "backup_full"
)

func resourceDatabaseBackup() *schema.Resource {
	return &schema.Resource{
		// This description is used by the documentation generator and the language server.
		Description: "Resource used to create a scheduler for a database backup job in a CockroachDB cluster.",

		CreateContext: resourceDatabaseBackupCreate,
		ReadContext:   resourceDatabaseBackupRead,
		UpdateContext: resourceDatabaseBackupUpdate,
		DeleteContext: resourceDatabaseBackupDelete,
		Schema: map[string]*schema.Schema{
			schedulerNameAttr: {
				Description: "Name of the scheduler.",
				Type:        schema.TypeString,
				Required:    true,
				ForceNew:    true,
			},
			schedulerDbNameAttr: {
				Description: "Name of the database where to run the backup.",
				Type:        schema.TypeString,
				Required:    true,
				ForceNew:    true,
			},
			schedulerBackupPathAttr: {
				Description: "The path where to save the backup, can be an s3 bucket.",
				Type:        schema.TypeString,
				Required:    true,
				ForceNew:    true,
			},
			backupFullBackupAttr: {
				Description: "Run full backup crontab",
				Type:        schema.TypeString,
				Optional:    true,
				ForceNew:    true,
				Default:     "ALWAYS",
			},
			backupReccuringAttr: {
				Description: "Backup reccuring attribute.",
				Type:        schema.TypeString,
				Optional:    true,
				ForceNew:    true,
				Default:     "@daily",
			},
			backupOptionsAttr: {
				Description: "The options to be used when setting up the scheduler",
				Type:        schema.TypeList,
				Elem: &schema.Schema{
					Type: schema.TypeString,
				},
				ForceNew: true,
				Optional: true,
			},
			argLocalPort: {
				Description: "Local port to be used for port-forward. (default is 26258), use different port to avoid same port opening.",
				Type:        schema.TypeString,
				Optional:    true,
				Default:     "26260",
			},
		},
	}
}

func resourceDatabaseBackupCreate(ctx context.Context, d *schema.ResourceData, meta interface{}) diag.Diagnostics {
	cockroachClient := meta.(*cockroachClient)

	local_port := d.Get(argLocalPort).(string)
	scheduler_name := d.Get(schedulerNameAttr).(string)
	db_name := d.Get(schedulerDbNameAttr).(string)
	scheduler_backup_path := d.Get(schedulerBackupPathAttr).(string)
	scheduler_full_backup := d.Get(backupFullBackupAttr).(string)
	scheduler_backup_reccuring := d.Get(backupReccuringAttr).(string)
	scheduler_backup_options := convertToString(d.Get(backupOptionsAttr).([]interface{}))

	dns := strings.Replace(cockroachClient.dns, "<local_port>", local_port, 1)

	// stopCh control the port forwarding lifecycle. When it gets closed the
	// port forward will terminate
	stopCh := make(chan struct{}, 1)
	// readyCh communicate when the port forward is ready to get traffic
	readyCh := make(chan struct{})

	set_scheduler_backup_options := ""

	if scheduler_name == "" {
		return diag.Errorf("Scheduler name can't be an empty string")
	}

	if db_name == "" {
		return diag.Errorf("Database name can't be an empty string")
	}

	if scheduler_backup_path == "" {
		return diag.Errorf("Backup path can't be an empty string")
	}

	if len(scheduler_backup_options) != 0 {
		set_scheduler_backup_options = "WITH " + strings.Join(scheduler_backup_options, " ")
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
		`CREATE SCHEDULE `+
			pq.QuoteIdentifier(scheduler_name)+
			` FOR BACKUP DATABASE `+
			pq.QuoteIdentifier(db_name)+
			` INTO `+
			pq.QuoteIdentifier(scheduler_backup_path)+
			` `+
			set_scheduler_backup_options+
			` RECURRING '`+
			scheduler_backup_reccuring+
			`'`+
			` FULL BACKUP `+
			scheduler_full_backup,
	)
	if err != nil {
		return diag.FromErr(err)
	}

	var id int

	err = conn.QueryRow(ctx, `SELECT schedule_id FROM scheduled_jobs WHERE schedule_name = $1`, scheduler_name).Scan(
		&id,
	)
	if err != nil {
		return diag.FromErr(err)
	}

	d.SetId(strconv.Itoa(id))

	close(stopCh)

	return diag.Diagnostics{}
}

func resourceDatabaseBackupRead(ctx context.Context, d *schema.ResourceData, meta interface{}) diag.Diagnostics {
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

	scheduller_id := d.Id()

	var scheduler_name string
	var schedule_expr string

	err = conn.QueryRow(ctx, `SELECT schedule_name, schedule_expr FROM scheduled_jobs WHERE schedule_id = $1`, scheduller_id).Scan(
		&scheduler_name,
		&schedule_expr,
	)

	if err != nil {
		return diag.FromErr(err)
	}

	if err := d.Set(schedulerNameAttr, scheduler_name); err != nil {
		return diag.FromErr(err)
	}

	if err := d.Set(backupReccuringAttr, schedule_expr); err != nil {
		return diag.FromErr(err)
	}

	close(stopCh)

	return diag.Diagnostics{}
}

func resourceDatabaseBackupUpdate(ctx context.Context, d *schema.ResourceData, meta interface{}) diag.Diagnostics {

	return diag.Diagnostics{}
}

func resourceDatabaseBackupDelete(ctx context.Context, d *schema.ResourceData, meta interface{}) diag.Diagnostics {
	cockroachClient := meta.(*cockroachClient)

	scheduller_id := d.Id()

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

	_, err = conn.Exec(ctx, `DROP SCHEDULE `+scheduller_id)
	if err != nil {
		return diag.FromErr(err)
	}

	d.SetId("")
	d.Set(schedulerDbNameAttr, "")

	close(stopCh)
	return diag.Diagnostics{}
}
