package provider

import (
	"context"
	"fmt"
	"github.com/hashicorp/terraform-plugin-sdk/v2/diag"
	"github.com/hashicorp/terraform-plugin-sdk/v2/helper/schema"
	"k8s.io/client-go/kubernetes"
	"k8s.io/client-go/rest"
	"k8s.io/client-go/tools/clientcmd"
	"log"
	"os"
	"strings"
	// "github.com/jackc/pgx/v4"
)

func init() {
	// Set descriptions to support markdown syntax, this will be used in document generation
	// and the language server.
	schema.DescriptionKind = schema.StringMarkdown

	// Customize the content of descriptions when output. For example you can add defaults on
	// to the exported descriptions if present.
	// schema.SchemaDescriptionBuilder = func(s *schema.Schema) string {
	// 	desc := s.Description
	// 	if s.Default != nil {
	// 		desc += fmt.Sprintf(" Defaults to `%v`.", s.Default)
	// 	}
	// 	return strings.TrimSpace(desc)
	// }
}

func New(version string) func() *schema.Provider {
	return func() *schema.Provider {
		p := &schema.Provider{
			Schema: providerSchema(),
			DataSourcesMap: map[string]*schema.Resource{
				"cockroach_database": dataSourceDatabase(),
			},
			ResourcesMap: map[string]*schema.Resource{
				"cockroach_database":        resourceDatabase(),
				"cockroach_database_backup": resourceDatabaseBackup(),
				"cockroach_user":            resourceUser(),
			},
		}

		p.ConfigureContextFunc = configure(version, p)

		return p
	}
}

type kubeConn struct {
	configPath  string
	nameSpace   string
	serviceName string
	remotePort  string
	kubeConfig  *rest.Config
	kubeClient  *kubernetes.Clientset
}

type cockroachClient struct {
	// Add whatever fields, client or connection info, etc. here
	// you would need to setup to communicate with the upstream
	// API.
	// conn    *pgx.Conn
	dns      string
	username string
	password string
	kubeConn kubeConn
}

const (
	argDns            = "dns"
	argUsername       = "username"
	argPassword       = "password"
	argKubeConfig     = "kube_config"
	argKubeConfigPath = "kube_config_path"
	argNamespace      = "namespace"
	argServiceName    = "service_name"
	argLocalPort      = "local_port"
	argRemotePort     = "remote_port"
)

func providerSchema() map[string]*schema.Schema {
	return map[string]*schema.Schema{
		argDns: {
			Type:        schema.TypeString,
			Optional:    true,
			Description: "DNS to access cockroachdb, if kubeconfig is specified this is optional",
		},
		argUsername: {
			Type:        schema.TypeString,
			Required:    true,
			Description: "The username used to access the database",
		},
		argPassword: {
			Type:        schema.TypeString,
			Required:    true,
			Description: "The password of the user used to access the database",
		},
		argKubeConfig: {
			Type:     schema.TypeList,
			Optional: true,
			Elem: &schema.Resource{
				Schema: map[string]*schema.Schema{
					argKubeConfigPath: {
						Type:        schema.TypeString,
						Optional:    true,
						Description: "Full path to a Kubernetes config",
						Default:     "~/.kube/config",
					},
					argNamespace: {
						Type:        schema.TypeString,
						Optional:    true,
						Description: "Kubernetes namespace where HC Vault is run",
					},
					argServiceName: {
						Type:        schema.TypeString,
						Optional:    true,
						Description: "Kubernetes service name of Vault",
					},
					argRemotePort: {
						Type:        schema.TypeString,
						Optional:    true,
						Description: "Remote service port to forward",
						Default:     "26257",
					},
				},
			},
		},
	}
}

func configure(version string, p *schema.Provider) func(context.Context, *schema.ResourceData) (interface{}, diag.Diagnostics) {
	return func(ctx context.Context, d *schema.ResourceData) (interface{}, diag.Diagnostics) {
		a := &cockroachClient{}

		a.username = d.Get(argUsername).(string)
		a.password = d.Get(argPassword).(string)

		if a.username == "" {
			return nil, diag.Errorf("database username can't be an empty string")
		}

		if a.password == "" {
			return nil, diag.Errorf("database password can't be an empty string")
		}

		if k := d.Get(argKubeConfig).([]interface{}); len(k) > 0 {
			kubeConn := k[0].(map[string]interface{})

			path := kubeConn[argKubeConfigPath].(string)

			if strings.Contains(path, "~") {
				homeDir, err := homeDir()
				if err != nil {
					return nil, diag.FromErr(err)
				}
				path = strings.Replace(path, "~", homeDir, -1)
			}

			// Create Kubernetes *rest.Config
			kubeConfig, err := clientcmd.BuildConfigFromFlags("", path)
			if err != nil {
				return nil, diag.FromErr(err)
			}
			a.kubeConn.kubeConfig = kubeConfig

			// Create Kubernetes *kubernetes.Clientset
			kubeClient, err := kubernetes.NewForConfig(a.kubeConn.kubeConfig)
			if err != nil {
				return nil, diag.FromErr(err)
			}
			a.kubeConn.kubeClient = kubeClient

			if namespace := kubeConn[argNamespace].(string); namespace != "" {
				a.kubeConn.nameSpace = namespace
			} else {
				return nil, diag.Errorf("Cockroachdb namespace is not specified")
			}

			if service := kubeConn[argServiceName].(string); service != "" {
				a.kubeConn.serviceName = service
			} else {
				return nil, diag.Errorf("Cockroachdb service name is not specified")
			}

			a.kubeConn.remotePort = kubeConn[argRemotePort].(string)

			// postgresql://master:PASSWORD@localhost:26257/defaultdb?sslmode=disable
			a.dns = fmt.Sprintf("postgresql://%s:%s@localhost:<local_port>/system?sslmode=disable", a.username, a.password)
		} else {
			if u := d.Get("argDns").(string); u != "" {
				a.dns = u
			}
		}

		if a.dns == "" {
			return nil, diag.Errorf("argument '%s' is required", "argDns")
		}

		return a, nil
	}
}

func logError(fmt string, v ...interface{}) {
	log.Printf("[ERROR] "+fmt, v)
}

func logInfo(fmt string, v ...interface{}) {
	log.Printf("[INFO] "+fmt, v)
}

func logDebug(fmt string, v ...interface{}) {
	log.Printf("[DEBUG] "+fmt, v)
}

func homeDir() (string, error) {
	if h := os.Getenv("HOME"); h != "" {
		return h, nil
	}
	return "", fmt.Errorf("unable to get HOME directory")
}
